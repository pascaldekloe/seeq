package stream

import (
	"bufio"
	"encoding/binary"
	"io"
	"math/bits"
)

// NewFramedWriter encodes each Entry framed with a 32-bit prefix. The MediaType
// size is limited to 255 bytes. The payload size is limited to 16 MiB − 1 B.
func NewFramedWriter(w io.Writer) Writer {
	return bufWriter{bufio.NewWriter(w)}
}

type bufWriter struct {
	w *bufio.Writer // output
}

// Write implements the Writer interface.
func (w bufWriter) Write(batch []Entry) error {
	for i := range batch {
		if len(batch[i].MediaType) > 255 || len(batch[i].Payload) > 0xFF_FFFF {
			return ErrSizeMax
		}
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(batch[i].Payload)<<8|len(batch[i].MediaType)))
		w.w.Write(header[:])
		w.w.WriteString(batch[i].MediaType)
		w.w.Write(batch[i].Payload)
	}

	return w.w.Flush()
}

// NewFramedReader decodes output from a NewFramedWriter. Partial entries at the
// end of input simply cause an io.EOF—not io.ErrUnexpectedEOF. Such incomplete
// content will pass on to any retries once the remaining data becomes available
// again.
func NewFramedReader(r io.Reader, offset uint64) Reader {
	return &bufReader{
		r:      r,
		offset: offset,
		buf:    make([]byte, 512),
	}
}

type bufReader struct {
	r      io.Reader // input
	offset uint64    // position

	buf  []byte // read buffer
	bufI int    // buffer position index
	bufN int    // buffer byte count

	// Reuse strings intead of a memory allocation per Entry.
	mediaTypes map[string]string
}

// Read implements the Reader interface.
func (r *bufReader) Read(basket []Entry) (n int, err error) {
	defer func() {
		r.offset += uint64(uint(n))
	}()

	var bufSplit int // circular buffer appliance
	if r.bufN == 0 {
		// empty buffer
		r.bufI = 0
	} else {
		// pending data
		bufSplit = r.bufI
	}

	// need EOF even with a zero length basket or a full basket
	for {
		const headerLen = 4

	BufferHeader:
		for r.bufN < headerLen {
			end := len(r.buf)
			if r.bufI < bufSplit {
				end = bufSplit
			}

			readN, err := io.ReadAtLeast(r.r, r.buf[r.bufI+r.bufN:end], headerLen-r.bufN)
			r.bufN += readN
			switch err {
			case nil:
				break BufferHeader // got it
			case io.ErrShortBuffer:
				break
			case io.ErrUnexpectedEOF:
				return n, io.EOF // partial entry at end OK
			default:
				return n, err
			}

			// grow buffer
			if r.bufI >= bufSplit && bufSplit >= headerLen {
				// roll over and move remainder, if any
				copy(r.buf[:r.bufN], r.buf[r.bufI:])
				r.bufI = 0
			} else {
				// buffer utilized by basket[:n]
				// assert len(basket) ≥ n > 0
				est := len(r.buf) * len(basket) / n
				est += headerLen // include read ahead
				grow := make([]byte, 1<<bits.Len(uint(est)))

				// Reserve the consumed for a better
				// growth estimate next time, if any.
				bufSplit = len(grow) - len(r.buf)

				// transfer remainder, if any
				copy(grow[:r.bufN], r.buf[r.bufI:])
				r.buf = grow
				r.bufI = 0
			}
		}

		if n >= len(basket) {
			return n, nil // full basket
		}

		// parse header
		header := uint(binary.BigEndian.Uint32(r.buf[r.bufI:]))
		mediaTypeLen := int(header & 0xFF)
		payloadLen := int(header >> 8)

	BufferRemainder:
		for l := headerLen + mediaTypeLen + payloadLen; r.bufN < l; {
			end := len(r.buf)
			if r.bufI < bufSplit {
				end = bufSplit
			}

			readN, err := io.ReadAtLeast(r.r, r.buf[r.bufI+r.bufN:end], l-r.bufN)
			r.bufN += readN
			switch err {
			case nil:
				break BufferRemainder // got it
			case io.ErrShortBuffer:
				break
			case io.ErrUnexpectedEOF:
				return n, io.EOF // partial entry at end OK
			default:
				return n, err
			}

			// grow buffer
			if r.bufI >= bufSplit && bufSplit >= l {
				// roll over and move remainder, if any
				copy(r.buf[:r.bufN], r.buf[r.bufI:])
				r.bufI = 0
			} else {
				// buffer utilized by basket[:n]
				// assert len(basket) > n ≥ 0
				est := (len(r.buf) + l) * len(basket) / (n + 1)
				est += headerLen // include read ahead
				grow := make([]byte, 1<<bits.Len(uint(est)))

				// Reserve the consumed for a better
				// growth estimate next time, if any.
				bufSplit = len(grow) - len(r.buf)

				// transfer remainder, if any
				copy(grow[:r.bufN], r.buf[r.bufI:])
				r.buf = grow
				r.bufI = 0
			}
		}

		mediaTypeOffset := r.bufI + headerLen
		payloadOffset := mediaTypeOffset + mediaTypeLen
		r.bufI = payloadOffset + payloadLen
		r.bufN -= headerLen + mediaTypeLen + payloadLen

		// no memory allocation for map lookup
		mediaType, ok := r.mediaTypes[string(r.buf[mediaTypeOffset:payloadOffset])]
		if !ok {
			// allocate new entry
			mediaType = string(r.buf[mediaTypeOffset:payloadOffset])
			// register for reuse
			if r.mediaTypes == nil {
				// lazy initiation
				r.mediaTypes = make(map[string]string)
			}
			r.mediaTypes[mediaType] = mediaType
		}

		// install 🧺
		basket[n].MediaType = mediaType
		basket[n].Payload = r.buf[payloadOffset:r.bufI:r.bufI]
		n++
	}
}

// Offset implements the Reader interface.
func (r *bufReader) Offset() uint64 { return r.offset }
