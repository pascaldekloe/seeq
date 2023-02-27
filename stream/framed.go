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

// NewFramedReader decodes output from a NewFramedWriter. The Reader must start
// at the beginning of a frame. Offset counts the number of entries read before
// the current Reader position.
//
// Partial entries at the end cause an io.EOF—not io.ErrUnexpectedEOF. Such
// incomplete content will pass on to retries once the remaining data becomes
// available again.
//
// Each unique media-type value is kept in memory. Read does not allocate any
// memory for entries with reoccurring media types, other than the (automatic)
// buffer resize once required.
func NewFramedReader(r io.Reader, offset uint64) Reader {
	return &bufReader{
		r:          r,
		offset:     offset,
		buf:        make([]byte, 512),
		mediaTypes: make(mediaTypes),
	}
}

// BufReader decodes a framed stream with a single buffer to read with and slice
// from. The buffer grows on demand, depending on the entry sizes encountered,
// and depending on the basket length of the Read invocations.
type bufReader struct {
	r      io.Reader // input
	offset uint64    // position

	buf  []byte // read buffer
	bufI int    // buffer position index
	bufN int    // buffer byte count

	mediaTypes // each unique value read thus far
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

		frameLen := headerLen + mediaTypeLen + payloadLen
	BufferFrame:
		for r.bufN < frameLen {
			end := len(r.buf)
			if r.bufI < bufSplit {
				end = bufSplit
			}

			readN, err := io.ReadAtLeast(r.r, r.buf[r.bufI+r.bufN:end], frameLen-r.bufN)
			r.bufN += readN
			switch err {
			case nil:
				break BufferFrame // got it
			case io.ErrShortBuffer:
				break
			case io.ErrUnexpectedEOF:
				return n, io.EOF // partial entry at end OK
			default:
				return n, err
			}

			// grow buffer
			if r.bufI >= bufSplit && bufSplit >= frameLen {
				// roll over and move remainder, if any
				copy(r.buf[:r.bufN], r.buf[r.bufI:])
				r.bufI = 0
			} else {
				// buffer utilized by basket[:n]
				// assert len(basket) > n ≥ 0
				est := (len(r.buf) + frameLen) * len(basket) / (n + 1)
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
		r.bufI += frameLen
		r.bufN -= frameLen

		basket[n].MediaType = r.mediaTypes.singleton(r.buf[mediaTypeOffset:payloadOffset])
		basket[n].Payload = r.buf[payloadOffset:r.bufI:r.bufI]
		n++
	}
}

// Offset implements the Reader interface.
func (r *bufReader) Offset() uint64 { return r.offset }
