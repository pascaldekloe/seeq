package stream

import (
	"bufio"
	"encoding/binary"
	"io"
	"math/bits"
	"os"
	"syscall"
	"unsafe"
)

type simpleBufWriter struct {
	w *bufio.Writer // output
}

// NewSimpleWriter encodes each Entry with a 32-bit header. The MediaType size
// is limited to 255 bytes, and the payload size is limited to 16 MiB − 1 B..
func NewSimpleWriter(w io.Writer) Writer {
	f, ok := w.(*os.File)
	if ok {
		return simpleFDWriter{fd: f.Fd()}
	}
	return simpleBufWriter{bufio.NewWriter(w)}
}

// Write implements the Writer interface.
func (w simpleBufWriter) Write(batch []Entry) error {
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

type simpleFDWriter struct {
	fd      uintptr
	headers [][4]byte // reusable buffer
	vectors []syscall.Iovec
}

// Write implements the Writer interface.
func (w simpleFDWriter) Write(batch []Entry) error {
	if len(batch) > cap(w.headers) {
		w.headers = make([][4]byte, len(batch))
		w.vectors = make([]syscall.Iovec, 0, len(batch)*3)
	} else {
		w.headers = w.headers[:len(batch)]
		w.vectors = w.vectors[:0]
	}

	for i := range batch {
		if len(batch[i].MediaType) > 255 || len(batch[i].Payload) > 0xFF_FFFF {
			return ErrSizeMax
		}
		binary.BigEndian.PutUint32(w.headers[i][:], uint32(len(batch[i].Payload)<<8|len(batch[i].MediaType)))
		w.vectors = append(w.vectors, syscall.Iovec{&w.headers[i][0], 4})
		if batch[i].MediaType != "" {
			// go won't allow address of string content
			b := (*[]byte)(unsafe.Pointer(&batch[i].MediaType))
			w.vectors = append(w.vectors, syscall.Iovec{&(*b)[0], uint64(uint(len(batch[i].MediaType)))})
		}
		if len(batch[i].Payload) != 0 {
			w.vectors = append(w.vectors, syscall.Iovec{&batch[i].Payload[0], uint64(uint(len(batch[i].Payload)))})
		}
	}

	_, _, errno := syscall.Syscall(syscall.SYS_WRITEV, w.fd, uintptr(unsafe.Pointer(&w.vectors[0])), uintptr(len(w.vectors)))
	if errno != 0 {
		return errno
	}
	return nil
}

type simpleReader struct {
	r      io.Reader // input
	offset uint64    // position

	buf  []byte // read buffer
	bufI int    // buffer position index
	bufN int    // buffer byte count

	// Reuse strings intead of a memory allocation per Entry.
	mediaTypes map[string]string
}

// NewSimpleReader decodes output from a SimpleWriter. Partial entries at the
// end of input simply cause an io.EOF—not io.ErrUnexpectedEOF.
func NewSimpleReader(r io.Reader, offset uint64) Reader {
	return &simpleReader{
		r:      r,
		offset: offset,
		buf:    make([]byte, 512),
	}
}

// Read implements the Reader interface.
func (r *simpleReader) Read(basket []Entry) (n int, err error) {
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
func (r *simpleReader) Offset() uint64 { return r.offset }
