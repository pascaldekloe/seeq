package stream

import (
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
)

// SimpleReader decodes output from a SimpleWriter. Partial entries at the end
// of input simply cause an io.EOF—not io.ErrUnexpectedEOF.
type SimpleReader struct {
	R io.Reader // input

	buf  []byte // read buffer
	bufI int    // buffer position index
	bufN int    // buffer byte count

	// Reuse strings intead of a memory allocation per Entry.
	mediaTypes map[string]string
}

// Read implements the Reader interface.
func (r *SimpleReader) Read(basket []Entry) (n int, err error) {
	var bufSplit int // circular buffer appliance
	switch {
	case r.buf == nil:
		// first use
		r.buf = make([]byte, 512)
	case r.bufN == 0:
		// empty buffer
		r.bufI = 0
	default:
		// pending data
		bufSplit = r.bufI
	}

	// need EOF even with a zero length basket or a full basket
	for {
		const headerLen = 4

		// buffer header
		if r.bufN < headerLen {
			// space check
			switch {
			case r.bufI >= bufSplit && len(r.buf)-r.bufI >= headerLen:
				break // fits after bufSplit

			case r.bufI >= bufSplit && bufSplit >= headerLen:
				// fits before bufSplit; transfer remainder if any
				copy(r.buf[:r.bufN], r.buf[r.bufI:])
				r.bufI = 0

			case r.bufI < bufSplit && bufSplit-r.bufI >= headerLen:
				break // fits before bufSplit

			default:
				// buffer utilized by basket[:n]
				// assert len(basket) ≥ n > 0
				est := len(r.buf) * len(basket) / n
				est += headerLen // include read ahead
				grow := make([]byte, 1<<bits.Len(uint(est)))

				// Reserve the consumed for a better
				// re-estimate next time, if any.
				bufSplit = len(grow) - len(r.buf)

				copy(grow[:r.bufN], r.buf[r.bufI:])
				r.buf = grow
				r.bufI = 0
			}

			// read header or more
			readN, err := io.ReadAtLeast(r.R, r.buf[r.bufI+r.bufN:], headerLen-r.bufN)
			r.bufN += readN
			if err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) {
					err = io.EOF // partial entry at end OK
				}
				return n, err
			}
		}

		if n >= len(basket) {
			return n, nil // full basket
		}

		// parse header
		header := uint(binary.BigEndian.Uint32(r.buf[r.bufI:]))
		mediaTypeLen := int(header & 0xFF)
		payloadLen := int(header >> 8)

		// buffer remainder
		if l := headerLen + mediaTypeLen + payloadLen; r.bufN < l {
			// space check
			switch {
			case r.bufI >= bufSplit && len(r.buf)-r.bufI >= l:
				break // fits after bufSplit

			case r.bufI >= bufSplit && bufSplit >= l:
				// fits before bufSplit; transfer remainder if any
				copy(r.buf[:r.bufN], r.buf[r.bufI:])
				r.bufI = 0

			case r.bufI < bufSplit && bufSplit-r.bufI >= l:
				break // fits before bufSplit

			default:
				// buffer utilized by basket[:n]
				// assert len(basket) > n ≥ 0
				est := (len(r.buf) + l) * len(basket) / (n + 1)
				est += headerLen // include read ahead
				grow := make([]byte, 1<<bits.Len(uint(est)))

				// Reserve the consumed for a better
				// re-estimate next time, if any.
				bufSplit = len(grow) - len(r.buf)

				copy(grow[:r.bufN], r.buf[r.bufI:])
				r.buf = grow
				r.bufI = 0
			}

			// read entry or more
			readN, err := io.ReadAtLeast(r.R, r.buf[r.bufI+r.bufN:], l-r.bufN)
			r.bufN += readN
			if err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) {
					err = io.EOF // partial entry at end OK
				}
				return n, err
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
