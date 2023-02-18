// Package stream implements append-only collections.
package stream

import (
	"errors"
	"io"
)

// ErrSizeMax denies an Entry on size constraints.
var ErrSizeMax = errors.New("stream entry exceeds size limit")

// Entry is the stream element.
type Entry struct {
	MediaType string // format
	Payload   []byte // content
}

// DeepCopy returns a full clone of each Entry.
func DeepCopy(entries ...Entry) []Entry {
	var l int
	for i := range entries {
		l += len(entries[i].Payload)
	}
	payloads := make([]byte, l)
	var offset int

	c := make([]Entry, len(entries))
	for i := range entries {
		c[i].MediaType = entries[i].MediaType

		end := offset + copy(payloads[offset:], entries[i].Payload)
		c[i].Payload = payloads[offset:end:end]
		offset = end
	}

	return c
}

// Reader iterates over stream content in chronological order.
type Reader interface {
	// Read acquires the next in line and places them into basket in order
	// of appearance. The error is non-nil when read count n < len(basket).
	// Live streams have a shifing EOF. Use short polling to follow.
	//
	// ⚠️ Clients may not retain Payload from basket[:n]. The bytes stop
	// being valid at the next read due to possible buffer reuse.
	Read(basket []Entry) (n int, err error)

	// Offset returns the number of entries passed since the very begining
	// of the stream.
	Offset() uint64
}

// Writer appends stream content.
type Writer interface {
	// Write adds batch to the stream in ascending order. Errors other than
	// ErrSizeMax are fatal to a Writer.
	Write(batch []Entry) error
}

// A ReadCloser requires cleanup after use.
type ReadCloser interface {
	Reader
	io.Closer
}

// A WriteCloser requires cleanup after use.
type WriteCloser interface {
	Writer
	io.Closer
}
