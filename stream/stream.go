// Package stream implements append-only collections.
package stream

import "errors"

// ErrSizeMax denies an Entry on size constraints.
var ErrSizeMax = errors.New("stream entry exceeds size limit")

// Entry is the stream element.
type Entry struct {
	MediaType string // format
	Payload   []byte // content
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
}

// Writer appends stream content.
type Writer interface {
	// Write adds batch to the stream in ascending order. Errors other than
	// ErrSizeMax are fatal to a Writer.
	Write(batch []Entry) error
}

// Cursor operates on a reusable buffer. It keeps track of the position with its
// sequence number.
type Cursor struct {
	R     Reader  // input
	Batch []Entry // read buffer
	SeqNo uint64  // Entry count
}

// Next resets Batch to the next in line. The amount is limited by the slice
// capacity. SeqNo gets updated accordingly.
func (c *Cursor) Next() error {
	n, err := c.R.Read(c.Batch[:cap(c.Batch)])
	c.Batch = c.Batch[:n]
	c.SeqNo += uint64(uint(n))
	return err
}
