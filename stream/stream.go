// Package stream implements sequential recording.
package stream

import "errors"

// ErrSizeMax denies a Record on size constraints.
var ErrSizeMax = errors.New("stream record exceeds size limit")

// Record is the stream element.
type Record struct {
	MediaType string // format
	Payload   []byte // content
}

// Reader provides access to a sequential recording.
type Reader interface {
	// ReadRecord places Records from a stream into basket in order of
	// appearance. The error is non-nil when read count n < len(basket).
	// Live streams have a shifing EOF. Use short polling to follow.
	//
	// ⚠️ Clients may not retain Payload from basket[:n]. The bytes stop
	// being valid at the next read due to possible buffer reuse.
	ReadRecords(basket []Record) (n int, err error)
}

// Writer feeds a recording.
type Writer interface {
	// WriteRecords appends batch to the recording in ascending order.
	// Any and all errors other than ErrSizeMax are fatal to a Writer.
	WriteRecords(batch []Record) error
}
