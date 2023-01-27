// Package streamtest provides utilities for tests with sequential recordings.
package streamtest

import (
	"bytes"
	"errors"
	"io"
	"runtime/debug"
	"sync"

	"github.com/pascaldekloe/seeq/stream"
)

// MockReader serves from a slice, with an option for custom errors.
type MockReader struct {
	sync.Mutex

	Queue []stream.Record // pending reads

	// Err is returned after Queue exhoustion. Nil defaults to io.EOF.
	Err error

	readRoutineID []byte // concurrency detection
}

// ReadRecords implements stream.Reader.
func (mock *MockReader) ReadRecords(basket []stream.Record) (n int, err error) {
	goroutineID := bytes.Fields(debug.Stack())[1]

	mock.Lock()
	defer mock.Unlock()

	switch {
	case mock.readRoutineID == nil:
		// set on first read
		mock.readRoutineID = goroutineID
	case string(mock.readRoutineID) == string(goroutineID):
		break // pass OK
	default:
		return 0, errors.New("mock stream ⛔️ read from multiple goroutines")
	}

	n = copy(basket, mock.Queue)
	mock.Queue = mock.Queue[n:] // pass
	switch {
	case len(mock.Queue) != 0:
		return n, nil
	case mock.Err != nil:
		return n, mock.Err
	default:
		return n, io.EOF
	}
}
