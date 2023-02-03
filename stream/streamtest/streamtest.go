// Package streamtest provides utilities for tests with sequential recordings.
package streamtest

import (
	"bytes"
	"errors"
	"io"
	"runtime/debug"
	"sync"
	"time"

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

// DelayReader returns a reader which delays every read by a fixed duration.
func DelayReader(r stream.Reader, d time.Duration) stream.Reader {
	if d <= 0 {
		panic("illegal delay")
	}
	return &delayReader{r, d}
}

type delayReader struct {
	r stream.Reader
	d time.Duration
}

// ReadRecords implements stream.Reader.
func (d *delayReader) ReadRecords(basket []stream.Record) (n int, err error) {
	time.Sleep(d.d)
	return d.ReadRecords(basket)
}

// DripNReader returns a reader which hits io.EOF every n records, starting with
// the first.
func DripNReader(r stream.Reader, n int) stream.Reader {
	if n <= 0 {
		panic("illegal drip count")
	}
	return &dripReader{r: r, dripN: n, remainN: 0}
}

type dripReader struct {
	r              stream.Reader
	dripN, remainN int
}

// ReadRecords implements stream.Reader.
func (d *dripReader) ReadRecords(basket []stream.Record) (n int, err error) {
	if d.remainN == 0 {
		d.remainN = d.dripN
		return 0, io.EOF
	}

	if len(basket) > d.remainN {
		basket = basket[:d.remainN]
	}

	n, err = d.r.ReadRecords(basket)
	if n < 0 || n > len(basket) {
		panic("read count out of bounds")
	}
	d.remainN -= n
	if err == nil && d.remainN == 0 {
		err = io.EOF
		d.remainN = d.dripN
	}
	return
}

type channelReader struct {
	C <-chan stream.Record
}

// ReadRecords implements stream.Reader.
func (r *channelReader) ReadRecords(basket []stream.Record) (n int, err error) {
	for {
		if len(r.C) == 0 {
			return n, io.EOF
		}
		if n >= len(basket) {
			return n, nil
		}

		basket[n] = <-r.C
		n++
	}
}

// ChannelReader returns a reader which serves from channel input.
func ChannelReader(bufN int) (stream.Reader, chan<- stream.Record) {
	c := make(chan stream.Record, bufN)
	return &channelReader{c}, c
}
