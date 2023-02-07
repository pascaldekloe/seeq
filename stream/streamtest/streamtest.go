// Package streamtest provides utilities for tests with streams.
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

	Queue []stream.Entry // pending reads

	// Err is returned after Queue exhoustion. Nil defaults to io.EOF.
	Err error

	readRoutineID []byte // concurrency detection
}

// Read implements stream.Reader.
func (mock *MockReader) Read(basket []stream.Entry) (n int, err error) {
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
		return 0, errors.New("mock stream read ⛔️ from multiple goroutines")
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

// Read implements stream.Reader.
func (d *delayReader) Read(basket []stream.Entry) (n int, err error) {
	time.Sleep(d.d)
	return d.Read(basket)
}

// DripNReader returns a reader which hits io.EOF every n entries, starting with
// the first.
func DripNReader(r stream.Reader, n int) stream.Reader {
	if n <= 0 {
		panic("need positive drip count")
	}
	return &dripReader{r: r, dripN: n, remainN: 0}
}

type dripReader struct {
	r              stream.Reader
	dripN, remainN int
}

// Read implements stream.Reader.
func (d *dripReader) Read(basket []stream.Entry) (n int, err error) {
	if d.remainN == 0 {
		d.remainN = d.dripN
		return 0, io.EOF
	}

	if len(basket) > d.remainN {
		basket = basket[:d.remainN]
	}

	n, err = d.r.Read(basket)
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
	C <-chan stream.Entry
}

// Read implements stream.Reader.
func (r *channelReader) Read(basket []stream.Entry) (n int, err error) {
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
func ChannelReader(bufN int) (stream.Reader, chan<- stream.Entry) {
	c := make(chan stream.Entry, bufN)
	return &channelReader{c}, c
}
