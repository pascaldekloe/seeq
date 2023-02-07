// Package streamtest provides utilities for tests with streams.
package streamtest

import (
	"bytes"
	"errors"
	"io"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/pascaldekloe/seeq/stream"
)

type channelReader struct {
	c <-chan stream.Entry

	// concurrency detection
	goroutineID atomic.Pointer[string]
}

// Read implements stream.Reader.
func (r *channelReader) Read(basket []stream.Entry) (n int, err error) {
	goroutineID := string(bytes.Fields(debug.Stack())[1])
	previous := r.goroutineID.Swap(&goroutineID)
	if previous != nil && *previous != goroutineID {
		return 0, errors.New("test stream read from multiple goroutines")
	}

	for {
		if len(r.c) == 0 {
			return n, io.EOF
		}
		if n >= len(basket) {
			return n, nil
		}

		basket[n] = <-r.c
		n++
	}
}

// ChannelReader returns a reader which serves from channel input.
func ChannelReader(bufN int) (stream.Reader, chan<- stream.Entry) {
	c := make(chan stream.Entry, bufN)
	return &channelReader{c: c}, c
}

// FixedReader returns a reader which serves a fixed sequence.
func FixedReader(entries ...stream.Entry) stream.Reader {
	r, c := ChannelReader(len(entries))
	for i := range entries {
		c <- entries[i]
	}
	return r
}

// ErrorReader returns a reader which replaces EOF with a custom error.
func ErrorReader(r stream.Reader, err error) stream.Reader {
	if err == nil {
		panic("need error")
	}
	return &errorReader{r, err}
}

type errorReader struct {
	r   stream.Reader
	err error
}

// Read implements stream.Reader.
func (r *errorReader) Read(basket []stream.Entry) (n int, err error) {
	n, err = r.r.Read(basket)
	if err == io.EOF {
		err = r.err
	}
	return
}

// DelayReader returns a reader which delays every read by a fixed duration.
func DelayReader(r stream.Reader, d time.Duration) stream.Reader {
	if d <= 0 {
		panic("need positive delay")
	}
	return &delayReader{r, d}
}

type delayReader struct {
	r stream.Reader
	d time.Duration
}

// Read implements stream.Reader.
func (r *delayReader) Read(basket []stream.Entry) (n int, err error) {
	time.Sleep(r.d)
	return r.r.Read(basket)
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
func (r *dripReader) Read(basket []stream.Entry) (n int, err error) {
	if r.remainN == 0 {
		r.remainN = r.dripN
		return 0, io.EOF
	}

	if len(basket) > r.remainN {
		basket = basket[:r.remainN]
	}

	n, err = r.r.Read(basket)
	if n < 0 || n > len(basket) {
		panic("read count out of bounds")
	}
	r.remainN -= n
	if err == nil && r.remainN == 0 {
		err = io.EOF
		r.remainN = r.dripN
	}
	return
}
