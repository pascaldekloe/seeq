// Package streamtest provides utilities for tests with streams.
package streamtest

import (
	"bytes"
	"errors"
	"io"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pascaldekloe/seeq/stream"
)

// VerifyContent verifies that the stream contains each Entry in argument order,
// and nothing more.
func VerifyContent(t *testing.T, r stream.Reader, entries ...stream.Entry) (ok bool) {
	t.Helper()

	basket := make([]stream.Entry, len(entries)+1)
	n, err := r.Read(basket)
	switch err {
	case nil:
		t.Errorf("stream read got more than %d entries", n)
		ok = false
	case io.EOF:
		if n != len(entries) {
			t.Errorf("stream read got %d entries, want %d", n, len(entries))
			ok = false
		}
	default:
		t.Error("stream read error:", err)
		ok = false
	}

	if n > len(entries) {
		n = len(entries)
	}
	for i := 0; i < n; i++ {
		got, want := basket[i], entries[i]

		switch {
		case got.MediaType == want.MediaType && string(got.Payload) == string(want.Payload):
			continue

		case want.MediaType == "text", strings.HasPrefix(want.MediaType, "text/"):
			t.Errorf("stream entry № %d got content %q payload %q, want content %q payload %q", i+1, got.MediaType, got.Payload, want.MediaType, want.Payload)
		default:
			t.Errorf("stream entry № %d got content %q payload %#x, want content %q payload %#x", i+1, got.MediaType, got.Payload, want.MediaType, want.Payload)
		}
		ok = false
	}

	return ok
}

// NewChannelWriter returns a writer which sends to a channel.
func NewChannelWriter() (stream.Writer, <-chan stream.Entry) {
	c := make(chan stream.Entry)
	return &channelWriter{out: c}, c
}

type channelWriter struct {
	out chan<- stream.Entry

	// concurrency detection
	goroutineID atomic.Pointer[string]
}

// Write implements the stream.Writer interface.
func (w *channelWriter) Write(batch []stream.Entry) error {
	goroutineID := string(bytes.Fields(debug.Stack())[1])
	previous := w.goroutineID.Swap(&goroutineID)
	if previous != nil && *previous != goroutineID {
		return errors.New("test stream write from multiple goroutines")
	}

	for i := range batch {
		w.out <- batch[i]
	}
	return nil
}

// NewChannelReader returns a reader which serves from channel input.
func NewChannelReader(bufN int) (stream.Reader, chan<- stream.Entry) {
	c := make(chan stream.Entry, bufN)
	return &channelReader{c: c}, c
}

type channelReader struct {
	c <-chan stream.Entry

	offset uint64

	// concurrency detection
	goroutineID atomic.Pointer[string]
}

// Read implements the stream.Reader interface.
func (r *channelReader) Read(basket []stream.Entry) (n int, err error) {
	defer func() {
		r.offset += uint64(uint(n))
	}()

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

		e := <-r.c
		basket[n].MediaType = e.MediaType
		// deep copy
		basket[n].Payload = append([]byte{}, e.Payload...)
		n++
	}
}

// Offset implements the stream.Reader interface.
func (r *channelReader) Offset() uint64 { return r.offset }

// NewFixedReader returns a reader which serves a fixed queue.
func NewFixedReader(queue ...stream.Entry) stream.Reader {
	queue = stream.AppendCopy(nil, queue...)
	r, c := NewChannelReader(len(queue))
	for i := range queue {
		c <- queue[i]
	}
	return r
}

// NewRepeatReader returns a reader which serves the same content endlessly.
type RepeatReader struct {
	MediaType string
	Payload   []byte
	ReadCount int
}

// Offset implements the stream.Reader interface.
func (repeated *RepeatReader) Offset() uint64 { return uint64(uint(repeated.ReadCount)) }

// Read implements the stream.Reader interface.
func (repeated *RepeatReader) Read(basket []stream.Entry) (n int, err error) {
	for i := range basket {
		basket[i].MediaType = repeated.MediaType
		basket[i].Payload = append(basket[i].Payload[:0], repeated.Payload...)
	}
	repeated.ReadCount += len(basket)
	return len(basket), nil
}

// ErrorReader returns a reader which replaces EOF from r with err.
func ErrorReader(r stream.Reader, err error) stream.Reader {
	return &errorReader{r, err}
}

type errorReader struct {
	r   stream.Reader
	err error
}

// Read implements the stream.Reader interface.
func (r *errorReader) Read(basket []stream.Entry) (n int, err error) {
	n, err = verifiedRead(r.r, basket)
	if err == io.EOF {
		err = r.err
	}
	return
}

// Offset implements the stream.Reader interface.
func (r *errorReader) Offset() uint64 { return r.r.Offset() }

// DelayReader returns a reader which delays every read from r with d.
func DelayReader(r stream.Reader, d time.Duration) stream.Reader {
	return &delayReader{r, d}
}

type delayReader struct {
	r stream.Reader
	d time.Duration
}

// Read implements the stream.Reader interface.
func (r *delayReader) Read(basket []stream.Entry) (n int, err error) {
	time.Sleep(r.d)
	return verifiedRead(r.r, basket)
}

// Offset implements the stream.Reader interface.
func (r *delayReader) Offset() uint64 { return r.r.Offset() }

// DripReader returns a reader which hits EOF every n entries from r, starting
// with the first.
func DripReader(r stream.Reader, n int) stream.Reader {
	return &dripReader{r: r, everyN: n, remain: 0}
}

type dripReader struct {
	r      stream.Reader
	everyN int // EOF every n read
	remain int
}

// Read implements the stream.Reader interface.
func (r *dripReader) Read(basket []stream.Entry) (n int, err error) {
	if r.remain <= 0 {
		r.remain = r.everyN
		return 0, io.EOF
	}

	if len(basket) > r.remain {
		basket = basket[:r.remain]
	}

	n, err = verifiedRead(r.r, basket)
	r.remain -= n
	if err == nil && r.remain <= 0 {
		r.remain = r.everyN
		return n, io.EOF
	}
	return
}

// Offset implements the stream.Reader interface.
func (r *dripReader) Offset() uint64 { return r.r.Offset() }

// VerifiedRead includes interface constraints check with a Read.
func verifiedRead(r stream.Reader, basket []stream.Entry) (n int, err error) {
	n, err = r.Read(basket)
	if n < 0 || n > len(basket) {
		panic("read count out of bounds")
	}
	if n < len(basket) && err == nil {
		panic("read less than basket without error")
	}
	return
}
