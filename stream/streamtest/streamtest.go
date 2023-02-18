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

	bucket := make([]stream.Entry, len(entries)+1)
	n, err := r.Read(bucket)
	switch err {
	case nil, io.EOF:
		if n != len(entries) {
			t.Errorf("stream read %d entries, want %d", n, len(entries))
		}
	default:
		t.Error("stream read error:", err)
		ok = false
	}

	if n > len(entries) {
		n = len(entries)
	}
	for i := 0; i < n; i++ {
		got, want := bucket[i], entries[i]

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

		basket[n] = stream.DeepCopy(<-r.c)[0]
		n++
	}
}

// NewChannelReader returns a reader which serves from channel input.
func NewChannelReader(bufN int) (stream.Reader, chan<- stream.Entry) {
	c := make(chan stream.Entry, bufN)
	return &channelReader{c: c}, c
}

// NewFixedReader returns a reader which serves a fixed queue.
func NewFixedReader(queue ...stream.Entry) stream.Reader {
	r, c := NewChannelReader(len(queue))
	for i := range stream.DeepCopy(queue...) {
		c <- queue[i]
	}
	return r
}

// ErrorReader returns a reader which replaces EOF from r with err.
func ErrorReader(r stream.Reader, err error) stream.Reader {
	if err == nil {
		panic("nil error")
	}
	return &errorReader{r, err}
}

type errorReader struct {
	r   stream.Reader
	err error
}

// Read implements stream.Reader.
func (r *errorReader) Read(basket []stream.Entry) (n int, err error) {
	n, err = verifiedRead(r.r, basket)
	if err == io.EOF {
		err = r.err
	}
	return
}

// DelayReader returns a reader which delays every read from r with d.
func DelayReader(r stream.Reader, d time.Duration) stream.Reader {
	if d <= 0 {
		panic("no delay")
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
	return verifiedRead(r.r, basket)
}

// DripReader returns a reader which hits EOF every n entries from r, starting
// with the first.
func DripReader(r stream.Reader, n int) stream.Reader {
	return &dripReader{r: r, everyN: n, remainN: 0}
}

type dripReader struct {
	r       stream.Reader
	everyN  int
	remainN int
}

// Read implements stream.Reader.
func (r *dripReader) Read(basket []stream.Entry) (n int, err error) {
	if r.remainN <= 0 {
		r.remainN = r.everyN
		return 0, io.EOF
	}

	if len(basket) > r.remainN {
		basket = basket[:r.remainN]
	}

	n, err = verifiedRead(r.r, basket)
	r.remainN -= n
	if err == nil && r.remainN == 0 {
		err = io.EOF
		r.remainN = r.everyN
	}
	return
}

// VerifiedRead includes interface constraints check with a Read.
func verifiedRead(r stream.Reader, basket []stream.Entry) (n int, err error) {
	n, err = r.Read(basket)
	if n < 0 || n > len(basket) {
		panic("read count out of bounds")
	}
	if n < len(basket) && err == nil {
		panic("read less that basket without error")
	}
	return
}

// NewRepoWith populates a repository for testing. The files are automatically
// removed when the test and all its subtests complete.
func NewRepoWith(t testing.TB, name string, entries ...stream.Entry) stream.Repo {
	t.Helper()

	repo := stream.RollingFiles{
		Dir:    t.TempDir(),
		ChunkN: 5,
	}

	w := repo.AppendTo(name)
	if err := w.Write(entries); err != nil {
		t.Errorf("write to %q got error: %s", name, err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("writer close of %q got error: %s", name, err)
	}

	return &repo
}
