// Package seeq implements state collection from streams.
package seeq

import (
	"errors"
	"io"
	"time"

	"github.com/pascaldekloe/seeq/stream"
)

// An Aggregate consumes a stream of T—typically stream.Entry—for one or more
// specific queries.
type Aggregate[T any] interface {
	// AddNext consumes a stream in chronological order. Malformed content
	// should be reported only. The stream must continue at all times.
	AddNext(batch []T)

	Dumper // produces a snapshot
	Loader // resets to a snapshot
}

// A Dumper can produce snapshots of its state (for a Loader).
type Dumper interface {
	// DumpTo must be called on a read-only instance only.
	DumpTo(io.Writer) error
}

// A Loader can reset to the state of a snapshot (from a Dumper).
type Loader interface {
	// LoadFrom must execute in isolation. Errors leave the Loader in an
	// undefined state. No methods other than Shutdown shall be invoked
	// after error.
	LoadFrom(io.Reader) error
}

// Clone copies the state from src into dest.
func Clone(dest Loader, src Dumper) error {
	return cloneWithSnapshot(dest, src, nil)
}

// CloneWithSnapshot copies the state from src into dest with a copy of the
// snapshot into w.
func CloneWithSnapshot(dest Loader, src Dumper, w io.Writer) error {
	if w == nil {
		panic("nil Writer")
	}
	return cloneWithSnapshot(dest, src, w)
}

func cloneWithSnapshot(dest Loader, src Dumper, w io.Writer) error {
	pr, pw := io.Pipe()
	defer pr.Close()

	// write snapshot into pipe
	go func() {
		pw.CloseWithError(src.DumpTo(pw))
	}()

	var r io.Reader
	if w == nil {
		r = pr
	} else {
		r = io.TeeReader(pr, w)
	}

	// load receives errors from src and w through the pipe
	err := dest.LoadFrom(r)
	if err != nil {
		pw.Close() // abort dump if still active
		return err
	}

	// partial snapshot reads not permitted to prevent mistakes
	switch n, err := io.Copy(io.Discard, r); {
	case err != nil:
		return err
	case n != 0:
		return errors.New("pending data after snapshot load")
	}

	return nil
}

// FeedEach sends batches from c to each Aggregate in order of their respective
// argument position. The error comes from the stream.Reader with nil for EOF.
func FeedEach(c *stream.Cursor, aggs ...Aggregate[stream.Entry]) (lastRead time.Time, err error) {
	for {
		err := c.Next()
		if err == io.EOF {
			lastRead = time.Now()
		}

		if len(c.Batch) != 0 {
			for i := range aggs {
				aggs[i].AddNext(c.Batch)
			}
		}

		switch err {
		case nil:
			continue
		case io.EOF:
			return lastRead, nil
		default:
			return lastRead, err
		}
	}
}
