// Package seeq implements state collection from streams.
package seeq

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/pascaldekloe/seeq/snapshot"
	"github.com/pascaldekloe/seeq/stream"
)

// An Aggregate consumes a stream of T—typically stream.Entry—for one or more
// specific queries.
type Aggregate[T any] interface {
	// AddNext consumes a stream in chronological order. Malformed content
	// should be reported only. The stream must continue at all times.
	AddNext(batch []T)

	Transfering
}

// A Transfering aggregate can produce snapshots of its state, and it can reset
// to the state of a snapshot.
type Transfering interface {
	// DumpTo must be called on a read-only instance only.
	DumpTo(io.Writer) error

	// LoadFrom must execute in isolation. Errors leave the aggregate in an
	// undefined state.
	LoadFrom(io.Reader) error
}

// Clone copies the state from src into dest. Snapshot Production is optional.
// Clone does not Commit nor Abort the Production.
func Clone(dest, src Transfering, p snapshot.Production) error {
	pr, pw := io.Pipe()
	defer pr.Close()

	// write snapshot into pipe
	go func() {
		pw.CloseWithError(src.DumpTo(pw))
	}()

	var r io.Reader
	if p == nil {
		r = pr
	} else {
		r = io.TeeReader(pr, p)
	}

	// load receives errors from src and w through the pipe
	err := dest.LoadFrom(r)
	if err != nil {
		return err
	}

	// partial snapshot reads not permitted to prevent mistakes
	switch n, err := io.Copy(io.Discard, r); {
	case err != nil:
		return fmt.Errorf("aggregate %T snapshot dump after load: %w", src, err)
	case n != 0:
		return fmt.Errorf("aggregate %T left %d bytes after snapshot load", dest, n)
	}

	return nil
}

// SyncEach applies all entries from r to each Aggregate in argument order. Buf
// defines the batch size for Read and AddNext. Error is nil on success-not EOF.
func SyncEach(r stream.Reader, buf []stream.Entry, aggs ...Aggregate[stream.Entry]) (lastRead time.Time, err error) {
	if len(buf) == 0 {
		return time.Time{}, errors.New("aggregate feed can't work on empty buffer")
	}

	for {
		n, err := r.Read(buf)
		if err == io.EOF {
			lastRead = time.Now()
		}

		if n > 0 {
			for i := range aggs {
				aggs[i].AddNext(buf[:n])
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
