// Package seeq implements state collection from streams.
package seeq

import (
	"fmt"
	"io"

	"github.com/pascaldekloe/seeq/snapshot"
)

// An Aggregate consumes a stream of T—typically stream.Entry—for one or more
// specific queries. Such queries may be served with exported fields and/or
// methods. The specifics are entirely up to the user and they are beyond the
// scope of this interface.
//
// Both LoadFrom and AddNext must execute in isolation. DumpTo is considdered to
// be a read-only operation. Therefore, DumpTo can be invoked simultaneously
// with other query methods (from multiple goroutines).
type Aggregate[T any] interface {
	// AddNext consumes a stream in chronological order. Malformed content
	// should be reported only. The stream must continue at all times.
	AddNext(batch []T)

	// DumpTo produces a snapshot/serial/backup of the Aggregate's state.
	// When the implementation makes use of third-party storage such as a
	// database, then the snapshot should include the stored content too.
	DumpTo(io.Writer) error

	// LoadFrom resets the Aggregate state to a snapshot from DumpTo.
	// Errors may leave the Aggregate in an undefined state.
	LoadFrom(io.Reader) error
}

// Copy the state from src into dst. Snapshot Production may be nil. Copy does
// not Commit nor Abort the Production.
func Copy[T any](dst, src Aggregate[T], p snapshot.Production) error {
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
	err := dst.LoadFrom(r)
	if err != nil {
		return err
	}

	// partial snapshot reads not permitted to prevent mistakes
	switch n, err := io.Copy(io.Discard, r); {
	case err != nil:
		return fmt.Errorf("aggregate %T snapshot dump after load: %w", src, err)
	case n != 0:
		return fmt.Errorf("aggregate %T left %d bytes after snapshot load", dst, n)
	}

	return nil
}
