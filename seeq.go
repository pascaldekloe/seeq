// Package seeq implements state collection from streams.
package seeq

import (
	"fmt"
	"io"

	"github.com/pascaldekloe/seeq/snapshot"
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

// Copy the state from src into dst. Snapshot Production may be nil. Copy does
// not Commit nor Abort the Production.
func Copy(dst, src Transfering, p snapshot.Production) error {
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
