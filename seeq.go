// Package seeq implements state collection from streams into so-called
// aggregates. This package distinguishes between aggregates that do use block
// devices [Database] and those who don't [InMemory], as block devices may
// encounter errors, and they generally require some sort of termination after
// use. Implementations may define their own query interfaces as desired, which
// is outside the scope of this library.
package seeq

import (
	"errors"
	"io"
)

// InMemory aggregates build state from a stream of T—typically stream.Entry.
// There is no error scenario without use of block devices. No shutdown needed
// either. Malformed content should be reported only. The stream must continue
// at all times.
type InMemory[T any] interface {
	// AddNext is invoked with each stream element in order of appearance
	// starting with the first.
	AddNext(T)

	Dumper // produces a snapshot of an InMemory
	Loader // resets an InMemory to a snapshot
}

// Database aggregates build state from a stream of T—typically stream.Entry.
// Errors from AddNext are fatal to the aggregate. Malformed content should be
// reported only. The stream must continue at all times.
type Database[T any] interface {
	// AddNext is invoked with each stream element in order of appearance
	// starting with the first.
	AddNext(batch []T) error

	Shutdown() // frees any resources in use

	Dumper // produces a snapshot of a Database
	Loader // resets a Database to a snapshot
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
	return CloneWithSnapshot(dest, src, nil)
}

// CloneWithSnapshot copies the state from src into dest with a copy of the
// snapshot into w.
func CloneWithSnapshot(dest Loader, src Dumper, w io.Writer) error {
	pr, pw := io.Pipe()
	defer pr.Close()

	// write snapshot into pipe
	go func() {
		err := src.DumpTo(pw)
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	var r io.Reader
	if w == nil {
		r = pr
	} else {
		r = io.TeeReader(pr, w)
	}

	// Load receives errors from src and snapshot through the pipe
	err := dest.LoadFrom(r)
	if err != nil {
		return err
	}

	// Partial reads from a Loader are not permitted to prevent hard-to-find
	// mistakes.
	switch n, err := io.Copy(io.Discard, r); {
	case err != nil:
		return err
	case n != 0:
		return errors.New("pending snapshot data after load without error")
	}

	return nil
}
