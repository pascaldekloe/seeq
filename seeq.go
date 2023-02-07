// Package seeq implements state collection from streams into so-called
// aggregates. This package distinguishes between aggregates that do use block
// devices [Database] and those who don't [InMemory], as block devices may
// encounter errors, and they generally require some sort of termination after
// use. Implementations may define their own query interfaces as desired, which
// is outside the scope of this library.
package seeq

import "io"

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
