package stream

import "errors"

// ErrFuture denies an offset beyond the current.
var ErrFuture = errors.New("stream offset not reached yet")

// Repo is a named stream-collection. A stream can have only one Writer. Any
// amount of Readers are permitted.
//
// Multiple goroutines may invoke methods on a Repo simultaneously.
type Repo interface {
	// ReadAt opens a stream by name and it skips the offset amount of
	// entries before the first Read.
	ReadAt(name string, offset uint64) ReadCloser

	// AppendTo opens a stream by name. A fail-safe will deny multiple
	// Writers to append to the same stream.
	AppendTo(name string) WriteCloser
}

// An ErrorWriter is for fatal errors.
type errorWriter struct{ err error }

// Write implements the Writer interface.
func (w errorWriter) Write(batch []Entry) error { return w.err }

// Close implements the io.Closer interface.
func (errorWriter) Close() error { return nil }
