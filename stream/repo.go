package stream

import "errors"

// ErrFuture denies an offset beyond the present number of entries.
var ErrFuture = errors.New("stream offset not reached yet")

// Collection provides read-access to named streams.
//
// Multiple goroutines may invoke a methods on Collection simultaneously.
type Collection interface {
	// ReaderAt opens a stream by name, skipping the
	// offset amount of entries before the first Read.
	ReaderAt(name string, offset uint64) ReadCloser
}

// Repo manages named streams.
//
// Multiple goroutines may invoke methods on a Repo simultaneously.
type Repo interface {
	Collection

	// AppendTo opens a stream by name for addition.
	AppendTo(name string) WriteCloser
}

// An ErrorWriter is for fatal errors.
type errorWriter struct{ err error }

// Write implements the Writer interface.
func (w errorWriter) Write(batch []Entry) error { return w.err }

// Close implements the io.Closer interface.
func (errorWriter) Close() error { return nil }
