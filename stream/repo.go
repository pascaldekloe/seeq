package stream

import "errors"

// ErrFuture denies an offset beyond the number of entries present.
var ErrFuture = errors.New("stream offset not reached yet")

// ReaderAt provides access to a named stream-collection.
//
// Multiple goroutines may invoke methods on a ReaderAt simultaneously.
type ReaderAt interface {
	// ReadAt opens a stream by name and it skips the offset amount of
	// entries before the first Read.
	ReadAt(name string, offset uint64) ReadCloser
}

// Repo is a named stream-collection. A stream can have only one Writer. Any
// amount of Readers are permitted.
//
// Multiple goroutines may invoke methods on a Repo simultaneously.
type Repo interface {
	ReaderAt

	// AppendTo opens a stream by name. When name is opened more than once,
	// then the redundant writers should return errors only as a fail-safe.
	AppendTo(name string) WriteCloser
}

// An ErrorWriter is for fatal errors.
type errorWriter struct{ err error }

// Write implements the Writer interface.
func (w errorWriter) Write(batch []Entry) error { return w.err }

// Close implements the io.Closer interface.
func (errorWriter) Close() error { return nil }
