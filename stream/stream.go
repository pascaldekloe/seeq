// Package stream implements append-only collections.
package stream

import (
	"errors"
	"io"
	"mime"
	"strings"
	"sync"
)

// ErrSizeMax denies an Entry on size constraints.
var ErrSizeMax = errors.New("stream entry exceeds size limit")

// Entry is the stream element.
type Entry struct {
	MediaType string // format
	Payload   []byte // content
}

// AppendCopy adds a deep copy from each src Entry to dst, and it returns the
// extended buffer.
func AppendCopy(dst []Entry, entries ...Entry) []Entry {
	// all payloads in one alloction
	var byteN int
	for i := range entries {
		byteN += len(entries[i].Payload)
	}
	payloads := make([]byte, 0, byteN)

	for i := range entries {
		payloads = append(payloads, entries[i].Payload...) // copy
		dst = append(dst, Entry{
			Payload:   payloads[:len(payloads):len(payloads)], // cap
			MediaType: entries[i].MediaType,
		})
		payloads = payloads[len(payloads):] // pass
	}
	return dst
}

// Reader iterates over stream content in chronological order.
type Reader interface {
	// Read acquires the next in line and places each into basket in order
	// of appearance. The error is non-nil when read count n < len(basket).
	// Live streams have a shifing io.EOF. Use short polling to follow.
	//
	// ⚠️ Implementations may retain Payload from basket[:n]. The bytes stop
	// being valid at the next read due to possible buffer reuse.
	Read(basket []Entry) (n int, err error)

	// Offset returns the number of entries passed since the very beginning
	// of the stream.
	Offset() uint64
}

// Writer appends stream content.
//
// Only a single goroutine may invoke Write. Wrap a Writer with a NewFunnel for
// use from multiple goroutines.
type Writer interface {
	// Write adds batch to the stream in ascending order. Errors other than
	// ErrSizeMax are fatal to a Writer. Implementations may not retain the
	// entries, i.e., any mutation to batch after Write must not affect the
	// stream content.
	Write(batch []Entry) error
}

// A ReadCloser requires cleanup after use.
type ReadCloser interface {
	Reader
	io.Closer
}

// A WriteCloser requires cleanup after use.
type WriteCloser interface {
	Writer
	io.Closer
}

// A LimitReader reads from Reader but stops with EOF after N entries.
type LimitReader struct {
	Reader
	N int64 // limit
}

// Offset implements the Reader interface.
func (limited *LimitReader) Offset() uint64 { return limited.Reader.Offset() }

// Read implements the Reader interface.
func (limited *LimitReader) Read(basket []Entry) (n int, err error) {
	if limited.N == 0 {
		return 0, io.EOF
	}

	if int64(len(basket)) > limited.N {
		basket = basket[:limited.N]
	}

	n, err = limited.Reader.Read(basket)
	limited.N -= int64(n)
	return
}

// NewFunnel returns a new Writer proxy which can be used by multiple goroutines
// simultaneously. The funnel may concatenate multiple Writes [batches] into one
// Write operation towards out. Funnel Write gets io.ErrClosedPipe after Close.
func NewFunnel(out Writer) WriteCloser {
	f := funnel{
		out: out,
		que: make(chan []Entry),
		err: make(chan error),

		closed: make(chan struct{}),
	}

	go f.drain()

	return &f
}

type funnel struct {
	out Writer       // destination
	que chan []Entry // batch handover
	err chan error   // write response

	closed    chan struct{} // close signal
	closeOnce sync.Once
}

func (f *funnel) drain() {
	for {
		var batch []Entry
		select {
		case <-f.closed:
			return
		case batch = <-f.que:
			break
		}

		batchN := 1
		for ; ; batchN++ {
			select {
			case more := <-f.que:
				batch = append(batch, more...)
				continue

			default:
				break
			}
			break
		}

		err := f.out.Write(batch)
		for i := 0; i < batchN; i++ {
			f.err <- err
		}
	}
}

// Write implements the Writer interface.
func (f *funnel) Write(batch []Entry) error {
	select {
	case f.que <- batch:
		return <-f.err
	case <-f.closed:
		return io.ErrClosedPipe
	}
}

// Close implements the io.Closer interface.
func (f *funnel) Close() error {
	f.closeOnce.Do(func() { close(f.closed) })
	return nil
}

// A MediaType has the decomposition of a MIME definition.
//
// https://www.rfc-editor.org/rfc/rfc2045
// https://www.iana.org/assignments/media-type-structured-suffix/media-type-structured-suffix.xml
type MediaType struct {
	Type, Subtype, Suffix string

	params map[string]string
}

// ParseMediaType returns the interpretation on best-effort basis.
func ParseMediaType(s string) MediaType {
	var t MediaType
	s, t.params, _ = mime.ParseMediaType(s)
	t.Type, s, _ = strings.Cut(s, "/")
	t.Subtype, t.Suffix, _ = strings.Cut(s, "+")
	return t
}

// Param returns the value for a given attribute.
func (t *MediaType) Param(attr string) (value string, found bool) {
	value, found = t.params[attr]
	return
}

var (
	mediaTypeCacheLock sync.RWMutex
	mediaTypeCache     map[string]MediaType
)

// CachedMediaType either resolves a cached instance, or it creates a new cached
// instance with ParseMediaType.
func CachedMediaType(s string) MediaType {
	mediaTypeCacheLock.RLock()
	t, ok := mediaTypeCache[s]
	mediaTypeCacheLock.RUnlock()
	if ok {
		return t
	}

	mediaTypeCacheLock.Lock()
	defer mediaTypeCacheLock.Unlock()
	// retry in write lock
	t, ok = mediaTypeCache[s]
	if !ok {
		t = ParseMediaType(s)
		// lazy init
		if mediaTypeCache == nil {
			mediaTypeCache = make(map[string]MediaType)
		}
		mediaTypeCache[s] = t
	}
	return t
}

// MediaTypes is a string set used to deduplicate memory.
type mediaTypes map[string]string

// Singleton maps the bytes to their string equivalent.
func (t mediaTypes) singleton(bytes []byte) string {
	// no memory allocation for map lookup
	s, ok := t[string(bytes)]
	if ok {
		return s
	}

	// allocate new entry
	s = string(bytes)
	// register for reuse
	t[s] = s
	return s
}
