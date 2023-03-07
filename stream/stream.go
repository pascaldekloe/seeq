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

// CloneAll returns a deep copy of source.
func CloneAll(source ...Entry) []Entry {
	var byteN int
	for i := range source {
		byteN += len(source[i].Payload)
	}
	payloads := make([]byte, byteN)
	var offset int

	clone := make([]Entry, len(source))
	for i := range source {
		// read-only strings don't need copy
		clone[i].MediaType = source[i].MediaType

		end := offset + copy(payloads[offset:], source[i].Payload)
		clone[i].Payload = payloads[offset:end:end]
		offset = end
	}

	return clone
}

// Reader iterates over stream content in chronological order.
type Reader interface {
	// Read acquires the next in line and places each into basket in order
	// of appearance. The error is non-nil when read count n < len(basket).
	// Live streams have a shifing io.EOF. Use short polling to follow.
	//
	// ⚠️ Clients may not retain Payload from basket[:n]. The bytes stop
	// being valid at the next read due to possible buffer reuse.
	Read(basket []Entry) (n int, err error)

	// Offset returns the number of entries passed since the very beginning
	// of the stream.
	Offset() uint64
}

// Writer appends stream content.
type Writer interface {
	// Write adds batch to the stream in ascending order. Errors other than
	// ErrSizeMax are fatal to a Writer.
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

// MediaType is the decomposition a MIME definition.
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

// CachedMediaType resolves or creates a cached instance.
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
