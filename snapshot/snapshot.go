// Package snapshot provides aggregate state persistence.
package snapshot

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// Production must end with either a Close or an Abort.
type Production interface {
	io.Writer      // appends
	Commit() error // applies
	Abort() error  // discards
}

// Archive manages a snapshot collection.
type Archive interface {
	Open(name string, seqNo uint64) (io.ReadCloser, error)
	Make(name string, seqNo uint64) (Production, error)

	// History lists each sequence number available in ascending order.
	History(name string) ([]uint64, error)
}

type fileDir string

// NewFileDir returns an Archive based on plain files in the directory specified
// by path.
func NewFileDir(path string) Archive { return fileDir(path) }

// Open implements the Archive interface.
func (dir fileDir) Open(name string, seqNo uint64) (io.ReadCloser, error) {
	// validate name
	path := filepath.Join(string(dir), fmt.Sprintf("%s-%016X.snapshot", name, seqNo))
	return os.Open(path)
}

// Make implements the Archive interface.
func (dir fileDir) Make(name string, seqNo uint64) (Production, error) {
	// validate name
	path := filepath.Join(string(dir), fmt.Sprintf("%s-%016X.spool", name, seqNo))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return nil, err
	}
	return fileProduction{f}, nil
}

type fileProduction struct {
	*os.File
}

// Write implements the io.Writer interface.
func (p fileProduction) Write(bytes []byte) (n int, err error) {
	return p.File.Write(bytes)
}

// WriteString adds the io.StringWriter optimization.
func (p fileProduction) WriteString(s string) (n int, err error) {
	return p.File.WriteString(s)
}

// ReadFrom adds the io.ReaderFrom optimization.
func (p fileProduction) ReadFrom(r io.Reader) (n int64, err error) {
	return p.File.ReadFrom(r)
}

// Commit implements the Production interface.
func (p fileProduction) Commit() error {
	defer p.File.Close()
	err := p.File.Sync()
	if err != nil {
		return err
	}
	name := p.File.Name()
	return os.Rename(name, name[:len(name)-6]+".snapshot")
}

// Abort implements the Production interface.
func (p fileProduction) Abort() error {
	defer p.File.Close()
	return os.Remove(p.File.Name())
}

func (dir fileDir) History(name string) ([]uint64, error) {
	entries, err := os.ReadDir(string(dir))
	if err != nil {
		return nil, err
	}
	var history []uint64

	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		fname := e.Name()
		if len(fname) != len(name)+26 || fname[:len(name)] != name || fname[len(name)] != '-' || fname[len(name)+17:] != ".snapshot" {
			continue
		}
		seqNo, err := strconv.ParseUint(fname[len(name)+1:len(name)+17], 16, 64)
		if err != nil {
			return nil, fmt.Errorf("snapshot file name %q: %w", fname, err)
		}

		history = append(history, seqNo)
	}

	return history, nil
}

// LastCommon returns the highest sequence number every name has in common with
// each other, or zero for no overlap at all.
func LastCommon(a Archive, names ...string) (seqNo uint64, err error) {
	if len(names) == 0 {
		return 0, nil
	}
	histories := make([][]uint64, len(names))
	for i, name := range names {
		var err error
		histories[i], err = a.History(name)
		if err != nil {
			return 0, err
		}
	}

	options := histories[0]
	others := histories[1:]
MatchOptions:
	for len(options) != 0 {
		// pop highest sequence number
		last := options[len(options)-1]
		options = options[:len(options)-1]

		for _, o := range others {
			_, found := sort.Find(len(o), func(i int) int {
				switch v := o[i]; {
				case last < v:
					return -1
				case last > v:
					return 1
				}
				return 0
			})
			if !found {
				continue MatchOptions
			}
		}
		// last in each history
		return last, nil
	}

	return 0, nil
}
