//go:build unix && !omitunix

// Package snapshot provides aggregate state persistence. Serials are identified
// by the aggregate name and the position of the input stream.
package snapshot

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

// NewFileDir returns an Archive based on plain files in the directory specified
// by path.
func NewFileDir(path string) Archive { return fileDir(path) }

type fileDir string

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
	return fileProduction{f: f}, nil
}

type fileProduction struct {
	f   *os.File
	err error
}

// Write implements the io.Writer interface.
func (p fileProduction) Write(bytes []byte) (n int, err error) {
	if p.err == nil {
		_, p.err = p.f.Write(bytes)
	}
	return len(bytes), nil
}

// Commit implements the Production interface.
func (p fileProduction) Commit() error {
	var syncErr error
	if p.err == nil {
		syncErr = p.f.Sync()
	}

	name := p.f.Name()
	// don't care about errors after fsync(2)
	p.f.Close()

	if p.err != nil {
		// leave .spool file
		return p.err
	}
	if syncErr != nil {
		// leave .spool file
		return syncErr
	}

	// swap .spool suffix
	return os.Rename(name, name[:len(name)-6]+".snapshot")
}

// Abort implements the Production interface.
func (p fileProduction) Abort() error {
	name := p.f.Name()
	p.f.Close()
	removeErr := os.Remove(name)
	if p.err != nil {
		return p.err
	}
	return removeErr
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
