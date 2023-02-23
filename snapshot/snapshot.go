// Package snapshot provides aggregate state persistence. Serials are identified
// by the aggregate name and the position of the input stream.
package snapshot

import (
	"io"
	"sort"
)

// Production must end with either a Commit or an Abort. Write never returns an
// error. Write errors are delayed to Commit or Abort instead.
type Production interface {
	io.Writer      // appends
	Commit() error // applies
	Abort() error  // discards
}

// Archive manages a snapshot collection.
type Archive interface {
	// Open fetches a serial, with fs.ErrNotExist on absense.
	Open(name string, seqNo uint64) (io.ReadCloser, error)
	// Make persists a serial. It may overwrite an existing one.
	Make(name string, seqNo uint64) (Production, error)

	// History lists each sequence number available in ascending order.
	History(name string) ([]uint64, error)
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
