//go:build unix && !omitunix

package stream

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

// RollingFiles concatenates in batches of ChunkN Entries per file.
type RollingFiles struct {
	Dir    string // filesystem path
	ChunkN uint64 // Entry count per file
}

// Files are stored with their offset in the stream, i.e., the number of entries
// passed.
func (repo *RollingFiles) file(name string, offset uint64) string {
	return filepath.Join(repo.Dir, fmt.Sprintf("%s-%016x.chunk", name, offset))
}

type fileOffsets []uint64

func (o fileOffsets) Len() int           { return len(o) }
func (o fileOffsets) Less(i, j int) bool { return o[i] < o[j] }
func (o fileOffsets) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

// List returns each file for the stream name present, listed by offset, sorted.
func (repo *RollingFiles) list(name string) (fileOffsets, error) {
	d, err := os.Open(filepath.Clean(repo.Dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	files, err := d.Readdirnames(-1)
	d.Close()
	if err != nil {
		return nil, err
	}

	var offsets fileOffsets
	for _, s := range files {
		// filter "${name}-????????????????.chunk"
		if len(s) != len(name)+23 ||
			s[:len(name)] != name ||
			s[len(name)] != '-' ||
			s[len(s)-6:] != ".chunk" {
			continue
		}

		u, err := strconv.ParseUint(s[len(s)-22:len(s)-6], 16, 64)
		if err != nil {
			continue // suspicious
		}
		offsets = append(offsets, u)
	}

	sort.Sort(offsets)
	return offsets, nil
}

func (repo *RollingFiles) ReadAt(name string, offset uint64) ReadCloser {
	return &rollingReader{repo: repo, name: name, skipN: offset}
}

type rollingReader struct {
	repo   *RollingFiles
	name   string
	file   *os.File
	dec    Reader
	offset uint64 // number of entries passed in the stream
	skipN  uint64 // pending number of entries to discard
}

// Close implements the io.Closer interface.
func (roll *rollingReader) Close() error {
	if roll.file == nil {
		return nil
	}
	return roll.file.Close()
}

// Read implements the Reader interface.
func (roll *rollingReader) Read(basket []Entry) (n int, err error) {
	// lazy init
	if roll.file == nil {
		offsets, err := roll.repo.list(roll.name)
		if err != nil {
			return 0, err
		}
		if len(offsets) == 0 {
			// archive empty
			if roll.skipN > 0 {
				return 0, ErrFuture
			}
			return 0, io.EOF
		}

		i := sort.Search(len(offsets), func(i int) bool { return offsets[i] >= roll.skipN })
		if i >= len(offsets) {
			return 0, fmt.Errorf("stream offset %d no longer available; archive starts at %d now", roll.skipN, offsets[0])
		}
		offset := offsets[i]

		f, err := os.Open(roll.repo.file(roll.name, offset))
		if err != nil {
			return 0, err
		}
		roll.file = f
		roll.dec = NewFramedReader(f, offset)
		roll.skipN -= offset
	}

	for discard := basket; roll.skipN > 0; {
		if len(discard) < 2 {
			discard = make([]Entry, 12)
		}
		if uint64(uint(len(discard))) > roll.skipN {
			discard = discard[:roll.skipN]
		}

		n, err := roll.dec.Read(discard)
		roll.skipN -= uint64(uint(n))
		switch err {
		case nil:
			continue
		case io.EOF:
			return 0, ErrFuture
		default:
			return 0, err
		}
	}

	n, err = roll.dec.Read(basket)
	for err == io.EOF {
		// see if there's a follow-up file
		offset := roll.dec.Offset()
		var f *os.File
		f, err = os.Open(roll.repo.file(roll.name, offset))
		if err != nil {
			if os.IsNotExist(err) {
				return n, io.EOF
			}
			return n, err
		}
		roll.file.Close() // close previous
		roll.file = f     // switch to next
		roll.dec = NewFramedReader(f, offset)

		var nn int
		nn, err = roll.dec.Read(basket[n:])
		n += nn
	}

	return n, err
}

// Offset implements the Reader interface.
func (roll *rollingReader) Offset() uint64 {
	// lazy init
	if roll.file == nil {
		return roll.skipN
	}
	return roll.dec.Offset()
}

// WriteLock protects against multiple writers on the same stream.
var writeLock sync.Map

func (repo *RollingFiles) AppendTo(name string) WriteCloser {
	w := &rollingWriter{repo: repo, name: name}
	_, loaded := writeLock.LoadOrStore(filepath.Join(repo.Dir, name), w)
	if loaded {
		return errorWriter{fmt.Errorf("stream %q already has a Writer active", name)}
	}
	return w
}

type rollingWriter struct {
	repo *RollingFiles // parent

	name   string
	offset uint64

	file *os.File
	enc  Writer
}

// Close implements the io.Closer interface.
func (roll *rollingWriter) Close() error {
	writeLock.Delete(filepath.Join(roll.repo.Dir, roll.name))
	if roll.file == nil {
		return nil
	}
	return roll.file.Close()
}

// Write implements the Writer interface.
func (roll *rollingWriter) Write(batch []Entry) error {
	// lazy init
	if roll.file == nil {
		offsets, err := roll.repo.list(roll.name)
		if err != nil {
			return err
		}
		if len(offsets) == 0 {
			// fresh start
			path := roll.repo.file(roll.name, 0)
			err = os.MkdirAll(filepath.Dir(path), 0o750)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o640)
			if err != nil {
				return err
			}
			roll.file = f
		} else {
			// open last
			offset := offsets[len(offsets)-1]
			path := roll.repo.file(roll.name, offset)
			entryN, err := initAndCount(path)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				return err
			}
			roll.offset = offset + entryN
			roll.file = f
		}
		roll.enc = NewFramedWriter(roll.file)
	}

	for len(batch) != 0 {
		// amount of entries in file allready
		fill := roll.offset % roll.repo.ChunkN
		// amount of entries remaining before rollover
		space := roll.repo.ChunkN - fill

		if space >= uint64(uint(len(batch))) {
			err := roll.enc.Write(batch)
			if err != nil {
				roll.file.Close() // block
				return err
			}
			roll.offset += uint64(uint(len(batch)))
			return nil
		}

		// fill current file
		err := roll.enc.Write(batch[:space])
		if err != nil {
			roll.file.Close() // block
			return err
		}
		roll.offset += space   // register
		batch = batch[space:]  // pass
		err = roll.file.Sync() // flush
		roll.file.Close()
		if err != nil {
			return err
		}

		// swap to new file
		f, err := os.OpenFile(roll.repo.file(roll.name, roll.offset), os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 640)
		if err != nil {
			return err
		}
		roll.file = f
		roll.enc = NewFramedWriter(f)
	}

	return nil
}

// InitAndCount trims an incomplete entry at the end, if any, and it returns the
// number of entries present/remaining.
func initAndCount(path string) (entryN uint64, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		// read header
		var header [4]byte
		n, err := io.ReadFull(r, header[:])
		switch err {
		case nil:
			break // pass

		case io.EOF:
			return entryN, nil // OK

		case io.ErrUnexpectedEOF:
			// truncate partial record
			// assert r.Buffered() == 0
			pos, err := f.Seek(-int64(n), io.SeekCurrent)
			if err != nil {
				return 0, err
			}
			err = f.Truncate(pos)
			if err != nil {
				return 0, err
			}
			return entryN, nil

		default:
			return 0, err
		}

		// skip body
		u := binary.BigEndian.Uint32(header[:])
		n, err = r.Discard(int(u&0xff + u>>8))
		switch err {
		case nil:
			entryN++ // pass

		case io.EOF:
			// truncate partial record
			// assert r.Buffered() == 0
			pos, err := f.Seek(int64(-len(header)-n), io.SeekCurrent)
			if err != nil {
				return 0, err
			}
			err = f.Truncate(pos)
			if err != nil {
				return 0, err
			}
			return entryN, nil

		default:
			return 0, err
		}
	}
}
