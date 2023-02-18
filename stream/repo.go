package stream

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

// ErrFuture denies a sequence numbers beyond the available.
var ErrFuture = errors.New("stream position not reached yet")

// Repo is a named stream-collection. A stream can have only one Writer. Any
// amount of Readers are permitted.
type Repo interface {
	// ReadAt opens a stream by name and it skips the offset amount of
	// entries before the first Read.
	ReadAt(name string, offset uint64) ReadCloser

	// AppendTo opens a stream by name. A fail-safe will deny multiple
	// Writers to append to the same stream.
	AppendTo(name string) WriteCloser
}

// RollingFiles concatenates in batches of ChunkN Entries per file.
type RollingFiles struct {
	Dir    string // filesystem path
	ChunkN uint64 // Entry count per file
}

// Files are stored with their offset in the stream, i.e., the number of entries
// passed.
func (repo *RollingFiles) file(name string, offset uint64) string {
	return filepath.Join(repo.Dir, fmt.Sprintf("%s-%016x", name, offset))
}

// List returns each file present with their corresponding offsets, i.e., the
// number entries passed before. The (files and their offsets) return is sorted.
func (repo *RollingFiles) list(name string) (files []string, offsets []uint64, err error) {
	// TODO(pascaldekloe): Skip glob for more error handling and flexible name content?
	files, err = filepath.Glob(filepath.Join(repo.Dir, name+"-*"))
	if err != nil {
		return nil, nil, err
	}

	offsets = make([]uint64, len(files))
	for i, s := range files {
		offsets[i], err = strconv.ParseUint(s[len(s)-16:], 16, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("malformed sequence number/offset in stream file %q: %w", s, err)
		}
	}

	return files, offsets, nil
}

func (repo *RollingFiles) ReadAt(name string, offset uint64) ReadCloser {
	return &rollingReader{repo: repo, name: name, skipN: offset}
}

type rollingReader struct {
	repo  *RollingFiles
	name  string
	file  *os.File
	dec   Reader
	seqNo uint64 // number of entries passed in the stream
	skipN uint64 // pending number of entries to discard
}

// Close implements the io.Closer interface.
func (roll *rollingReader) Close() error { return roll.file.Close() }

// Read implements the Reader interface.
func (roll *rollingReader) Read(basket []Entry) (n int, err error) {
	// lazy init
	if roll.file == nil {
		files, offsets, err := roll.repo.list(roll.name)
		if err != nil {
			return 0, err
		}
		if len(files) == 0 {
			// archive empty
			if roll.skipN > 0 {
				return 0, ErrFuture
			}
			return 0, io.EOF
		}

		i := sort.Search(len(offsets), func(i int) bool { return offsets[i] >= roll.skipN })
		if i >= len(files) {
			return 0, fmt.Errorf("sequence number %d no longer available; archive starts at %d now", roll.skipN+1, offsets[0]+1)
		}

		f, err := os.Open(files[i])
		if err != nil {
			return 0, err
		}
		roll.file = f
		roll.dec = NewSimpleReader(f)
		roll.seqNo = offsets[i]
		roll.skipN -= offsets[i]
	}

	for discard := basket; roll.skipN > 0; {
		if len(discard) < 2 {
			discard = make([]Entry, 12)
		}
		if uint64(uint(len(discard))) > roll.skipN {
			discard = discard[:roll.skipN]
		}

		n, err := roll.dec.Read(discard)
		roll.seqNo += uint64(uint(n))
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
	roll.seqNo += uint64(uint(n))
	for err == io.EOF {
		// see if there's a follow-up file
		var f *os.File
		f, err = os.Open(roll.repo.file(roll.name, roll.seqNo))
		if err != nil {
			if os.IsNotExist(err) {
				return n, io.EOF
			}
			return n, err
		}
		roll.file.Close() // close previous
		roll.file = f     // switch to next
		roll.dec = NewSimpleReader(f)

		var nn int
		nn, err = roll.dec.Read(basket[n:])
		roll.seqNo += uint64(uint(nn))
		n += nn
	}

	return n, err
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

type errorWriter struct{ err error }

// Write implements the Writer interface.
func (w errorWriter) Write(batch []Entry) error { return w.err }

// Close implements the io.Closer interface.
func (errorWriter) Close() error { return nil }

type rollingWriter struct {
	repo *RollingFiles // parent

	name  string
	seqNo uint64

	file *os.File
	enc  Writer
}

// Close implements the io.Closer interface.
func (roll *rollingWriter) Close() error {
	writeLock.Delete(filepath.Join(roll.repo.Dir, roll.name))
	return roll.file.Close()
}

// Write implements the Writer interface.
func (roll *rollingWriter) Write(batch []Entry) error {
	// lazy init
	if roll.file == nil {
		files, offsets, err := roll.repo.list(roll.name)
		if err != nil {
			return err
		}
		if len(files) == 0 {
			// fresh start
			f, err := os.OpenFile(roll.repo.file(roll.name, 0), os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o640)
			if err != nil {
				return err
			}
			roll.file = f
		} else {
			last := len(files) - 1
			entryN, err := initAndCount(files[last])
			if err != nil {
				return err
			}
			f, err := os.OpenFile(files[last], os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				return err
			}
			roll.seqNo = offsets[last] + uint64(entryN)
			roll.file = f
		}
		roll.enc = NewSimpleWriter(roll.file)
	}

	for len(batch) != 0 {
		// amount of entries in file allready
		fill := roll.seqNo % roll.repo.ChunkN
		// amount of entries remaining before rollover
		space := roll.repo.ChunkN - fill

		if space >= uint64(uint(len(batch))) {
			err := roll.enc.Write(batch)
			if err != nil {
				roll.file.Close() // block
				return err
			}
			roll.seqNo += uint64(uint(len(batch)))
			return nil
		}

		// fill current file
		err := roll.enc.Write(batch[:space])
		if err != nil {
			roll.file.Close() // block
			return err
		}
		roll.seqNo += space    // register
		batch = batch[space:]  // pass
		err = roll.file.Sync() // flush
		roll.file.Close()
		if err != nil {
			return err
		}

		// swap to new file
		f, err := os.OpenFile(roll.repo.file(roll.name, roll.seqNo), os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 640)
		if err != nil {
			return err
		}
		roll.file = f
		roll.enc = NewSimpleWriter(f)
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
