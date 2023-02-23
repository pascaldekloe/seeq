//go:build unix && !omitunix

package snapshot

import (
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
)

func TestFileDirCommit(t *testing.T) {
	const name, seqNo = "x", 1001
	a := newTestFileDir(t)

	const payload = "Hello world!"
	p, err := a.Make(name, seqNo)
	if err != nil {
		t.Fatal("Make got error:", err)
	}
	_, err = io.Copy(p, iotest.OneByteReader(strings.NewReader(payload)))
	if err != nil {
		t.Error("Copy got error:", err)
	}
	if err := p.Commit(); err != nil {
		t.Error("Commit got error:", err)
	}

	h, err := a.History(name)
	if err != nil {
		t.Error("History got error:", err)
	}
	if len(h) != 1 || h[0] != seqNo {
		t.Errorf("History got %d, want [ %d ]", h, seqNo)
	}

	r, err := a.Open(name, seqNo)
	if err != nil {
		t.Fatal("Open got error:", err)
	}
	bytes, err := io.ReadAll(r)
	if err != nil {
		t.Error("ReadAll got error:", err)
	}
	if string(bytes) != payload {
		t.Errorf("Read got %q, want %q", bytes, payload)
	}
}

func TestFileDirAbort(t *testing.T) {
	const name, seqNo = "foo", 42
	a := newTestFileDir(t)

	p, err := a.Make(name, seqNo)
	if err != nil {
		t.Fatal("Make got error:", err)
	}
	_, err = io.WriteString(p, "Hello world!")
	if err != nil {
		t.Error("WriteString got error:", err)
	}
	if err := p.Abort(); err != nil {
		t.Error("Abort got error:", err)
	}

	h, err := a.History(name)
	if err != nil {
		t.Error("History got error:", err)
	}
	if len(h) != 0 {
		t.Errorf("History got %d, want none", h)
	}

	r, err := a.Open(name, seqNo)
	switch {
	case err == nil:
		t.Error("Open aborted got no error")
		r.Close()
	case !errors.Is(err, fs.ErrNotExist):
		t.Errorf("Open got error %v, want a fs.ErrNotExist", err)
	}
}

func TestFileDirRange(t *testing.T) {
	const name, seqNo1, seqNo2, seqNo3 = "test", 42, 99, 0x1234567890abcdef
	a := newTestFileDir(t)

	for _, seqNo := range []uint64{seqNo1, seqNo2, seqNo3} {
		p, err := a.Make(name, seqNo)
		if err != nil {
			t.Fatalf("Make sequent number %d got error: %s", seqNo, err)
		}
		if err := p.Commit(); err != nil {
			t.Errorf("Commit sequent number %d got error: %s", seqNo, err)
		}
	}

	h, err := a.History(name)
	if err != nil {
		t.Error("History got error:", err)
	}
	if len(h) != 3 || h[0] != seqNo1 || h[1] != seqNo2 || h[2] != seqNo3 {
		t.Errorf("History got %d, want [ %d %d %d ]", h, seqNo1, seqNo2, seqNo3)
	}

	r, err := a.Open(name, seqNo3)
	if err != nil {
		t.Fatal("Open got error:", err)
	}
	if err = r.Close(); err != nil {
		t.Error("Close got error:", err)
	}
}

func newTestFileDir(t *testing.T) Archive {
	a := NewFileDir(t.TempDir())
	dir := a.(fileDir)

	t.Cleanup(func() {
		if t.Failed() {
			// print non-hidden files for context
			files, err := filepath.Glob(string(dir) + "/*")
			if err != nil {
				t.Error("archive directory content unavailable:", err)
			} else {
				t.Logf("archive directory contains %q", files)
			}
		}
	})

	return a
}
