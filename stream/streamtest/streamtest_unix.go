//go:build unix && !omitunix

// Package streamtest provides utilities for tests with streams.
package streamtest

import (
	"testing"

	"github.com/pascaldekloe/seeq/stream"
)

// NewRepoWith populates a repository for testing. The files are automatically
// removed when the test and all its subtests complete.
func NewRepoWith(t testing.TB, name string, entries ...stream.Entry) stream.Repo {
	t.Helper()

	repo := stream.RollingFiles{
		Dir:    t.TempDir(),
		ChunkN: 5,
	}

	w := repo.AppendTo(name)
	if err := w.Write(entries); err != nil {
		t.Errorf("stream %q write error: %s", name, err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("stream %q close error: %s", name, err)
	}

	return &repo
}
