//go:build unix && !omitunix

package streamtest_test

import (
	"testing"

	"github.com/pascaldekloe/seeq/stream"
	"github.com/pascaldekloe/seeq/stream/streamtest"
)

func TestNewRepoWith(t *testing.T) {
	const streamName = "foo"
	entries := []stream.Entry{
		{"text", []byte{'A'}},
		{"text", []byte{'B'}},
	}
	repo := streamtest.NewRepoWith(t, streamName, entries...)

	r := repo.ReadAt(streamName, 0)
	streamtest.VerifyContent(t, r, entries...)
	err := r.Close()
	if err != nil {
		t.Error("stream close error:", err)
	}
}
