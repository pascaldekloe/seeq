package seeq_test

import (
	"testing"

	"github.com/pascaldekloe/seeq"
	"github.com/pascaldekloe/seeq/stream"
	"github.com/pascaldekloe/seeq/stream/streamtest"
)

func TestSyncEach(t *testing.T) {
	tests := [][]stream.Entry{
		{},
		{{MediaType: "text/plain", Payload: []byte("foo")}},
		{{}, {MediaType: "text/void"}, {}},
	}

	run := func(buf []stream.Entry) {
		for _, test := range tests {
			r := streamtest.NewFixedReader(test...)
			rec1 := make(Recording, 0)
			rec2 := make(Recording, 0)
			_, err := seeq.SyncEach(r, buf, &rec1, &rec2)
			if err != nil {
				t.Errorf("got error %q for: %+v", err, test)
				continue
			}

			rec1.VerifyEqual(t, test...)
			rec2.VerifyEqual(t, test...)
		}
	}

	t.Run("Singles", func(t *testing.T) {
		run(make([]stream.Entry, 1))
	})
	t.Run("Batch2", func(t *testing.T) {
		run(make([]stream.Entry, 2))
	})
}

func minimialConstructor[T any]() (*T, error) { return new(T), nil }

// TestNewReleaseSyncError seals user-friendly errors.
func TestNewReleaseSyncError(t *testing.T) {
	t.Run("NotStruct", func(t *testing.T) {
		type FaultyConfig []seeq.Aggregate[stream.Entry]
		_, err := seeq.NewReleaseSync(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "aggregate group seeq_test.FaultyConfig is of kind slice—not struct"
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})

	t.Run("NoAggs", func(t *testing.T) {
		type FaultyConfig struct{ Foo *TextStats }
		_, err := seeq.NewReleaseSync(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "aggregate group seeq_test.FaultyConfig has no aggregate tags"
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})

	t.Run("Interface", func(t *testing.T) {
		type FaultyConfig struct {
			Agg1 string `aggregate:"test-agg"`
		}
		_, err := seeq.NewReleaseSync(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "aggregate group seeq_test.FaultyConfig, field Agg1, type string does not implement seeq.Aggregate[github.com/pascaldekloe/seeq/stream.Entry]"
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})

	t.Run("Export", func(t *testing.T) {
		type FaultyConfig struct {
			agg1 string `aggregate:"test-agg"`
		}
		_, err := seeq.NewReleaseSync(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "aggregate group seeq_test.FaultyConfig, field agg1 is not exported; first letter must upper-case"
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})

	t.Run("NameDupe", func(t *testing.T) {
		type FaultyConfig struct {
			Agg1 *TextStats `aggregate:"test-agg"`
			Agg2 *Recording `aggregate:"test-agg"`
		}
		_, err := seeq.NewReleaseSync(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "aggregate group seeq_test.FaultyConfig has both field Agg1 and field Agg2 tagged as \"test-agg\""
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})
}
