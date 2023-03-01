package seeq_test

import (
	"testing"

	"github.com/pascaldekloe/seeq"
	"github.com/pascaldekloe/seeq/stream"
)

func minimialConstructor[T any]() (*T, error) { return new(T), nil }

// TestGroupError seals user-friendly errors.
func TestGroupError(t *testing.T) {
	t.Run("NotStruct", func(t *testing.T) {
		type FaultyConfig []seeq.Aggregate[stream.Entry]
		_, err := seeq.NewGroup(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "seeq_test.FaultyConfig is of kind slice, need struct"
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})

	t.Run("NoAggs", func(t *testing.T) {
		type FaultyConfig struct{ Foo *WORMStats }
		_, err := seeq.NewGroup(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "seeq_test.FaultyConfig has no aggregate tags"
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})

	t.Run("Interface", func(t *testing.T) {
		type FaultyConfig struct {
			Agg1 string `aggregate:"test-agg"`
		}
		_, err := seeq.NewGroup(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "seeq_test.FaultyConfig field Agg1 type string does not implement seeq.Aggregate[github.com/pascaldekloe/seeq/stream.Entry]"
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})

	t.Run("Export", func(t *testing.T) {
		type FaultyConfig struct {
			agg1 string `aggregate:"test-agg"`
		}
		_, err := seeq.NewGroup(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "seeq_test.FaultyConfig field agg1 is not exported [title-case]"
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})

	t.Run("NameDupe", func(t *testing.T) {
		type FaultyConfig struct {
			Agg1 *WORMStats `aggregate:"test-agg"`
			Agg2 *WORMCheck `aggregate:"test-agg"`
		}
		_, err := seeq.NewGroup(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "seeq_test.FaultyConfig has both field Agg1 and field Agg2 tagged as \"test-agg\""
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})
}
