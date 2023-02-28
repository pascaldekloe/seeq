package seeq_test

import (
	"testing"

	"github.com/pascaldekloe/seeq"
)

func minimialConstructor[T any]() (*T, error) { return new(T), nil }

// TestGroupConfigError seals user-friendly errors.
func TestGroupConfigError(t *testing.T) {
	t.Run("NoAggs", func(t *testing.T) {
		type FaultyConfig struct{ Foo *WORMStats }
		_, err := seeq.NewLightGroup(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "aggregate set seeq_test.FaultyConfig has no aggregate tags"
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})

	t.Run("Interface", func(t *testing.T) {
		type FaultyConfig struct {
			Agg1 string `aggregate:"test-agg"`
		}
		_, err := seeq.NewLightGroup(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "aggregate set seeq_test.FaultyConfig field Agg1 type string does not implement seeq.Aggregate[github.com/pascaldekloe/seeq/stream.Entry]"
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})

	t.Run("Export", func(t *testing.T) {
		type FaultyConfig struct {
			agg1 string `aggregate:"test-agg"`
		}
		_, err := seeq.NewLightGroup(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "aggregate set seeq_test.FaultyConfig field agg1 is not exported"
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})

	t.Run("NameDupe", func(t *testing.T) {
		type FaultyConfig struct {
			Agg1 *WORMStats `aggregate:"test-agg"`
			Agg2 *WORMCheck `aggregate:"test-agg"`
		}
		_, err := seeq.NewLightGroup(minimialConstructor[FaultyConfig])
		if err == nil {
			t.Error("no error")
		}
		const want = "aggregate set seeq_test.FaultyConfig has both field Agg1 and field Agg2 tagged as \"test-agg\""
		if got := err.Error(); got != want {
			t.Errorf("got error %q, want %q", got, want)
		}
	})
}
