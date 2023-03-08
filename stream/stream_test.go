package stream_test

import (
	"fmt"
	"testing"

	"github.com/pascaldekloe/seeq/stream"
)

func TestCloneAll(t *testing.T) {
	var tests = [][]stream.Entry{
		{},
		{{}},
		{{"text", nil}, {"", []byte{'A'}}},
	}

	for _, entries := range tests {
		clone := stream.CloneAll(entries...)
		if len(clone) != len(entries) {
			t.Errorf("got %d entries, want %d", len(clone), len(entries))
			continue
		}
		for i := range clone {
			if clone[i].MediaType != entries[i].MediaType {
				t.Errorf("media type at index %d got %q, want %q", i, clone[i].MediaType, entries[i].MediaType)
			}
			if string(clone[i].Payload) != string(entries[i].Payload) {
				t.Errorf("payload at index %d got %q, want %q", i, clone[i].Payload, entries[i].Payload)
			}
			if len(clone[i].Payload) != 0 && &clone[i].Payload[0] == &entries[i].Payload[0] {
				t.Errorf("payload at index %d equals input", i)
			}
		}
	}
}

func TestCloneAllAlloc(t *testing.T) {
	e1 := stream.Entry{"text", []byte("one")}
	e2 := stream.Entry{"text", []byte("two")}
	e3 := stream.Entry{"text", []byte("three")}
	e4 := stream.Entry{"text", []byte("four")}
	t.Run("Variadic", func(t *testing.T) {
		avg := testing.AllocsPerRun(1, func() {
			stream.CloneAll(e1, e2, e3, e4)
		})
		if avg != 2 {
			t.Errorf("got %f allocations on average, want 2", avg)
		}
	})

	es := []stream.Entry{e1, e2, e3, e4}
	t.Run("Slice", func(t *testing.T) {
		avg := testing.AllocsPerRun(1, func() {
			stream.CloneAll(es...)
		})
		if avg != 2 {
			t.Errorf("got %f allocations on average, want 2", avg)
		}
	})
}

func ExampleMediaType() {
	// parse rich MIME
	const sample = `application/hal+json;profile="https://example.com/v1"`
	t := stream.CachedMediaType(sample)

	fmt.Println("• type:", t.Type)
	fmt.Println("• subtype:", t.Subtype)
	fmt.Println("• suffix:", t.Suffix)
	if v, found := t.Param("profile"); found {
		fmt.Println("• profile:", v)
	}
	// Output:
	// • type: application
	// • subtype: hal
	// • suffix: json
	// • profile: https://example.com/v1
}
