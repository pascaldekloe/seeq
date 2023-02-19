package stream_test

import (
	"testing"

	"github.com/pascaldekloe/seeq/stream"
)

func TestDeepCopy(t *testing.T) {
	var tests = [][]stream.Entry{
		{},
		{{}},
		{{"text", nil}, {"", []byte{'A'}}},
	}

	for _, entries := range tests {
		clone := stream.DeepCopy(entries...)
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
