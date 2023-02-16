package stream_test

import (
	"fmt"
	"testing"

	"github.com/pascaldekloe/seeq/stream"
	"github.com/pascaldekloe/seeq/stream/streamtest"
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

func ExampleCursor() {
	// demo input
	r := streamtest.NewFixedReader(
		stream.Entry{"text", []byte("Ⅰ")},
		stream.Entry{"text", []byte("Ⅱ")},
		stream.Entry{"text", []byte("Ⅲ")},
		stream.Entry{"text", []byte("Ⅳ")},
		stream.Entry{"text", []byte("Ⅴ")},
	)

	// cursor setup
	const batchSize = 3
	c := stream.Cursor{R: r, Batch: make([]stream.Entry, 0, batchSize)}

	// print input
	for {
		err := c.Next()
		fmt.Printf("№ %d: ", c.SeqNo)
		for i := range c.Batch {
			fmt.Printf("%s ", c.Batch[i].Payload)
		}
		fmt.Println(err)
		if err != nil {
			return
		}
	}
	// Output:
	// № 3: Ⅰ Ⅱ Ⅲ <nil>
	// № 5: Ⅳ Ⅴ EOF
}
