package seeq

import (
	"encoding/json"
	"io"
	"testing"

	"github.com/pascaldekloe/seeq/stream"
	"github.com/pascaldekloe/seeq/stream/streamtest"
)

type Recording []stream.Entry

// AddNext implements the seeq.Aggregate interface.
func (rec *Recording) AddNext(batch []stream.Entry) {
	*rec = append(*rec, stream.DeepCopy(batch...)...)
}

// DumpTo implements the seeq.Aggregate interface.
func (rec *Recording) DumpTo(w io.Writer) error {
	return json.NewEncoder(w).Encode(rec)
}

// LoadTo implements the seeq.Aggregate interface.
func (rec *Recording) LoadFrom(r io.Reader) error {
	return json.NewDecoder(r).Decode(rec)
}

func (rec Recording) VerifyEqual(t testing.TB, want ...stream.Entry) (ok bool) {
	t.Helper()

	if len(rec) != len(want) {
		t.Errorf("aggregate got %d stream-entries, want %d", len(rec), len(want))
		ok = false
	}

	n := len(rec)
	if len(want) < n {
		n = len(want)
	}

	for i := 0; i < n; i++ {
		if rec[i].MediaType != want[i].MediaType || string(rec[i].Payload) != string(want[i].Payload) {
			t.Errorf("aggregate got entry № %d type %q, payload %q, want type %q, payload %q",
				i+1, rec[i].MediaType, rec[i].Payload, want[i].MediaType, want[i].Payload)
			ok = false
		}
	}
	return
}

func TestSyncAll(t *testing.T) {
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
			_, err := SyncEach(r, buf, &rec1, &rec2)
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
