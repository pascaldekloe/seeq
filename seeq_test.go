package seeq_test

import (
	"encoding/json"
	"io"
	"testing"

	"github.com/pascaldekloe/seeq/stream"
)

// Recording is seeq.Aggregate for tests.
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
