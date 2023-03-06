package seeq_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/pascaldekloe/seeq"
	"github.com/pascaldekloe/seeq/stream"
)

// Recording is seeq.Aggregate for testing.
type Recording []stream.Entry

// AddNext implements the seeq.Aggregate interface.
func (rec *Recording) AddNext(batch []stream.Entry, offset uint64) error {
	if offset != uint64(len(*rec)) {
		return fmt.Errorf("recording aggregate got offset %d, want %d", offset, len(*rec))
	}
	*rec = append(*rec, stream.DeepCopy(batch...)...)
	return nil
}

// DumpTo implements the seeq.Aggregate interface.
func (rec *Recording) DumpTo(w io.Writer) error {
	return json.NewEncoder(w).Encode(rec)
}

// LoadFrom implements the seeq.Aggregate interface.
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

// FailingSnapshot is seeq.Aggregate for testing. Both DumpTo and LoadFrom
// return Err AfterNBytes.
type FailingSnapshot struct {
	Err         error
	AfterNBytes int64
}

// AddNext implements the seeq.Aggregate interface.
func (fail FailingSnapshot) AddNext(batch []stream.Entry, offset uint64) error { return nil }

// DumpTo implements the seeq.Aggregate interface.
func (fail FailingSnapshot) DumpTo(w io.Writer) error {
	if fail.AfterNBytes > 0 {
		_, err := w.Write(make([]byte, fail.AfterNBytes))
		if err != nil {
			return fmt.Errorf("failing snapshot dump got error before %d bytes: %w", fail.AfterNBytes, err)
		}
	}
	return fail.Err
}

// LoadFrom implements the seeq.Aggregate interface.
func (fail FailingSnapshot) LoadFrom(r io.Reader) error {
	if fail.AfterNBytes > 0 {
		n, err := io.CopyN(io.Discard, r, fail.AfterNBytes)
		if n < fail.AfterNBytes {
			return fmt.Errorf("failing snapshot load got error before %d bytes: %w", fail.AfterNBytes, err)
		}
	}
	return fail.Err
}

func TestCopyError(t *testing.T) {
	t.Run("DumpNone", func(t *testing.T) {
		src := FailingSnapshot{Err: errors.New("DumpNone test error")}
		dst := FailingSnapshot{AfterNBytes: 99}
		err := seeq.Copy[stream.Entry](dst, src, nil)

		// dst should receive the error from src
		const want = "failing snapshot load got error before 99 bytes: DumpNone test error"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("DumpSome", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 2, Err: errors.New("DumpSome test error")}
		dst := FailingSnapshot{AfterNBytes: 99}
		err := seeq.Copy[stream.Entry](dst, src, nil)

		// dst should receive the error from src
		const want = "failing snapshot load got error before 99 bytes: DumpSome test error"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("DumpEOF", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 2, Err: io.EOF}
		dst := FailingSnapshot{AfterNBytes: 99}
		err := seeq.Copy[stream.Entry](dst, src, nil)

		// dst should receive the error from src
		const want = "failing snapshot load got error before 99 bytes: aggregate snapshot dump did EOF"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("LoadNone", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 99}
		dst := FailingSnapshot{Err: errors.New("LoadNone test error")}
		err := seeq.Copy[stream.Entry](dst, src, nil)

		const want = "LoadNone test error"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("LoadSome", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 99}
		dst := FailingSnapshot{AfterNBytes: 2, Err: errors.New("LoadSome test error")}
		err := seeq.Copy[stream.Entry](dst, src, nil)

		const want = "LoadSome test error"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("IgnoreSome", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 10}
		dst := FailingSnapshot{AfterNBytes: 2}
		err := seeq.Copy[stream.Entry](dst, src, nil)

		const want = "aggregate seeq_test.FailingSnapshot left 8 bytes after snapshot load"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("ErrAfter", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 10, Err: errors.New("ErrAfter test error")}
		dst := FailingSnapshot{AfterNBytes: 2, Err: nil}
		err := seeq.Copy[stream.Entry](dst, src, nil)

		const want = "aggregate seeq_test.FailingSnapshot snapshot dump after load: ErrAfter test error"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})
}
