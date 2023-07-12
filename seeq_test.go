package seeq_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/pascaldekloe/seeq"
	"github.com/pascaldekloe/seeq/stream"
	"github.com/pascaldekloe/seeq/stream/streamtest"
)

// Recording is seeq.Aggregate for testing.
type Recording []stream.Entry

// AddNext implements the seeq.Aggregate interface.
func (rec *Recording) AddNext(batch []stream.Entry, offset uint64) error {
	if offset != uint64(len(*rec)) {
		return fmt.Errorf("recording aggregate got offset %d, want %d", offset, len(*rec))
	}
	*rec = stream.AppendCopy(*rec, batch...)
	return nil
}

// DumpTo implements the seeq.Snapshotable interface.
func (rec *Recording) DumpTo(w io.Writer) error {
	return json.NewEncoder(w).Encode(rec)
}

// LoadFrom implements the seeq.Snapshotable interface.
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

// DumpTo implements the seeq.Snapshotable interface.
func (fail FailingSnapshot) DumpTo(w io.Writer) error {
	if fail.AfterNBytes > 0 {
		_, err := w.Write(make([]byte, fail.AfterNBytes))
		if err != nil {
			return fmt.Errorf("failing snapshot dump got error before %d bytes: %w", fail.AfterNBytes, err)
		}
	}
	return fail.Err
}

// LoadFrom implements the seeq.Snapshotable interface.
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
		err := seeq.Copy(dst, src, nil)

		// dst should receive the error from src
		const want = "failing snapshot load got error before 99 bytes: DumpNone test error"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("DumpSome", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 2, Err: errors.New("DumpSome test error")}
		dst := FailingSnapshot{AfterNBytes: 99}
		err := seeq.Copy(dst, src, nil)

		// dst should receive the error from src
		const want = "failing snapshot load got error before 99 bytes: DumpSome test error"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("DumpEOF", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 2, Err: io.EOF}
		dst := FailingSnapshot{AfterNBytes: 99}
		err := seeq.Copy(dst, src, nil)

		// dst should receive the error from src
		const want = "failing snapshot load got error before 99 bytes: aggregate snapshot dump did EOF"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("LoadNone", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 99}
		dst := FailingSnapshot{Err: errors.New("LoadNone test error")}
		err := seeq.Copy(dst, src, nil)

		const want = "LoadNone test error"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("LoadSome", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 99}
		dst := FailingSnapshot{AfterNBytes: 2, Err: errors.New("LoadSome test error")}
		err := seeq.Copy(dst, src, nil)

		const want = "LoadSome test error"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("IgnoreSome", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 10}
		dst := FailingSnapshot{AfterNBytes: 2}
		err := seeq.Copy(dst, src, nil)

		const want = "aggregate seeq_test.FailingSnapshot left 8 bytes after snapshot load"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})

	t.Run("ErrAfter", func(t *testing.T) {
		src := FailingSnapshot{AfterNBytes: 10, Err: errors.New("ErrAfter test error")}
		dst := FailingSnapshot{AfterNBytes: 2, Err: nil}
		err := seeq.Copy(dst, src, nil)

		const want = "aggregate seeq_test.FailingSnapshot snapshot dump after load: ErrAfter test error"
		if err == nil || err.Error() != want {
			t.Errorf("got error %q, want %q", err, want)
		}
	})
}

func TestSync(t *testing.T) {
	tests := [][]stream.Entry{
		{},
		{{MediaType: "text/plain", Payload: []byte("foo")}},
		{{}, {MediaType: "text/void"}, {}},
	}

	runBatchSize := func(batchSize int) {
		for _, test := range tests {
			r := streamtest.NewFixedReader(test...)
			rec := make(Recording, 0)
			_, err := seeq.Sync(&rec, r, make([]stream.Entry, batchSize))
			if err != nil {
				t.Errorf("got error %q for: %+v", err, test)
				continue
			}

			rec.VerifyEqual(t, test...)
		}
	}

	t.Run("Singles", func(t *testing.T) {
		runBatchSize(1)
	})
	t.Run("Batch2", func(t *testing.T) {
		runBatchSize(2)
	})
}

func TestAsync(t *testing.T) {
	tests := [][]stream.Entry{
		{},
		{{MediaType: "text/plain", Payload: []byte("foo")}},
		{{}, {MediaType: "text/void"}, {}},
		{
			{MediaType: "text/a"},
			{MediaType: "text/b"},
			{MediaType: "text/c"},
			{MediaType: "text/d"},
			{MediaType: "text/e"},
			{MediaType: "text/f"},
		},
	}

	runBatchSize := func(batchSize int) {
		for _, test := range tests {
			r := streamtest.NewFixedReader(test...)
			rec := make(Recording, 0)
			_, err := seeq.Async(&rec, r, batchSize)
			if err != nil {
				t.Errorf("got error %q for: %+v", err, test)
				continue
			}

			rec.VerifyEqual(t, test...)
		}
	}

	t.Run("Singles", func(t *testing.T) {
		runBatchSize(1)
	})
	t.Run("Batch2", func(t *testing.T) {
		runBatchSize(2)
	})
}

type delayAgg[T any] struct {
	time.Duration
	*time.Timer
}

func newDelayAgg[T any](delay time.Duration) seeq.Aggregate[T] {
	t := time.NewTimer(42)
	if !t.Stop() {
		<-t.C
	}
	return delayAgg[T]{delay, t}
}

func (delay delayAgg[T]) AddNext(batch []T, offset uint64) error {
	delay.Timer.Reset(time.Duration(len(batch)) * delay.Duration)
	<-delay.Timer.C
	return nil
}

func BenchmarkSyncAndAsync(b *testing.B) {
	source := streamtest.RepeatReader{
		MediaType: "text/bench",
		Payload:   []byte("something small"),
	}

	b.Run("Read1µs", func(b *testing.B) {
		r := streamtest.DelayReader(&source, time.Microsecond)
		b.Run("Agg1µs", func(b *testing.B) {
			a := newDelayAgg[stream.Entry](time.Microsecond)
			b.Run("Batch12", func(b *testing.B) {
				benchmarkSyncAndAsync(b, a, r, 12)
			})
			b.Run("Batch60", func(b *testing.B) {
				benchmarkSyncAndAsync(b, a, r, 60)
			})
		})
	})

	b.Run("Read1ms", func(b *testing.B) {
		b.Run("Agg1ms", func(b *testing.B) {
			a := newDelayAgg[stream.Entry](time.Millisecond)
			b.Run("Batch12", func(b *testing.B) {
				r := streamtest.DelayReader(&source, 12*time.Millisecond)
				benchmarkSyncAndAsync(b, a, r, 12)
			})
			b.Run("Batch60", func(b *testing.B) {
				r := streamtest.DelayReader(&source, 60*time.Millisecond)
				benchmarkSyncAndAsync(b, a, r, 60)
			})
		})
	})
}

func benchmarkSyncAndAsync(b *testing.B, a seeq.Aggregate[stream.Entry], r stream.Reader, batchSize int) {
	b.Run("Sync", func(b *testing.B) {
		_, err := seeq.Sync(a, &stream.LimitReader{r, int64(b.N)}, make([]stream.Entry, batchSize))
		if err != nil {
			b.Fatal("Sync error: ", err)
		}
	})
	b.Run("Async", func(b *testing.B) {
		_, err := seeq.Async(a, &stream.LimitReader{r, int64(b.N)}, batchSize)
		if err != nil {
			b.Fatal("Async error: ", err)
		}
	})
}
