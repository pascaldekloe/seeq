package streamtest_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/pascaldekloe/seeq/stream"
	"github.com/pascaldekloe/seeq/stream/streamtest"
)

// enure interface compliance
var _ = stream.Reader((*streamtest.MockReader)(nil))

func TestMockReader(t *testing.T) {
	t.Run("EmptyBucket", func(t *testing.T) {
		m := new(streamtest.MockReader)
		if n, err := m.ReadRecords(nil); n != 0 || err != io.EOF {
			t.Errorf("read empty queue got (%d, %v), want (0, EOF)", n, err)
		}

		m.Queue = []stream.Record{{}}
		if n, err := m.ReadRecords(nil); n != 0 || err != nil {
			t.Errorf("read enqueued got (%d, %v), want (0, nil)", n, err)
		}
	})

	t.Run("ReadSingles", func(t *testing.T) {
		const first, second = "first payload", "second payload"
		mock := &streamtest.MockReader{
			Queue: []stream.Record{
				{"text", []byte(first)},
				{"text", []byte(second)},
			},
		}

		var basket [1]stream.Record
		n, err := mock.ReadRecords(basket[:])
		if n != 1 || err != nil {
			t.Errorf("first read got (%d, %v), want (1, nil)", n, err)
		}
		if got := basket[0].Payload; string(got) != first {
			t.Errorf("first read got %q, want %q", got, first)
		}

		// reuse basket
		n, err = mock.ReadRecords(basket[:])
		if n != 1 || err != io.EOF {
			t.Errorf("second read got (%d, %v), want (1, EOF)", n, err)
		}
		if got := basket[0].Payload; string(got) != second {
			t.Errorf("second read got %q, want %q", got, second)
		}
	})

	t.Run("CustomErr", func(t *testing.T) {
		customErr := errors.New("test error")
		mock := &streamtest.MockReader{
			Queue: []stream.Record{{"text", nil}},
			Err:   customErr,
		}

		var basket [2]stream.Record
		n, err := mock.ReadRecords(basket[:])
		switch {
		case n != 1:
			t.Errorf("read got %d records, want 1", n)
		case basket[0].MediaType != "text":
			t.Errorf(`read got media type %q, want "text"`, basket[0].MediaType)
		}
		switch err {
		case nil:
			t.Error("read got no error")
		case customErr:
			break // OK
		default:
			t.Errorf("read got error %q, want %q", err, customErr)
		}
	})
}

func TestMockReaderRoutines(t *testing.T) {
	mock := &streamtest.MockReader{
		Queue: []stream.Record{
			{"text", []byte{'A'}},
			{"text", []byte{'B'}},
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)

		// read in other routine
		var buf [1]stream.Record
		n, err := mock.ReadRecords(buf[:1])
		if n != 1 || err != nil {
			t.Fatalf("first read got (%d, %v), want (1, nil)", n, err)
		}
	}()
	<-done // awaits first read

	const want = "read from multiple goroutines"
	// now try from this routine
	var buf [1]stream.Record
	n, err := mock.ReadRecords(buf[:1])
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("second read got (%d, %v), want error with %q", n, err, want)
	}
}

func TestChannelReader(t *testing.T) {
	t.Run("EmptyBucket", func(t *testing.T) {
		r, ch := streamtest.ChannelReader(12)
		n, err := r.ReadRecords(nil)
		if n != 0 || err != io.EOF {
			t.Errorf("read on empty channel got (%d, %v), want (0, EOF)", n, err)
		}

		ch <- stream.Record{}
		n, err = r.ReadRecords(nil)
		if n != 0 || err != nil {
			t.Errorf("read on channel content got (%d, %v), want (0, nil)", n, err)
		}
	})

	t.Run("Refill", func(t *testing.T) {
		r, ch := streamtest.ChannelReader(99)
		var buf [7]stream.Record

		for i := 0; i < len(buf); i++ {
			ch <- stream.Record{}
		}
		n, err := r.ReadRecords(buf[:])
		if n != len(buf) || err != io.EOF {
			t.Errorf("initial read got (%d, %v), want (%d, EOF)", n, err, len(buf))
		}
		n, err = r.ReadRecords(buf[:])
		if n != 0 || err != io.EOF {
			t.Errorf("read after EOF got (%d, %v), want (0, EOF)", n, err)
		}

		for i := 0; i < 2*len(buf); i++ {
			ch <- stream.Record{}
		}
		n, err = r.ReadRecords(buf[:])
		if n != len(buf) || err != nil {
			t.Errorf("read refill got (%d, %v), want (%d, nil)", n, err, len(buf))
		}
		n, err = r.ReadRecords(buf[:])
		if n != len(buf) || err != io.EOF {
			t.Errorf("read refill got (%d, %v), want (%d, EOF)", n, err, len(buf))
		}
		n, err = r.ReadRecords(buf[:])
		if n != 0 || err != io.EOF {
			t.Errorf("read after EOF got (%d, %v), want (0, EOF)", n, err)
		}
	})
}
