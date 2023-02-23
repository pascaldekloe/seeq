package streamtest_test

import (
	"errors"
	"io"
	"testing"

	"github.com/pascaldekloe/seeq/stream"
	"github.com/pascaldekloe/seeq/stream/streamtest"
)

func TestReaders(t *testing.T) {
	t.Run("EmptyBasket", func(t *testing.T) {
		r, ch := streamtest.NewChannelReader(1)
		if n, err := r.Read(nil); n != 0 || err != io.EOF {
			t.Errorf("read empty queue got (%d, %v), want (0, EOF)", n, err)
		}

		ch <- stream.Entry{}
		if n, err := r.Read(nil); n != 0 || err != nil {
			t.Errorf("read enqueued got (%d, %v), want (0, nil)", n, err)
		}
	})

	t.Run("DripSingles", func(t *testing.T) {
		a := stream.Entry{"text/turtle", []byte(`<http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> "Spiderman" .`)}
		b := stream.Entry{"text/turtle;charset=utf-8", []byte(`<http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> "Человек-паук"@ru .`)}
		r := streamtest.DripReader(streamtest.NewFixedReader(a, b), 1)

		var basket [3]stream.Entry
		n, err := r.Read(nil)
		if n != 0 || err != io.EOF {
			t.Fatalf("initial read got (%d, %v), want (0, EOF)", n, err)
		}

		n, err = r.Read(basket[:])
		if n != 1 || err != io.EOF {
			t.Fatalf("second read got (%d, %v), want (1, EOF)", n, err)
		}
		if basket[0].MediaType != a.MediaType || string(basket[0].Payload) != string(a.Payload) {
			t.Errorf("second read got %q %q, want %q %q",
				basket[0].MediaType, basket[0].Payload,
				a.MediaType, a.Payload)
		}

		n, err = r.Read(basket[:])
		if n != 1 || err != io.EOF {
			t.Errorf("third read got (%d, %v), want (1, EOF)", n, err)
		}
		if basket[0].MediaType != b.MediaType || string(basket[0].Payload) != string(b.Payload) {
			t.Errorf("third read got %q %q, want %q %q",
				basket[0].MediaType, basket[0].Payload,
				b.MediaType, b.Payload)
		}
	})

	t.Run("CustomErr", func(t *testing.T) {
		r := streamtest.NewFixedReader(
			stream.Entry{"text", []byte("foo")},
			stream.Entry{"text", []byte("bar")},
		)

		testErr := errors.New("test error")
		r = streamtest.ErrorReader(r, testErr)

		n, err := r.Read(make([]stream.Entry, 2))
		if n != 2 {
			t.Errorf("read got %d entries, want 2", n)
		}
		if err != testErr {
			t.Errorf("read got error %v, want %v", err, testErr)
		}
	})
}

func TestReaderRoutines(t *testing.T) {
	r := streamtest.NewFixedReader(
		stream.Entry{"text", []byte{'A'}},
		stream.Entry{"text", []byte{'B'}},
	)

	// read from another routine
	done := make(chan struct{})
	go func() {
		defer close(done)

		var buf [1]stream.Entry
		n, err := r.Read(buf[:])
		if n != 1 || err != nil {
			t.Errorf("first read got (%d, %v), want (1, nil)", n, err)
		}
	}()
	<-done

	const want = "test stream read from multiple goroutines"
	var buf [1]stream.Entry
	n, err := r.Read(buf[:])
	if err == nil || err.Error() != want {
		t.Fatalf("second read got (%d, %v), want (0, %s)", n, err, want)
	}
}
