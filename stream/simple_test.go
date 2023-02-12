package stream_test

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/pascaldekloe/seeq/stream"
)

// ensure interface compliance
var _ = stream.Reader((*stream.SimpleReader)(nil))

func TestSimpleReader(t *testing.T) {
	assertEntry := func(t *testing.T, buf []stream.Entry, i int, wantMediaType, wantPayload string) {
		e := &buf[i]
		if e.MediaType != wantMediaType {
			t.Errorf("entry[%d] got media type %q, want %q", i, e.MediaType, wantMediaType)
		}
		if string(e.Payload) != wantPayload {
			t.Errorf("entry[%d] got payload %q, want %q", i, e.Payload, wantPayload)
		}
	}

	t.Run("EmptyEntries", func(t *testing.T) {
		// two entries, both with zero media type and with zero payload
		const sample = "\x00\x00\x00\x00" + "\x00\x00\x00\x00"
		r := stream.SimpleReader{R: strings.NewReader(sample)}
		buf := make([]stream.Entry, 3)
		n, err := r.Read(buf)
		if err != io.EOF {
			t.Fatal("read error:", err)
		}
		if n != 2 {
			t.Errorf("got %d entries, want 2", n)
		}
		for i := range buf {
			assertEntry(t, buf, i, "", "")
		}
	})

	t.Run("NoData", func(t *testing.T) {
		r := stream.SimpleReader{R: strings.NewReader("")}
		buf := make([]stream.Entry, 2)
		n, err := r.Read(buf)
		if err != io.EOF {
			t.Fatalf("got read error %v, want io.EOF", err)
		}
		if n != 0 {
			t.Errorf("got %d entries, want 0", n)
		}
		for i := range buf {
			assertEntry(t, buf, i, "", "")
		}
	})

	t.Run("1stHeaderTerm", func(t *testing.T) {
		const sample = "\x00\x00\x03" // incomplete header
		r := stream.SimpleReader{R: iotest.OneByteReader(strings.NewReader(sample))}
		buf := make([]stream.Entry, 2)
		n, err := r.Read(buf)
		if err != io.EOF {
			t.Fatalf("got read error %v, want io.EOF", err)
		}
		if n != 0 {
			t.Errorf("got %d entries, want 0", n)
		}
		for i := range buf {
			assertEntry(t, buf, i, "", "")
		}
	})

	t.Run("1stBodyNone", func(t *testing.T) {
		const sample = "\x00\x00\x03\x0A" // absent body
		r := stream.SimpleReader{R: iotest.OneByteReader(strings.NewReader(sample))}
		buf := make([]stream.Entry, 2)
		n, err := r.Read(buf)
		if err != io.EOF {
			t.Fatalf("got read error %v, want io.EOF", err)
		}
		if n != 0 {
			t.Errorf("got %d entries, want 0", n)
		}
		for i := range buf {
			assertEntry(t, buf, i, "", "")
		}
	})

	t.Run("1stBodyTerm", func(t *testing.T) {
		const sample = "\x00\x00\x03\x0Atex" // incomplete body
		r := stream.SimpleReader{R: iotest.OneByteReader(strings.NewReader(sample))}
		buf := make([]stream.Entry, 2)
		n, err := r.Read(buf)
		if err != io.EOF {
			t.Fatalf("got read error %v, want io.EOF", err)
		}
		if n != 0 {
			t.Errorf("got %d entries, want none", n)
		}
		for i := range buf {
			assertEntry(t, buf, i, "", "")
		}
	})

	t.Run("2ndHeaderNone", func(t *testing.T) {
		const sample = "\x00\x00\x03\x0Atext/plainONE"
		r := stream.SimpleReader{R: iotest.OneByteReader(strings.NewReader(sample))}
		buf := make([]stream.Entry, 2)
		n, err := r.Read(buf)
		if err != io.EOF {
			t.Fatalf("got read error %v, want io.EOF", err)
		}
		if n != 1 {
			t.Errorf("got %d entries, want 1", n)
		}
		assertEntry(t, buf, 0, "text/plain", "ONE")
		assertEntry(t, buf, 1, "", "")
	})

	t.Run("2ndHeaderTerm", func(t *testing.T) {
		const sample = "\x00\x00\x03\x0Atext/plainONE" +
			"\x00" // incomplete header
		r := stream.SimpleReader{R: iotest.OneByteReader(strings.NewReader(sample))}
		buf := make([]stream.Entry, 2)
		n, err := r.Read(buf)
		if err != io.EOF {
			t.Fatalf("got read error %v, want io.EOF", err)
		}
		if n != 1 {
			t.Errorf("got %d entries, want 1", n)
		}
		assertEntry(t, buf, 0, "text/plain", "ONE")
		assertEntry(t, buf, 1, "", "")
	})

	t.Run("2ndBodyNone", func(t *testing.T) {
		const sample = "\x00\x00\x03\x0Atext/plainONE" +
			"\x00\x00\x03\x0A" // absent body
		r := stream.SimpleReader{R: iotest.OneByteReader(strings.NewReader(sample))}
		buf := make([]stream.Entry, 2)
		n, err := r.Read(buf)
		if err != io.EOF {
			t.Fatalf("got read error %v, want io.EOF", err)
		}
		if n != 1 {
			t.Errorf("got %d entries, want 1", n)
		}
		assertEntry(t, buf, 0, "text/plain", "ONE")
		assertEntry(t, buf, 1, "", "")
	})

	t.Run("2ndBodyTerm", func(t *testing.T) {
		const sample = "\x00\x00\x03\x0Atext/plainONE" +
			"\x00\x00\x03\x0Atext/plainT" // incomplete body
		r := stream.SimpleReader{R: iotest.OneByteReader(strings.NewReader(sample))}
		buf := make([]stream.Entry, 2)
		n, err := r.Read(buf)
		if err != io.EOF {
			t.Fatalf("got read error %v, want io.EOF", err)
		}
		if n != 1 {
			t.Errorf("got %d entries, want 1", n)
		}
		assertEntry(t, buf, 0, "text/plain", "ONE")
		assertEntry(t, buf, 1, "", "")
	})

	t.Run("ReadFull", func(t *testing.T) {
		const sample = "\x00\x00\x03\x1Btext/plain;charset=us-asciiONE" +
			"\x00\x00\x03\x0Atext/plainTWO"
		r := stream.SimpleReader{R: iotest.OneByteReader(strings.NewReader(sample))}
		buf := make([]stream.Entry, 2)
		n, err := r.Read(buf)
		if err != io.EOF {
			t.Fatalf("got read error %v, want io.EOF", err)
		}
		if n != 2 {
			t.Errorf("got %d entries, want 2", n)
		}
		assertEntry(t, buf, 0, "text/plain;charset=us-ascii", "ONE")
		assertEntry(t, buf, 1, "text/plain", "TWO")
	})
}

func TestSimpleReaderAllocs(t *testing.T) {
	const sample = "\x00\x00\x03\x0Atext/plainONE" +
		"\x00\x00\x03\x0Atext/plainTWO" +
		"\x00\x00\x05\x0Atext/plainTHREE"
	r := stream.SimpleReader{R: strings.NewReader(sample)}

	// read one entry at a time
	var buf [1]stream.Entry

	// The anonymous func gets invoked 3 times: 1 warm-up + 2 measured runs.
	allocAvg := testing.AllocsPerRun(2, func() {
		n, err := r.Read(buf[:])
		if err != nil && err != io.EOF {
			t.Fatal("read error:", err)
		}
		if n != 1 {
			t.Fatalf("got %d entries, want 1", n)
		}
		if len(buf[0].MediaType) == 0 {
			t.Errorf("media type missing")
		}
		if len(buf[0].Payload) == 0 {
			t.Errorf("payload missing")
		}
	})

	// Only the first Read, which is ignored from the average,
	// may allocate the read buffer and the media-type string.
	if allocAvg != 0 {
		t.Errorf("got %f memory alloctions on average, want none", allocAvg)
	}
}

func FuzzSimpleReader(f *testing.F) {
	f.Add([]byte("\x00\x00\x05\x0Atext/plainéén"+
		"\x00\x00\x03\x0Atext/plaintwee"),
		uint8(2),
	)
	f.Fuzz(func(t *testing.T, in []byte, n uint8) {
		r := stream.SimpleReader{R: bytes.NewReader(in)}

		var buf [3]stream.Entry
		_, err := r.Read(buf[:n&3])
		switch err {
		case nil, io.EOF:
			break // OK
		default:
			t.Error("read got error:", err)
		}
	})
}
