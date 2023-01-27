package stream_test

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/pascaldekloe/seeq/stream"
)

// enure interface compliance
var _ = stream.Reader((*stream.SimpleReader)(nil))

func TestSimpleReader(t *testing.T) {
	assertRecord := func(t *testing.T, buf []stream.Record, i int, wantMediaType, wantPayload string) {
		r := &buf[i]
		if r.MediaType != wantMediaType {
			t.Errorf("record[%d] got media type %q, want %q", i, r.MediaType, wantMediaType)
		}
		if string(r.Payload) != wantPayload {
			t.Errorf("record[%d] got payload %q, want %q", i, r.Payload, wantMediaType)
		}
	}

	t.Run("EmptyRecords", func(t *testing.T) {
		// two records, both with zero media type and with zero payload
		const sample = "\x00\x00\x00\x00\x00\x00\x00\x00" + "\x00\x00\x00\x00\x00\x00\x00\x00"
		r := stream.SimpleReader{R: strings.NewReader(sample)}
		buf := make([]stream.Record, 3)
		n, err := r.ReadRecords(buf)
		if err != io.EOF {
			t.Fatal("read records error:", err)
		}
		if n != 2 {
			t.Errorf("got %d records, want 2", n)
		}
		for i := range buf {
			assertRecord(t, buf, i, "", "")
		}
	})

	t.Run("SmallReads", func(t *testing.T) {
		const sample = "\x00\x00\x00\x1B\x00\x00\x00\x03text/plain;charset=us-asciiONE" +
			"\x00\x00\x00\x0A\x00\x00\x00\x03text/plainTWO"
		r := stream.SimpleReader{R: iotest.OneByteReader(strings.NewReader(sample))}
		buf := make([]stream.Record, 3)
		n, err := r.ReadRecords(buf)
		if err != io.EOF {
			t.Fatalf("got read records error %v, want io.EOF", err)
		}
		if n != 2 {
			t.Errorf("got %d records, want 2", n)
		}
		assertRecord(t, buf, 0, "text/plain;charset=us-ascii", "ONE")
		assertRecord(t, buf, 1, "text/plain", "TWO")
		assertRecord(t, buf, 2, "", "")
	})

	t.Run("NoData", func(t *testing.T) {
		r := stream.SimpleReader{R: strings.NewReader("")}
		buf := make([]stream.Record, 2)
		n, err := r.ReadRecords(buf)
		if err != io.EOF {
			t.Fatalf("got read records error %v, want io.EOF", err)
		}
		if n != 0 {
			t.Errorf("got %d records, want 0", n)
		}
		for i := range buf {
			assertRecord(t, buf, i, "", "")
		}
	})

	t.Run("1stHeaderTerm", func(t *testing.T) {
		const sample = "\x00\x00\x00\x0A\x00\x00" // incomplete header
		r := stream.SimpleReader{R: strings.NewReader(sample)}
		buf := make([]stream.Record, 2)
		n, err := r.ReadRecords(buf)
		if err != io.EOF {
			t.Fatalf("got read records error %v, want io.EOF", err)
		}
		if n != 0 {
			t.Errorf("got %d records, want 0", n)
		}
		for i := range buf {
			assertRecord(t, buf, i, "", "")
		}
	})

	t.Run("1stBodyNone", func(t *testing.T) {
		const sample = "\x00\x00\x00\x0A\x00\x00\x00\x03" // absent body
		r := stream.SimpleReader{R: strings.NewReader(sample)}
		buf := make([]stream.Record, 2)
		n, err := r.ReadRecords(buf)
		if err != io.EOF {
			t.Fatalf("got read records error %v, want io.EOF", err)
		}
		if n != 0 {
			t.Errorf("got %d records, want 0", n)
		}
		for i := range buf {
			assertRecord(t, buf, i, "", "")
		}
	})

	t.Run("1stBodyTerm", func(t *testing.T) {
		const sample = "\x00\x00\x00\x0A\x00\x00\x00\x03tex" // incomplete body
		r := stream.SimpleReader{R: strings.NewReader(sample)}
		buf := make([]stream.Record, 2)
		n, err := r.ReadRecords(buf)
		if err != io.EOF {
			t.Fatalf("got read records error %v, want io.EOF", err)
		}
		if n != 0 {
			t.Errorf("got %d records, want none", n)
		}
		for i := range buf {
			assertRecord(t, buf, i, "", "")
		}
	})

	t.Run("2ndHeaderNone", func(t *testing.T) {
		const sample = "\x00\x00\x00\x0A\x00\x00\x00\x03text/plainONE"
		r := stream.SimpleReader{R: strings.NewReader(sample)}
		buf := make([]stream.Record, 2)
		n, err := r.ReadRecords(buf)
		if err != io.EOF {
			t.Fatalf("got read records error %v, want io.EOF", err)
		}
		if n != 1 {
			t.Errorf("got %d records, want 1", n)
		}
		assertRecord(t, buf, 0, "text/plain", "ONE")
		assertRecord(t, buf, 1, "", "")
	})

	t.Run("2ndHeaderTerm", func(t *testing.T) {
		const sample = "\x00\x00\x00\x0A\x00\x00\x00\x03text/plainONE" +
			"\x00\x00\x00\x0A\x00\x00" // incomplete header
		r := stream.SimpleReader{R: strings.NewReader(sample)}
		buf := make([]stream.Record, 2)
		n, err := r.ReadRecords(buf)
		if err != io.EOF {
			t.Fatalf("got read records error %v, want io.EOF", err)
		}
		if n != 1 {
			t.Errorf("got %d records, want 1", n)
		}
		assertRecord(t, buf, 0, "text/plain", "ONE")
		assertRecord(t, buf, 1, "", "")
	})

	t.Run("2ndBodyNone", func(t *testing.T) {
		const sample = "\x00\x00\x00\x0A\x00\x00\x00\x03text/plainONE" +
			"\x00\x00\x00\x0A\x00\x00\x00\x03" // absent body
		r := stream.SimpleReader{R: strings.NewReader(sample)}
		buf := make([]stream.Record, 2)
		n, err := r.ReadRecords(buf)
		if err != io.EOF {
			t.Fatalf("got read records error %v, want io.EOF", err)
		}
		if n != 1 {
			t.Errorf("got %d records, want 1", n)
		}
		assertRecord(t, buf, 0, "text/plain", "ONE")
		assertRecord(t, buf, 1, "", "")
	})

	t.Run("2ndBodyTerm", func(t *testing.T) {
		const sample = "\x00\x00\x00\x0A\x00\x00\x00\x03text/plainONE" +
			"\x00\x00\x00\x0A\x00\x00\x00\x03tex" // incomplete body
		r := stream.SimpleReader{R: strings.NewReader(sample)}
		buf := make([]stream.Record, 2)
		n, err := r.ReadRecords(buf)
		if err != io.EOF {
			t.Fatalf("got read records error %v, want io.EOF", err)
		}
		if n != 1 {
			t.Errorf("got %d records, want 1", n)
		}
		assertRecord(t, buf, 0, "text/plain", "ONE")
		assertRecord(t, buf, 1, "", "")
	})
}

func TestSimpleReaderAllocs(t *testing.T) {
	const sample = "\x00\x00\x00\x0A\x00\x00\x00\x03text/plainONE" +
		"\x00\x00\x00\x0A\x00\x00\x00\x03text/plainTWO" +
		"\x00\x00\x00\x0A\x00\x00\x00\x05text/plainTHREE"
	r := stream.SimpleReader{R: strings.NewReader(sample)}

	// read one record at a time
	var buf [1]stream.Record

	// The anonymous func gets invoked 3 times: 1 warm-up + 2 measured runs.
	allocAvg := testing.AllocsPerRun(2, func() {
		n, err := r.ReadRecords(buf[:])
		if err != nil && err != io.EOF {
			t.Fatal("read records error:", err)
		}
		if n != 1 {
			t.Fatalf("got %d records, want 1", n)
		}
		if len(buf[0].MediaType) == 0 {
			t.Errorf("media type missing")
		}
		if len(buf[0].Payload) == 0 {
			t.Errorf("payload missing")
		}
	})

	// Only the first ReadRecords, which is ignored from the average,
	// may allocate the read buffer and the media-type string.
	if allocAvg != 0 {
		t.Errorf("got %f memory alloctions on average, want none", allocAvg)
	}
}

func FuzzSimpleReader(f *testing.F) {
	f.Add([]byte("\x00\x00\x00\x0A\x00\x00\x00\x03text/plainONE"+
		"\x00\x00\x00\x0A\x00\x00\x00\x03text/plainTWO"+
		"\x00\x00\x00\x0A\x00\x00\x00\x05text/plainTHREE"),
		uint8(3),
	)
	f.Fuzz(func(t *testing.T, in []byte, n uint8) {
		r := stream.SimpleReader{R: bytes.NewReader(in)}

		var buf [3]stream.Record
		_, err := r.ReadRecords(buf[:n&3])
		switch err {
		case nil, io.EOF:
			break // OK
		default:
			if !strings.Contains(err.Error(), "malformed header") {
				t.Error("read got error:", err)
			}
		}
	})
}
