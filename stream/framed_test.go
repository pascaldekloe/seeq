package stream_test

import (
	"bytes"
	"encoding/hex"
	"io"
	"math/rand"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/pascaldekloe/seeq/stream"
)

func TestFramedReader(t *testing.T) {
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
		r := stream.NewFramedReader(strings.NewReader(sample), 0)
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
		r := stream.NewFramedReader(strings.NewReader(""), 0)
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
		r := stream.NewFramedReader(iotest.OneByteReader(strings.NewReader(sample)), 0)
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
		r := stream.NewFramedReader(iotest.OneByteReader(strings.NewReader(sample)), 0)
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
		r := stream.NewFramedReader(iotest.OneByteReader(strings.NewReader(sample)), 0)
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
		r := stream.NewFramedReader(iotest.OneByteReader(strings.NewReader(sample)), 0)
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
		r := stream.NewFramedReader(iotest.OneByteReader(strings.NewReader(sample)), 0)
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
		r := stream.NewFramedReader(iotest.OneByteReader(strings.NewReader(sample)), 0)
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
		r := stream.NewFramedReader(iotest.OneByteReader(strings.NewReader(sample)), 0)
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
		r := stream.NewFramedReader(iotest.OneByteReader(strings.NewReader(sample)), 0)
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

func TestFramedReaderAllocs(t *testing.T) {
	const sample = "\x00\x00\x03\x0Atext/plainONE" +
		"\x00\x00\x03\x0Atext/plainTWO" +
		"\x00\x00\x05\x0Atext/plainTHREE"
	r := stream.NewFramedReader(strings.NewReader(sample), 0)

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

func FuzzFramedReader(f *testing.F) {
	f.Add([]byte("\x00\x00\x05\x0Atext/plainéén"+
		"\x00\x00\x03\x0Atext/plaintwee"),
		uint8(2),
	)
	f.Fuzz(func(t *testing.T, in []byte, n uint8) {
		r := stream.NewFramedReader(bytes.NewReader(in), 0)

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

// TestPipe is a fat integration test for the framed reader & writer.
func TestPipe(t *testing.T) {
	pr, pw := io.Pipe()
	timeout := time.AfterFunc(2*time.Second, func() {
		t.Error("test expired")
		pr.Close()
	})
	defer timeout.Stop()

	const testN = 10_000 // stream entry count

	testData := make([]byte, 1024)
	rand.Read(testData)
	// vary payload per entry, including zero
	payloadSlice := func(entryN int) []byte {
		return testData[:entryN%len(testData)]
	}

	// write testN stream-entries into the pipe
	go func() {
		defer pw.Close()
		w := stream.NewFramedWriter(pw)
		batch := make([]stream.Entry, 5)

		var entryN int // production count
		for writeN := 1; entryN < testN; writeN++ {
			// vary batch size per read, including zero
			batch = batch[:(writeN*3)%(cap(batch)+1)]
			if todo := testN - entryN; len(batch) > todo {
				batch = batch[:todo]
			}

			for i := range batch {
				entryN++
				batch[i].Payload = payloadSlice(entryN)
				batch[i].MediaType = "application/octet-stream"
			}

			err := w.Write(batch)
			if err != nil {
				t.Error(err)
				return
			}
		}
	}()

	r := stream.NewFramedReader(pr, 0)
	basket := make([]stream.Entry, 5)

	entryN := 0 // evaluate count
	for readN := 1; !t.Failed(); readN++ {
		// vary basket size per read, including zero
		basket = basket[:(readN*7)%(cap(basket)+1)]
		n, err := r.Read(basket)

		// handle basket[:n] before err
		for i := 0; i < n; i++ {
			entryN++
			if string(basket[i].Payload) != string(payloadSlice(entryN)) {
				t.Errorf("payload from entry № %d is off", entryN)
				t.Log("got:\n", hex.Dump(basket[i].Payload))
				t.Log("want:\n", hex.Dump(payloadSlice(entryN)))
			}
		}

		switch err {
		case nil:
			if entryN >= testN {
				t.Fatalf("no EOF after %d (out of %d) entries read", entryN, testN)
			}
		case io.EOF:
			if entryN != testN {
				t.Fatalf("got EOF after %d (out of %d) entries read", entryN, testN)
			}
			return // OK
		default:
			t.Fatalf("got error after %d (out of %d) entries read: %s", entryN, testN, err)
		}
	}
}
