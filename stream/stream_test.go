package stream_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/pascaldekloe/seeq/stream"
	"github.com/pascaldekloe/seeq/stream/streamtest"
)

func TestAppendCopy(t *testing.T) {
	var tests = [][]stream.Entry{
		{},
		{{}},
		{{"text", nil}, {"", []byte{'A'}}},
	}

	for _, entries := range tests {
		clone := stream.AppendCopy(nil, entries...)
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

func TestFunnel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	w, ch := streamtest.NewChannelWriter()
	f := stream.NewFunnel(w)
	defer func() {
		err := f.Close()
		if err != nil {
			t.Error("funnel Close error:", err)
		}

		err = f.Write(nil)
		if err != io.ErrClosedPipe {
			t.Errorf("funnel Write after close got error %v, want io.ErrClosedPipe", err)
		}
	}()

	// start all routines at once to maximise collision chances
	hold := make(chan struct{})

	const routineN = 12
	for routineI := 0; routineI < routineN; routineI++ {
		go func() {
			// write full byte range in each payload
			bytes := make([]byte, 256)
			batch := make([]stream.Entry, 256)
			for i := range batch {
				batch[i].Payload = bytes[i : i+1]
				batch[i].Payload[0] = byte(i)
			}

			<-hold
			err := f.Write(batch)
			if err != nil {
				t.Error("write error:", err)
			}
		}()
	}

	time.Sleep(time.Millisecond)
	close(hold)

	// each routine writes full byte-range
	for i := 0; i < routineN*256; i++ {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatalf("test channel closed while reading message № %d", i+1)
			}
			if len(e.Payload) != 1 || e.Payload[0] != byte(i) {
				t.Fatalf("got payload %#x, want %#x", e.Payload, byte(i))
			}

		case <-ctx.Done():
			t.Fatal("test timeout")
		}
	}
}

func TestAppendCopy_alloc(t *testing.T) {
	e1 := stream.Entry{"text", []byte("one")}
	e2 := stream.Entry{"text", []byte("two")}
	e3 := stream.Entry{"text", []byte("three")}
	e4 := stream.Entry{"text", []byte("four")}
	buf := make([]stream.Entry, 0, 4)
	avg := testing.AllocsPerRun(1, func() {
		stream.AppendCopy(buf, e1, e2, e3, e4)
	})
	if avg != 1 {
		t.Errorf("did %f allocations, want 1", avg)
	}
}

func ExampleMediaType() {
	// parse rich MIME
	const sample = `application/hal+json;profile="https://example.com/v1"`
	t := stream.CachedMediaType(sample)

	fmt.Println("• type:", t.Type)
	fmt.Println("• subtype:", t.Subtype)
	fmt.Println("• suffix:", t.Suffix)
	if v, found := t.Param("profile"); found {
		fmt.Println("• profile:", v)
	}
	// Output:
	// • type: application
	// • subtype: hal
	// • suffix: json
	// • profile: https://example.com/v1
}
