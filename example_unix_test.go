//go:build unix && !omitunix

package seeq_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pascaldekloe/seeq"
	"github.com/pascaldekloe/seeq/stream"
)

func Example() {
	repo := &stream.RollingFiles{
		Dir:    "testdata/example-repo",
		ChunkN: 99,
	}
	if err := os.RemoveAll(repo.Dir); err != nil {
		fmt.Println(err)
		return
	}

	const streamName = "demo-stream"
	w := repo.AppendTo(streamName)
	defer w.Close()
	err := w.Write([]stream.Entry{
		{"text/plain;v=1.0;rel=token", []byte("Hello,")},
		{"text/plain;v=1.0;rel=token", []byte("World!")},
		{"application/my-event+json", []byte(`{
  "dc:dateSubmitted": "2023-03-06T17:33:08Z",
  "dc:dateAccepted":  "2023-03-06T17:33:09Z",
  "dc:title":         "Demo Event"
}`)},
	})
	if err != nil {
		fmt.Println("events lost:", err)
		return
	}

	group, err := seeq.NewGroup(NewDemoAggs)
	if err != nil {
		fmt.Println("illegal setup:", err)
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fmt.Println(group.SyncFromRepo(repo, "demo-stream"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	q, err := group.LiveSince(ctx, time.Now().Add(-100*time.Millisecond))
	if err != nil {
		fmt.Println("aggregate lookup expired:", err)
		return
	}

	fmt.Printf("• stream offset: %d\n", q.Offset)
	fmt.Printf("• average text size: %d bytes\n", q.Aggs.TextStats.SizeAvg())
	submitted, accepted, err := q.Aggs.EventTimes.ByIndex(0)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("• event № 1 submitted: %s\n", submitted)
		fmt.Printf("• event № 1 accepted: %s\n", accepted)
	}

	group.Interrupt()
	<-done
	// Output:
	// • stream offset: 3
	// • average text size: 6 bytes
	// • event № 1 submitted: 2023-03-06 17:33:08 +0000 UTC
	// • event № 1 accepted: 2023-03-06 17:33:09 +0000 UTC
	// aggregate synchronisation received an interrupt
}
