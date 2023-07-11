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

	w := repo.AppendTo("demo-stream")
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

	sync, err := seeq.NewReleaseSync(NewDemoAggs)
	if err != nil {
		fmt.Println("broken setup:", err)
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fmt.Println(sync.SyncFromRepo(repo, "demo-stream"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fix, err := sync.LiveSince(time.Now().Add(-100*time.Millisecond), ctx.Done())
	if err != nil {
		fmt.Println("aggregate group unavailable:", err)
		return
	}

	fmt.Printf("• stream offset: %d\n", fix.Offset)
	fmt.Printf("• average text size: %d bytes\n", fix.Q.TextStats.SizeAvg())
	submitted, accepted, err := fix.Q.EventTimes.ByIndex(0)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("• event № 1 submitted: %s\n", submitted)
		fmt.Printf("• event № 1 accepted: %s\n", accepted)
	}

	sync.Interrupt()
	<-done
	// Output:
	// • stream offset: 3
	// • average text size: 6 bytes
	// • event № 1 submitted: 2023-03-06 17:33:08 +0000 UTC
	// • event № 1 accepted: 2023-03-06 17:33:09 +0000 UTC
	// aggregate synchronisation received an interrupt
}
