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
		{"text/msg+plain;v=1.0;rel=token", []byte("Hello,")},
		{"text/msg+plain;v=1.0;rel=token", []byte("World!")},
	})
	if err != nil {
		fmt.Println("events lost:", err)
		return
	}

	group, err := seeq.NewGroup(NewWORMAggs)
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
	fmt.Printf("• average payload size: %d bytes\n", q.Aggs.Stats.EventSizeAvg())
	fmt.Printf("• hash digest: %#x\n", q.Aggs.Crypto.Digest.Sum(nil))

	group.Interrupt()
	<-done
	// Output:
	// • stream offset: 2
	// • average payload size: 6 bytes
	// • hash digest: 0x8f4ec1811c6c4261c97a7423b3a56d69f0f160074f39745af20bb5fcf65ccf78
	// aggregate synchronisation received an interrupt
}
