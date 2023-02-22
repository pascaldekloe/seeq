package seeq_test

import (
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash"
	"io"
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

	group, err := seeq.NewLightGroup(NewWORMAggs)
	if err != nil {
		fmt.Println("illegal setup:", err)
		return
	}
	go func() {
		fmt.Println(group.SyncFrom(repo, "demo-stream"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	q, err := group.LiveSince(ctx, time.Now().Add(-100*time.Millisecond))
	if err != nil {
		fmt.Println("aggregate lookup expired:", err)
		return
	}

	fmt.Printf("Stream %q read to offset %d.\n", streamName, q.SeqNo)
	fmt.Printf("The average payload size was %d bytes.\n", q.Aggs.Stats.EventSizeAvg())
	fmt.Printf("digest: %#x\n", q.Aggs.Crypto.Digest.Sum(nil))
	// Output:
	// Stream "demo-stream" read to offset 2.
	// The average payload size was 6 bytes.
	// digest: 0x8f4ec1811c6c4261c97a7423b3a56d69f0f160074f39745af20bb5fcf65ccf78
}

// WORMAggs demonstrates a collection of two aggregates.
type WORMAggs struct {
	Stats  *WORMStats `aggregate:"demo-stats"`
	Crypto *WORMCheck `aggregate:"demo-crypto"`
}

// NewWORMAggs is a constructor.
func NewWORMAggs() (*WORMAggs, error) {
	return &WORMAggs{
		Stats:  new(WORMStats),
		Crypto: &WORMCheck{sha256.New()},
	}, nil
}

// WORMStats demonstrates metrics collection on an event stream. The exported
// fields can be used directly when aquired through a seeq.QuerySet.
type WORMStats struct {
	EventCount   int64 `json:event-count,string`    // number of entries
	EventSizeSum int64 `json:event-size-sum,string` // payload byte count
}

// EventSizeAvg demonstrates a simple query beyond the exported fields. Note how
// no errors are rather common here.
func (stats *WORMStats) EventSizeAvg() int64 {
	if stats.EventCount == 0 {
		return -1
	}
	return stats.EventSizeSum / stats.EventCount
}

// AddNext implements the seeq.Aggregate interface.
func (stats *WORMStats) AddNext(batch []stream.Entry) {
	stats.EventCount += int64(len(batch))
	for i := range batch {
		stats.EventSizeSum += int64(len(batch[i].Payload))
	}
}

// DumpTo implements the seeq.Aggregate interface.
func (stats *WORMStats) DumpTo(w io.Writer) error {
	return json.NewEncoder(w).Encode(stats)
}

// LoadFrom implements the seeq.Aggregate interface.
func (stats *WORMStats) LoadFrom(r io.Reader) error {
	*stats = WORMStats{} // reset
	return json.NewDecoder(r).Decode(stats)
}

// WORMCheck demonstrates payload consumption. The exported field can be used
// directly when aquired through a seeq.QuerySet.
type WORMCheck struct {
	Digest hash.Hash
}

// AddNext implements the seeq.Aggregate interface.
func (ck *WORMCheck) AddNext(batch []stream.Entry) {
	for i := range batch {
		ck.Digest.Write(batch[i].Payload)
	}
}

// DumpTo implements the seeq.Aggregate interface.
func (ck *WORMCheck) DumpTo(w io.Writer) error {
	return gob.NewEncoder(w).Encode(ck)
}

// LoadFrom implements the seeq.Aggregate interface.
func (ck *WORMCheck) LoadFrom(r io.Reader) error {
	*ck = WORMCheck{} // reset
	return gob.NewDecoder(r).Decode(ck)
}
