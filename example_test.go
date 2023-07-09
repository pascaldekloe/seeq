package seeq_test

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/pascaldekloe/seeq/stream"
)

// DemoAggs demonstrates a collection of two aggregates.
type DemoAggs struct {
	*TextStats  `aggregate:"demo-inmemory"`
	*EventTimes `aggregate:"demo-database"`
}

// NewDemoAggs returns the aggregate collection for stream-offset zero.
func NewDemoAggs() (*DemoAggs, error) {
	times, err := NewEventTimes()
	if err != nil {
		return nil, err
	}
	return &DemoAggs{new(TextStats), times}, nil
}

// TextStats demonstrates an in-memory aggregate.
type TextStats struct {
	MsgCount int64 `json:"msg-count,string"`
	SizeSum  int64 `json:"size-sum,string"`
}

// SizeAvg demonstrates a simple query beyond exported fields.
// Note the absense of errors.
func (stats *TextStats) SizeAvg() int64 {
	// No concurrency issues to worry about as query methods
	// get invoked on read-only instances exclusively.
	if stats.MsgCount == 0 {
		return -1
	}
	return stats.SizeSum / stats.MsgCount
}

// AddNext implements the seeq.Aggregate interface.
func (stats *TextStats) AddNext(batch []stream.Entry, offset uint64) error {
	// no concurrency issues to worry about
	for i := range batch {
		parts := stream.CachedMediaType(batch[i].MediaType)
		switch {
		case parts.Type == "text" && parts.Subtype == "plain":
			stats.MsgCount++
			stats.SizeSum += int64(len(batch[i].Payload))
		}
	}
	return nil
}

// DumpTo implements the seeq.Aggregate interface.
func (stats *TextStats) DumpTo(w io.Writer) error {
	return json.NewEncoder(w).Encode(stats)
}

// LoadFrom implements the seeq.Aggregate interface.
func (stats *TextStats) LoadFrom(r io.Reader) error {
	*stats = TextStats{} // reset
	return json.NewDecoder(r).Decode(stats)
}

// EventTimes demonstrates a database aggregate.
type EventTimes struct {
	File *os.File
}

// NewEventTimes returns a new aggregate for stream-offset zero.
func NewEventTimes() (*EventTimes, error) {
	f, err := os.CreateTemp("", "timestamps.")
	if err != nil {
		return nil, err
	}
	runtime.SetFinalizer(f, func(f *os.File) { f.Close() })

	// file is fully gone once closed
	err = os.Remove(f.Name())
	if err != nil {
		return nil, err
	}

	return &EventTimes{f}, nil
}

// ByIndex demonstates a query with block devices.
func (times *EventTimes) ByIndex(i int64) (submitted, accepted time.Time, err error) {
	var record [16]byte
	_, err = times.File.ReadAt(record[:], i*int64(len(record)))
	submitted = time.UnixMilli(int64(binary.BigEndian.Uint64(record[:8]))).UTC()
	accepted = time.UnixMilli(int64(binary.BigEndian.Uint64(record[8:]))).UTC()
	return
}

// AddNext implements the seeq.Aggregate interface.
func (times *EventTimes) AddNext(batch []stream.Entry, offset uint64) error {
	for i := range batch {
		// filter "my-event" entries
		parts := stream.CachedMediaType(batch[i].MediaType)
		if parts.Subtype != "my-event" || parts.Suffix != "json" {
			continue
		}

		// parse timestamps
		var meta struct {
			Submitted time.Time `json:"dc:dateSubmitted"`
			Accepted  time.Time `json:"dc:dateAccepted"`
		}
		err := json.Unmarshal(batch[i].Payload, &meta)
		if err != nil {
			log.Printf("stream entry № %d corrupt: %s", offset+uint64(i)+1, err)
			continue
		}

		// write timestamps
		var record [16]byte
		binary.BigEndian.PutUint64(record[:8], uint64(meta.Submitted.UnixMilli()))
		binary.BigEndian.PutUint64(record[8:], uint64(meta.Accepted.UnixMilli()))
		_, err = times.File.Write(record[:])
		if err != nil {
			return err // filesystem malfunction is fatal
		}
	}

	return times.File.Sync()
}

// DumpTo implements the seeq.Aggregate interface.
func (times *EventTimes) DumpTo(w io.Writer) error {
	_, err := times.File.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, times.File)
	return err
}

// LoadFrom implements the seeq.Aggregate interface.
func (times *EventTimes) LoadFrom(r io.Reader) error {
	_, err := times.File.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	n, err := io.Copy(times.File, r)
	if err != nil {
		return err
	}
	return times.File.Truncate(n)
}
