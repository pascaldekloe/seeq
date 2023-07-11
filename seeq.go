// Package seeq implements state collection from streams.
package seeq

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/pascaldekloe/seeq/stream"
)

// An Aggregate consumes a stream of T—typically stream.Entry—in order to serve
// specific queries. Interpretations may use any combination of fields and/or
// methods to do so. AddNext must execute sequentially, in isolation, and before
// any of such queries.
type Aggregate[T any] interface {
	// AddNext consumes a stream in chronological order. Offset counts the
	// number of entries before the first batch element. Malformed content
	// should be reported only. The stream must continue at all times. An
	// error return is fatal to the Aggregate.
	AddNext(batch []T, offset uint64) error
}

// A Snapshotable can persist and recover its state in full. LoadFrom must
// execute in isolation. DumpTo is considdered to be a read-only operation.
// Therefore, DumpTo may be invoked simultaneously with query methods.
type Snapshotable interface {
	// DumpTo produces a snapshot/serial/backup of the Aggregate's state.
	// When the implementation makes use of third-party storage such as a
	// database, then the snapshot should include the stored content.
	DumpTo(io.Writer) error

	// LoadFrom resets the Aggregate state to a snapshot from DumpTo.
	// Errors may leave the Aggregate in an undefined state.
	LoadFrom(io.Reader) error
}

// Copy the state from src into dst. Snapshot may be omitted with nil.
func Copy(dst, src Snapshotable, snapshot io.Writer) error {
	pr, pw := io.Pipe()
	defer pr.Close()

	// write snapshot into pipe
	go func() {
		err := src.DumpTo(pw)
		if err == io.EOF {
			err = fmt.Errorf("aggregate snapshot dump did %w", err)
		}
		pw.CloseWithError(err)
	}()

	var r io.Reader
	if snapshot == nil {
		r = pr
	} else {
		r = io.TeeReader(pr, snapshot)
	}

	// load receives errors from src and w through the pipe
	err := dst.LoadFrom(r)
	if err != nil {
		return err
	}

	// partial snapshot reads not permitted to prevent mistakes
	switch n, err := io.Copy(io.Discard, r); {
	case err != nil:
		return fmt.Errorf("aggregate %T snapshot dump after load: %w", src, err)
	case n != 0:
		return fmt.Errorf("aggregate %T left %d bytes after snapshot load", dst, n)
	}

	return nil
}

// Sync applies all entries from r to each Aggregate in argument order. The
// length of buf defines the batch size for Read and AddNext. Error is nil on
// success-not EOF.
func Sync[T Aggregate[stream.Entry]](r stream.Reader, buf []stream.Entry, aggs ...T) (lastRead time.Time, err error) {
	if len(buf) == 0 {
		return time.Time{}, errors.New("aggregate feed can't work on empty buffer")
	}

	for {
		offset := r.Offset()
		n, err := r.Read(buf)
		if err == io.EOF {
			lastRead = time.Now()
		}

		if n > 0 {
			for i := range aggs {
				err = aggs[i].AddNext(buf[:n], offset)
				if err != nil {
					return time.Time{}, fmt.Errorf("aggregate synchronisation halt at stream entry № %d: %w", offset+uint64(i)+1, err)
				}
			}
		}

		switch err {
		case nil:
			continue
		case io.EOF:
			return lastRead, nil
		default:
			return lastRead, err
		}
	}
}

// Fix has a live T frozen to serve queries.
type Fix[T any] struct {
	Q *T // read-only

	// Offset is input stream position. The value matches the number of
	// stream entries applied to Q
	Offset uint64

	// Live is when the input stream had no more than Offset available.
	LiveAt time.Time
}

// groupConfig defines a struct setup with embedded aggregates.
type groupConfig[Group any] struct {
	aggFieldIndices []int    // struct position
	aggNames        []string // user label (from tag)

	snapshotables    []int // indexes which implement Snapshotable
	notSnapshotables []int // indexes which do not implement Snapshotable
}

func newGroupConfig[Group any]() (*groupConfig[Group], error) {
	groupType := reflect.TypeOf((*Group)(nil)).Elem()
	if k := groupType.Kind(); k != reflect.Struct {
		return nil, fmt.Errorf("aggregate group %s is of kind %s—not %s", groupType, k, reflect.Struct)
	}

	var c groupConfig[Group]

	// stream.Entry only for now
	aggType := reflect.TypeOf((*Aggregate[stream.Entry])(nil)).Elem()
	snapshotableType := reflect.TypeOf((*Snapshotable)(nil)).Elem()

	fieldN := groupType.NumField()
	for i := 0; i < fieldN; i++ {
		field := groupType.Field(i)

		tag, ok := field.Tag.Lookup("aggregate")
		if !ok {
			continue // not tagged as aggregate
		}
		c.aggFieldIndices = append(c.aggFieldIndices, i)

		name, _, _ := strings.Cut(tag, ",")
		if name == "" {
			// default to path in group struct(ure)
			name = groupType.String() + "." + field.Name
		}
		c.aggNames = append(c.aggNames, name)

		if !field.IsExported() {
			return nil, fmt.Errorf("aggregate group %s, field %s is not exported; first letter must upper-case", groupType, field.Name)
		}
		if !field.Type.Implements(aggType) {
			return nil, fmt.Errorf("aggregate group %s, field %s, type %s does not implement %s", groupType, field.Name, field.Type, aggType)
		}
		if !field.Type.Implements(snapshotableType) {
			return nil, fmt.Errorf("aggregate group %s, field %s, type %s does not implement %s", groupType, field.Name, field.Type, snapshotableType)
		}
	}

	if len(c.aggNames) == 0 {
		return nil, fmt.Errorf("aggregate group %s has no aggregate tags", groupType)
	}

	// duplicate name check
	indexPerName := make(map[string]int)
	for i, name := range c.aggNames {
		previous, ok := indexPerName[name]
		if ok {
			return nil, fmt.Errorf("aggregate group %s has both field %s and field %s tagged as %q",
				groupType,
				groupType.Field(c.aggFieldIndices[previous]).Name,
				groupType.Field(c.aggFieldIndices[i]).Name,
				name)
		}
		indexPerName[name] = i
	}

	return &c, nil
}

func (c *groupConfig[Group]) extract(g *Group) (*Proxy[stream.Entry], []Snapshotable) {
	aggs := make([]Aggregate[stream.Entry], 0, len(c.aggFieldIndices))
	snaps := make([]Snapshotable, 0, len(c.aggFieldIndices))

	v := reflect.ValueOf(g).Elem()
	for i := range c.aggFieldIndices {
		agg := v.Field(c.aggFieldIndices[i]).Interface()
		aggs = append(aggs, agg.(Aggregate[stream.Entry]))
		snaps = append(snaps, agg.(Snapshotable))
	}
	return NewProxy(aggs...), snaps
}
