// Package seeq implements state collection from streams.
package seeq

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/pascaldekloe/seeq/snapshot"
	"github.com/pascaldekloe/seeq/stream"
)

// An Aggregate consumes a stream of T—typically stream.Entry—for one or more
// specific queries. Such queries may be served with exported fields and/or
// methods. The specifics are beyond the scope of this interface.
//
// Both LoadFrom and AddNext shall execute in isolation. DumpTo is considdered
// to be a read-only operation. Therefore, DumpTo can be invoked simultaneously
// (from multiple goroutines) with any query methods.
type Aggregate[T any] interface {
	// AddNext consumes a stream in chronological order. Malformed content
	// should be reported only. The stream must continue at all times. Any
	// error return is fatal to the Aggregate.
	//
	// Offset counts the number of entries passed before batch, since the
	// very beginning of the stream.
	AddNext(batch []T, offset uint64) error

	// DumpTo produces a snapshot/serial/backup of the Aggregate's state.
	// When the implementation makes use of third-party storage such as a
	// database, then the snapshot should include the stored content too.
	DumpTo(io.Writer) error

	// LoadFrom resets the Aggregate state to a snapshot from DumpTo.
	// Errors may leave the Aggregate in an undefined state.
	LoadFrom(io.Reader) error
}

// Copy the state from src into dst. Snapshot Production may be nil. Copy does
// not Commit nor Abort the Production.
func Copy[T any](dst, src Aggregate[T], p snapshot.Production) error {
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
	if p == nil {
		r = pr
	} else {
		r = io.TeeReader(pr, p)
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

// SyncEach applies all entries from r to each Aggregate in argument order. Buf
// defines the batch size for Read and AddNext. Error is nil on success-not EOF.
func SyncEach(r stream.Reader, buf []stream.Entry, aggs ...Aggregate[stream.Entry]) (lastRead time.Time, err error) {
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

// groupConfig has the configuration of each embedded Aggregate.
type groupConfig[Group any] struct {
	constructor     func() (*Group, error)
	aggFieldIndices []int    // struct position
	aggNames        []string // user label (from tag)
}

func newGroupConfig[Group any](constructor func() (*Group, error)) (*groupConfig[Group], error) {
	c := groupConfig[Group]{constructor: constructor}

	groupType := reflect.TypeOf((*Group)(nil)).Elem()
	aggType := reflect.TypeOf((*Aggregate[stream.Entry])(nil)).Elem()
	if k := groupType.Kind(); k != reflect.Struct {
		return nil, fmt.Errorf("aggregate group %s is of kind %s—not %s", groupType, k, reflect.Struct)
	}

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
			name = groupType.String() + "." + field.Name
		}
		c.aggNames = append(c.aggNames, name)

		if !field.IsExported() {
			return nil, fmt.Errorf("aggregate group %s, field %s is not exported; first letter must upper-case", groupType, field.Name)
		}
		if !field.Type.Implements(aggType) {
			return nil, fmt.Errorf("aggregate group %s, field %s, type %s does not implement %s", groupType, field.Name, field.Type, aggType)
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

func (c *groupConfig[Group]) newGroup() (*Group, []Aggregate[stream.Entry], error) {
	group, err := c.constructor()
	if err != nil {
		return nil, nil, err
	}

	aggs := make([]Aggregate[stream.Entry], len(c.aggFieldIndices))
	v := reflect.ValueOf(group).Elem()
	for i := range aggs {
		aggs[i] = v.Field(c.aggFieldIndices[i]).Interface().(Aggregate[stream.Entry])
	}

	return group, aggs, nil
}
