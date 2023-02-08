package seeq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/pascaldekloe/seeq/stream"
)

// QuerySet contains aggregates with any and all entries from an input stream
// applied at some point in time. The AggregateSet is read-only—ready to serve
// queries. No more updates shall be applied.
type QuerySet[AggregateSet any] struct {
	Set *AggregateSet // read-only
	// The sequence number equals the amount of stream entries applied.
	SeqNo uint64
	// Live is defined as the latest EOF read from the input stream.
	LiveAt time.Time
}

var aggregateType = reflect.TypeOf(struct{ Aggregate[stream.Entry] }{}).Field(0).Type

// aggregateSetFields returns the fields tagged as "aggregate".
func aggregateSetFields(setType reflect.Type) ([]reflect.StructField, error) {
	if k := setType.Kind(); k != reflect.Struct {
		return nil, fmt.Errorf("aggregate set %s is a %s—not a struct", setType, k)
	}

	fieldN := setType.NumField()
	fields := make([]reflect.StructField, 0, fieldN)
	for i := 0; i < fieldN; i++ {
		field := setType.Field(i)

		tag, ok := field.Tag.Lookup("aggregate")
		if !ok {
			continue // not tagged as aggregate
		}
		if tag != "" {
			return nil, fmt.Errorf("aggregate field %s from %s has unsupported tag %q", field.Name, setType, tag)
		}

		if !field.IsExported() {
			return nil, fmt.Errorf("aggregate field %s from %s is not exported", field.Name, setType)
		}

		if !field.Type.Implements(aggregateType) {
			return nil, fmt.Errorf("aggregate field %s from %s does not implement %s", field.Name, setType, aggregateType)
		}

		fields = append(fields, field)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf(`%s has no fields tagged as "aggregate"`, setType)
	}
	return fields, nil
}

// LightGroup feeds a single AggregateSet sequentially. Whenever a Live method
// expires its current QuerySet, then the update sigleton gets used next. The
// Sync method creates a new clone from a new snapshot to continue with. This
// setup works well for states with fast snapshot handling [Dumper & Loader].
//
// The AggregateSet must be a struct with one or more of its fields tagged as
// "aggregate". Each field tagged as aggregate must implement the Aggregate
// interface.
type LightGroup[AggregateSet any] struct {
	newSet func() (*AggregateSet, error) // constructor

	// AggFields has one or more Aggregate fields from AggregateSet
	aggFields []reflect.StructField

	// latest singleton, or nil initially
	live chan *QuerySet[AggregateSet]
	// working copy handover
	release chan QuerySet[AggregateSet]
}

// NewLightGroup returns a new installation that must be fed with the SyncFrom
// method in order to use any of the Live methods. NewSet is expected to return
// empty sets ready for use.
func NewLightGroup[AggregateSet any](newSet func() (*AggregateSet, error)) (*LightGroup[AggregateSet], error) {
	// read & validate AggregateSet structure
	fields, err := aggregateSetFields(reflect.TypeOf((*AggregateSet)(nil)).Elem())
	if err != nil {
		return nil, err
	}

	g := LightGroup[AggregateSet]{
		newSet:    newSet,
		aggFields: fields,
		live:      make(chan *QuerySet[AggregateSet], 1),
		release:   make(chan QuerySet[AggregateSet]),
	}
	g.live <- nil // initial placeholder

	return &g, nil
}

// Aggregates returns the aggregate values of a set struct.
func (g *LightGroup[AggregateSet]) aggregates(set *AggregateSet) []Aggregate[stream.Entry] {
	v := reflect.ValueOf(set).Elem()
	aggs := make([]Aggregate[stream.Entry], len(g.aggFields))
	for i := range aggs {
		aggs[i] = v.FieldByIndex(g.aggFields[i].Index).Interface().(Aggregate[stream.Entry])
	}
	return aggs
}

// AggregateLabel returns a descriptive name.
func (g *LightGroup[AggregateSet]) aggregateLabel(field reflect.StructField) string {
	setType := reflect.TypeOf((*AggregateSet)(nil)).Elem()

	t := field.Type
	for {
		switch t.Kind() {
		case reflect.Interface, reflect.Pointer:
			t = t.Elem() // resolve
		default:
			return fmt.Sprintf("%s@%s.%s", t, setType, field.Name)
		}
	}
}

// SyncFrom applies events to the Aggregates until end-of-stream.
func (g *LightGroup[AggregateSet]) SyncFrom(in stream.Reader) error {
	// create a working copy which we feed from in
	workingCopy := QuerySet[AggregateSet]{SeqNo: 0}
	var err error
	workingCopy.Set, err = g.newSet()
	if err != nil {
		return fmt.Errorf("aggregate synchronization halt on initial instantiation: %w", err)
	}
	aggs := g.aggregates(workingCopy.Set)

	// once live the QuerySet is offered to release
	var offerTimer *time.Timer // short-poll delay

	var buf [99]stream.Entry
	for {
		n, err := in.Read(buf[:])
		// ⚠️ delayed error check

		if err == io.EOF {
			workingCopy.LiveAt = time.Now() // live moment
		}

		for _, agg := range aggs {
			agg.AddNext(buf[:n])
		}
		workingCopy.SeqNo += uint64(n)

		switch err {
		case nil:
			continue
		case io.EOF:
			break
		default:
			return fmt.Errorf("aggregate synchronization halt on input: %w", err)
		}

		// offer working copy during liveDelay
		const liveDelay = 10 * time.Millisecond
		if offerTimer == nil {
			offerTimer = time.NewTimer(liveDelay)
		} else {
			offerTimer.Reset(liveDelay)
		}
		select {
		case <-offerTimer.C:
			continue // check stream again
		case g.release <- workingCopy:
			if !offerTimer.Stop() {
				<-offerTimer.C
			}
		}
		// workingCopy.Set is read-only now

		aggs, err = g.fork(&workingCopy, aggs)
		if err != nil {
			return err
		}
	}
}

func (g *LightGroup[AggregateSet]) fork(workingCopy *QuerySet[AggregateSet], workingCopyAggs []Aggregate[stream.Entry]) ([]Aggregate[stream.Entry], error) {
	// new copy
	cloneSet, err := g.newSet()
	if err != nil {
		return nil, fmt.Errorf("aggregate set synchronization halt on instation: %w", err)
	}
	cloneAggs := g.aggregates(cloneSet)

	// copy snapshots of each aggregate
	// buffer to preserve error order, if any
	done := make(chan error, len(workingCopyAggs))
	for i := range workingCopyAggs {
		go func(i int) {
			in, out := workingCopyAggs[i], cloneAggs[i]
			aggLabel := g.aggregateLabel(g.aggFields[i])

			// TODO(pascaldekloe): apply snapshot directory & expire previous
			// The second resolution serves a flood protection.
			snapshotName := aggLabel + "." + workingCopy.LiveAt.UTC().Format("2006-01-02T15:04:05Z")
			snapshotFile, err := os.OpenFile(snapshotName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o660)
			switch {
			case err == nil:
				defer snapshotFile.Close()
			case os.IsExist(err):
				// already have a snapshot for this very second
			default:
				// snapshots are an optional optimization
				log.Print("continue without snapshot persistence: ", err)
			}

			err = CloneWithSnapshot(out, in, snapshotFile)
			if err != nil {
				done <- err
				return
			}

			done <- snapshotFile.Sync()
		}(i)
	}

	var errs []error
	for range cloneAggs {
		err := <-done
		if err != nil {
			errs = append(errs, err)
		}
	}

	switch len(errs) {
	case 0:
		workingCopy.Set = cloneSet
		return cloneAggs, nil
	case 1:
		return nil, errs[0]
	default:
		// try and dedupe
		msgSet := make(map[string]struct{}, len(errs))
		for _, err := range errs {
			msgSet[err.Error()] = struct{}{}
		}

		firstMsg := errs[0].Error()
		delete(msgSet, firstMsg)
		if len(msgSet) == 0 {
			return nil, errs[0]
		}

		others := make([]string, 0, len(msgSet))
		for s := range msgSet {
			others = append(others, s)
		}
		return nil, fmt.Errorf("%w; followed by %q", errs[0], others)
	}
}

// ErrLiveFuture denies freshness.
var ErrLiveFuture = errors.New("agg: live view from future not available")

// LiveSince returns aggregates no older than notBefore.
func (g *LightGroup[AggregateSet]) LiveSince(ctx context.Context, notBefore time.Time) (QuerySet[AggregateSet], error) {
	tolerance := time.Since(notBefore)
	if tolerance < 0 {
		return QuerySet[AggregateSet]{}, ErrLiveFuture
	}

	ctxDone := ctx.Done()
	select {
	case aggs := <-g.live:
		switch {
		case aggs == nil:
			break // discard placeholder
		case aggs.LiveAt.Before(notBefore):
			break // discard expired
		default:
			g.live <- aggs // unlock singleton
			return *aggs, nil
		}
	case <-ctxDone:
		return QuerySet[AggregateSet]{}, ctx.Err()
	}

	for {
		select {
		case aggs := <-g.release:
			if aggs.LiveAt.Before(notBefore) {
				continue // discard again
				// Such unfortunate waste could be
				// avoided with a feedback channel.
			}
			g.live <- &aggs // unlock
			return aggs, nil
		case <-ctxDone:
			g.live <- nil // unlock [waste for nothing]
			return QuerySet[AggregateSet]{}, ctx.Err()
		}
	}
}
