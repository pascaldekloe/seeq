package seeq

import (
	"context"
	"errors"
	"fmt"
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
	Aggs *AggregateSet // read-only
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
func (g *LightGroup[AggregateSet]) SyncFrom(c *stream.Cursor) error {
	// instantiate working copy
	set, aggs, err := g.fork(0, nil)
	if err != nil {
		return err
	}

	// once live the QuerySet is offered to release
	var offerTimer *time.Timer // short-poll delay

	for {
		lastReadTime, err := FeedEach(c, aggs...)
		if err != nil {
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
			break // no demand
		case g.release <- QuerySet[AggregateSet]{Aggs: set, SeqNo: c.SeqNo, LiveAt: lastReadTime}:
			if !offerTimer.Stop() {
				<-offerTimer.C
			}

			// swap working copy
			set, aggs, err = g.fork(c.SeqNo, aggs)
			if err != nil {
				return err
			}
		}
	}
}

func (g *LightGroup[AggregateSet]) fork(seqNo uint64, old []Aggregate[stream.Entry]) (*AggregateSet, []Aggregate[stream.Entry], error) {
	set, err := g.newSet()
	if err != nil {
		return nil, nil, fmt.Errorf("aggregate synchronization halt on instantiation: %w", err)
	}

	aggs := make([]Aggregate[stream.Entry], len(g.aggFields))
	v := reflect.ValueOf(set).Elem()
	for i := range aggs {
		aggs[i] = v.FieldByIndex(g.aggFields[i].Index).Interface().(Aggregate[stream.Entry])
	}

	if len(old) != len(aggs) {
		return set, aggs, nil
	}

	// copy snapshots of each aggregate
	// buffer to preserve error order, if any
	done := make(chan error, len(aggs))
	for i := range aggs {
		go func(i int) {
			// TODO(pascaldekloe): apply snapshot directory & expire previous
			// The second resolution serves a flood protection.
			snapshotName := fmt.Sprintf("%s-%d.snapshot", g.aggregateLabel(g.aggFields[i]), seqNo)
			snapshotFile, err := os.OpenFile(snapshotName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o660)
			if err != nil {
				// snapshots are an optional optimization
				log.Print("continue without snapshot persistence: ", err)
				done <- Clone(aggs[i], old[i])
				return
			}
			defer snapshotFile.Close()

			err = CloneWithSnapshot(aggs[i], old[i], snapshotFile)
			if err != nil {
				done <- err
				return
			}
			done <- snapshotFile.Sync()
		}(i)
	}

	var errs []error
	for range aggs {
		err := <-done
		if err != nil {
			errs = append(errs, err)
		}
	}

	switch len(errs) {
	case 0:
		return set, aggs, nil
	case 1:
		return nil, nil, errs[0]
	}
	// try and dedupe
	msgSet := make(map[string]struct{}, len(errs))
	for _, err := range errs {
		msgSet[err.Error()] = struct{}{}
	}

	firstMsg := errs[0].Error()
	delete(msgSet, firstMsg)
	if len(msgSet) == 0 {
		return nil, nil, errs[0]
	}

	others := make([]string, 0, len(msgSet))
	for s := range msgSet {
		others = append(others, s)
	}
	return nil, nil, fmt.Errorf("%w; followed by %q", errs[0], others)
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
