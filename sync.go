package seeq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/pascaldekloe/seeq/snapshot"
	"github.com/pascaldekloe/seeq/stream"
)

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

// Fix contains aggregates which are ready to serve queries. No more stream
// content shall be applied.
type Fix[Aggs any] struct {
	Aggs *Aggs // read-only

	// Offset is stream position. The value matches the number of stream
	// entries applied to each aggregate in Aggs.
	Offset uint64

	// Live is when the input stream was consumed in full.
	LiveAt time.Time
}

// AggField is an Aggs struct field tagged as aggregate.
type aggField struct {
	index   int    // struct position
	aggName string // user label
}

// AggsFields validates the configuration of Aggs and it returns each field
// tagged as aggregate.
func aggsFields[Aggs any]() ([]aggField, error) {
	aggsType := reflect.TypeOf((*Aggs)(nil)).Elem()
	aggType := reflect.TypeOf((*Aggregate[stream.Entry])(nil)).Elem()
	if k := aggsType.Kind(); k != reflect.Struct {
		return nil, fmt.Errorf("%s is of kind %s, need %s", aggsType, k, reflect.Struct)
	}

	var found []aggField

	fieldN := aggsType.NumField()
	for i := 0; i < fieldN; i++ {
		field := aggsType.Field(i)

		tag, ok := field.Tag.Lookup("aggregate")
		if !ok {
			continue // not tagged as aggregate
		}

		name, _, _ := strings.Cut(tag, ",")
		if name == "" {
			name = aggsType.String() + "." + field.Name
		}

		if !field.IsExported() {
			return nil, fmt.Errorf("%s field %s is not exported [title-case]", aggsType, field.Name)
		}
		if !field.Type.Implements(aggType) {
			return nil, fmt.Errorf("%s field %s type %s does not implement %s", aggsType, field.Name, field.Type, aggType)
		}

		found = append(found, aggField{i, name})
	}

	if len(found) == 0 {
		return nil, fmt.Errorf("%s has no aggregate tags", aggsType)
	}

	// duplicate name check
	indexPerName := make(map[string]int)
	for _, f := range found {
		previous, ok := indexPerName[f.aggName]
		if ok {
			return nil, fmt.Errorf("%s has both field %s and field %s named as aggregate %q",
				aggsType, aggsType.Field(previous).Name, aggsType.Field(f.index).Name, f.aggName)
		}
		indexPerName[f.aggName] = f.index
	}

	return found, nil
}

// Group manages a set of aggregates together as a group. Aggs must be a struct
// with one or more of its fields tagged as `aggregate:"some_name"`. Any of such
// fields must implement the Aggregate[stream.Entry] interface.
type Group[Aggs any] struct {
	constructor func() (*Aggs, error)
	aggsFields  []aggField

	// latest singleton, or nil initially
	live chan *Fix[Aggs]
	// working copy handover
	release chan Fix[Aggs]
	// halts synchronisation
	interrupt chan struct{}

	Snapshots snapshot.Archive // optional
}

// NewGroup validates the Aggs configuration before returning a new setup.
// Constructor must return each aggregate in its initial state, i.e., the Aggs
// must match stream offset zero.
func NewGroup[Aggs any](constructor func() (*Aggs, error)) (*Group[Aggs], error) {
	// read & validate configuration
	fields, err := aggsFields[Aggs]()
	if err != nil {
		return nil, err
	}

	g := Group[Aggs]{
		constructor: constructor,
		aggsFields:  fields,
		live:        make(chan *Fix[Aggs], 1),
		release:     make(chan Fix[Aggs]),
		interrupt:   make(chan struct{}, 1),
	}
	g.live <- nil // initial placeholder

	return &g, nil
}

// ErrInterrupt is the result of an Interrupt request.
var ErrInterrupt = errors.New("aggregate synchronisation received an interrupt")

// Interrupt halts any ongoing synchronisation with ErrInterrupt.
func (g *Group[Aggs]) Interrupt() {
	select {
	case g.interrupt <- struct{}{}:
		break // signal installed
	default:
		break // signal already pending
	}
}

// SyncFrom a stream until failure or Interrupt.
func (g *Group[Aggs]) SyncFrom(r stream.Reader) error {
	// clear any pending interrupt request
	select {
	case <-g.interrupt:
	default:
	}

	// instantiate working copy
	instance, aggs, err := g.fork(0, nil)
	if err != nil {
		return err
	}

	return g.syncFrom(r, instance, aggs)
}

// SyncFromRepo until failure or Interrupt.
func (g *Group[Aggs]) SyncFromRepo(streams stream.Repo, streamName string) error {
	// clear any pending interrupt request
	select {
	case <-g.interrupt:
	default:
	}

	if g.Snapshots == nil {
		return g.SyncFrom(streams.ReadAt(streamName, 0))
	}

	// instantiate working copy
	instance, aggs, err := g.fork(0, nil)
	if err != nil {
		return err
	}

	aggNames := make([]string, 0, len(g.aggsFields))
	for _, f := range g.aggsFields {
		aggNames = append(aggNames, f.aggName)
	}
	offset, err := snapshot.LastCommon(g.Snapshots, aggNames...)
	if err != nil {
		return err
	}
	if offset == 0 {
		return g.SyncFrom(streams.ReadAt(streamName, 0))
	}

	errs := make(chan error, len(aggs))
	for i := range aggs {
		go func(i int) {
			r, err := g.Snapshots.Open(aggNames[i], offset)
			if err != nil {
				errs <- err
				return
			}
			err = aggs[i].LoadFrom(r)
			r.Close()
			errs <- err
		}(i)
	}

	var errN int
	for range aggs {
		err := <-errs
		if err != nil {
			errN++
			log.Print(err)
		}
	}
	if errN != 0 {
		return fmt.Errorf("aggregate group synchronisation halt on %d snapshot recovery error(s)", errN)
	}

	return g.syncFrom(streams.ReadAt(streamName, offset), instance, aggs)
}

func (g *Group[Aggs]) syncFrom(r stream.Reader, workingCopy *Aggs, aggs []Aggregate[stream.Entry]) error {
	// once live the Fix is offered to release
	var offerTimer *time.Timer // short-poll delay
	buf := make([]stream.Entry, 99)
	for {
		lastReadTime, err := SyncEach(r, buf, aggs...)
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
		case g.release <- Fix[Aggs]{Aggs: workingCopy, Offset: r.Offset(), LiveAt: lastReadTime}:
			if !offerTimer.Stop() {
				<-offerTimer.C
			}

			// swap working copy
			workingCopy, aggs, err = g.fork(r.Offset(), aggs)
			if err != nil {
				return err
			}
		case <-g.interrupt:
			return ErrInterrupt
		}
	}
}

func (g *Group[Aggs]) fork(offset uint64, old []Aggregate[stream.Entry]) (*Aggs, []Aggregate[stream.Entry], error) {
	instance, err := g.constructor()
	if err != nil {
		return nil, nil, fmt.Errorf("aggregate synchronization halt on instantiation: %w", err)
	}

	aggs := make([]Aggregate[stream.Entry], len(g.aggsFields))
	v := reflect.ValueOf(instance).Elem()
	for i := range aggs {
		aggs[i] = v.Field(g.aggsFields[i].index).Interface().(Aggregate[stream.Entry])
	}

	if len(old) != len(aggs) {
		return instance, aggs, nil
	}

	// parallel Copy each aggregate
	done := make(chan error, len(aggs)) // buffer preserves order
	for i := range aggs {
		go func(i int) {
			var prod snapshot.Production
			if g.Snapshots != nil {
				var err error
				prod, err = g.Snapshots.Make(g.aggsFields[i].aggName, offset)
				if err != nil {
					log.Print("snapshot omitted: ", err)
				}
			}

			err := Copy(aggs[i], old[i], prod)
			done <- err

			switch {
			case prod == nil:
				break
			case err == nil:
				err := prod.Commit()
				if err != nil {
					log.Print("snapshot loss: ", err)
				}
			default:
				err := prod.Abort()
				if err != nil {
					log.Print("snapshot abandon: ", err)
				}
			}
		}(i)
	}

	var errs []error
	for range aggs {
		err := <-done
		if err != nil {
			errs = append(errs, err)
		}
	}
	err = errors.Join(errs...)
	if err != nil {
		return nil, nil, err
	}
	return instance, aggs, nil
}

// ErrLiveFuture denies freshness.
var ErrLiveFuture = errors.New("aggregate from future not available")

// LiveSince returns aggregates no older than notBefore. The notBefore range is
// protected with ErrLiveFuture.
func (g *Group[Aggs]) LiveSince(ctx context.Context, notBefore time.Time) (Fix[Aggs], error) {
	tolerance := time.Since(notBefore)
	if tolerance < 0 {
		return Fix[Aggs]{}, ErrLiveFuture
	}

	ctxDone := ctx.Done()
	select {
	case fix := <-g.live:
		switch {
		case fix == nil:
			break // discard placeholder
		case fix.LiveAt.Before(notBefore):
			break // discard expired
		default:
			g.live <- fix // unlock singleton
			return *fix, nil
		}
	case <-ctxDone:
		return Fix[Aggs]{}, ctx.Err()
	}

	for {
		select {
		case fix := <-g.release:
			if fix.LiveAt.Before(notBefore) {
				continue // discard again
				// Such unfortunate waste could be
				// avoided with a feedback channel.
			}
			g.live <- &fix // unlock
			return fix, nil
		case <-ctxDone:
			g.live <- nil // unlock [waste for nothing]
			return Fix[Aggs]{}, ctx.Err()
		}
	}
}
