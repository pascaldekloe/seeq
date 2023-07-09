package seeq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/pascaldekloe/seeq/snapshot"
	"github.com/pascaldekloe/seeq/stream"
)

// ReleaseSync updates a T sequentially. Once a T is live, the instance may be
// taken offline for queries. Synchonisation will continue with a new copy until
// the previous expires (with notBefore from LiveSince).
//
//	     (start)
//	        │
//	[ new aggregate ]<────────────────┐
//	        │                         │
//	¿ have snapshot ? ─── no ──┐      │
//	        │                  │      │
//	       yes                 │      │
//	        │                  │      │
//	[ load snapshot ]          │      │
//	        │                  │      │
//	        ├<─────────────────┘      │
//	        │                         │
//	[  read record  ] <────────┐      │
//	        │                  │      │
//	¿ end of stream ? ─── no ──┤      │
//	        │                  │      │
//	       yes                 │      │
//	        │                  │      │
//	¿ query request ? ─── no ──┘      │
//	        │                         │
//	       yes ── [ make snapshot ] ──┘
//	        │
//	[ serve queries ] <────────┐
//	        │                  │
//	¿  still recent ? ── yes ──┘
//	        │
//	        no
//	        │
//	      (stop)
//
// T must be a struct with one or more of its fields tagged as
// `aggregate:"some_name"`. Each of such fields must implement
// SnapshotAggregate[stream.Entry].
type ReleaseSync[T any] struct {
	groupConfig[T]

	// latest singleton, or nil initially
	live chan *Fix[T]
	// working copy handover
	release chan Fix[T]
	// halts synchronisation
	interrupt chan struct{}

	Snapshots snapshot.Archive // optional
}

// NewReleaseSync validates the configuration of T before returning a new setup.
// Constructor must return a new instance in its initial state, i.e., the return
// must match stream offset zero.
func NewReleaseSync[T any](constructor func() (*T, error)) (*ReleaseSync[T], error) {
	// read & validate configuration
	c, err := newGroupConfig(constructor)
	if err != nil {
		return nil, err
	}

	sync := ReleaseSync[T]{
		groupConfig: *c,
		live:        make(chan *Fix[T], 1),
		release:     make(chan Fix[T]),
		interrupt:   make(chan struct{}, 1),
	}
	sync.live <- nil // initial placeholder

	return &sync, nil
}

// ErrInterrupt is the result of an Interrupt request.
var ErrInterrupt = errors.New("aggregate synchronisation received an interrupt")

// Interrupt halts any ongoing synchronisation with ErrInterrupt.
func (sync *ReleaseSync[T]) Interrupt() {
	select {
	case sync.interrupt <- struct{}{}:
		break // signal installed
	default:
		break // signal already pending
	}
}

// SyncFrom a stream until failure or Interrupt.
func (sync *ReleaseSync[T]) SyncFrom(r stream.Reader) error {
	// clear any pending interrupt request
	select {
	case <-sync.interrupt:
	default:
	}

	group, aggs, err := sync.newGroup()
	if err != nil {
		return err
	}

	return sync.syncGroupFrom(r, group, aggs)
}

// SyncFromRepo until failure or Interrupt.
func (sync *ReleaseSync[T]) SyncFromRepo(streams stream.Repo, streamName string) error {
	if sync.Snapshots == nil {
		return sync.SyncFrom(streams.ReadAt(streamName, 0))
	}

	// clear any pending interrupt request
	select {
	case <-sync.interrupt:
	default:
	}

	group, aggs, err := sync.newGroup()
	if err != nil {
		return err
	}

	offset, err := snapshot.LastCommon(sync.Snapshots, sync.aggNames...)
	if err != nil {
		return err
	}
	if offset == 0 {
		return sync.SyncFrom(streams.ReadAt(streamName, 0))
	}

	errs := make(chan error, len(aggs))
	for i := range aggs {
		go func(i int) {
			r, err := sync.Snapshots.Open(sync.aggNames[i], offset)
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
		return fmt.Errorf("synchronisation halt on %d snapshot recovery error(s)", errN)
	}

	return sync.syncGroupFrom(streams.ReadAt(streamName, offset), group, aggs)
}

func (sync *ReleaseSync[T]) syncGroupFrom(r stream.Reader, group *T, aggs []SnapshotAggregate[stream.Entry]) error {
	// once live the Fix is offered to release
	var offerTimer *time.Timer // short-poll delay
	buf := make([]stream.Entry, 99)
	for {
		lastReadTime, err := Sync(r, buf, aggs...)
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
		case sync.release <- Fix[T]{Q: group, Offset: r.Offset(), LiveAt: lastReadTime}:
			if !offerTimer.Stop() {
				<-offerTimer.C
			}

			// swap working copy
			group, aggs, err = sync.forkGroup(r.Offset(), aggs)
			if err != nil {
				return err
			}
		case <-sync.interrupt:
			return ErrInterrupt
		}
	}
}

func (sync *ReleaseSync[T]) forkGroup(offset uint64, old []SnapshotAggregate[stream.Entry]) (*T, []SnapshotAggregate[stream.Entry], error) {
	group, aggs, err := sync.newGroup()
	if err != nil {
		return nil, nil, fmt.Errorf("aggregate synchronization halt on instantiation: %w", err)
	}

	// parallel Copy each aggregate
	done := make(chan error, len(aggs)) // buffer preserves order
	for i := range aggs {
		go func(i int) {
			var prod snapshot.Production
			if sync.Snapshots != nil {
				var err error
				prod, err = sync.Snapshots.Make(sync.aggNames[i], offset)
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
	return group, aggs, nil
}

// ErrLiveFuture denies freshness.
var ErrLiveFuture = errors.New("aggregate from future not available")

// LiveSince returns a T live no older than notBefore. The notBefore range is
// protected with ErrLiveFuture.
func (sync *ReleaseSync[T]) LiveSince(ctx context.Context, notBefore time.Time) (Fix[T], error) {
	tolerance := time.Since(notBefore)
	if tolerance < 0 {
		return Fix[T]{}, ErrLiveFuture
	}

	ctxDone := ctx.Done()
	select {
	case fix := <-sync.live:
		switch {
		case fix == nil:
			break // discard placeholder
		case fix.LiveAt.Before(notBefore):
			break // discard expired
		default:
			sync.live <- fix // unlock singleton
			return *fix, nil
		}
	case <-ctxDone:
		return Fix[T]{}, ctx.Err()
	}

	for {
		select {
		case fix := <-sync.release:
			if fix.LiveAt.Before(notBefore) {
				continue // discard again
				// Such unfortunate waste could be
				// avoided with a feedback channel.
			}
			sync.live <- &fix // unlock
			return fix, nil
		case <-ctxDone:
			sync.live <- nil // unlock [waste for nothing]
			return Fix[T]{}, ctx.Err()
		}
	}
}
