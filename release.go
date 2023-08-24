package seeq

import (
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
	newGroup func() (*T, error)

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
	c, err := newGroupConfig[T]()
	if err != nil {
		return nil, err
	}

	sync := ReleaseSync[T]{
		groupConfig: *c,
		newGroup:    constructor,
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

	group, err := sync.newGroup()
	if err != nil {
		return err
	}
	proxy, snaps := sync.groupConfig.extract(group)
	return sync.syncGroupFrom(r, group, proxy, snaps)
}

// SyncFromRepo until failure or Interrupt.
func (sync *ReleaseSync[T]) SyncFromRepo(streams stream.Repo, streamName string) error {
	if sync.Snapshots == nil {
		return sync.SyncFrom(streams.ReaderAt(streamName, 0))
	}

	// clear any pending interrupt request
	select {
	case <-sync.interrupt:
	default:
	}

	group, err := sync.newGroup()
	if err != nil {
		return err
	}
	proxy, snaps := sync.groupConfig.extract(group)

	offset, err := snapshot.LastCommon(sync.Snapshots, sync.groupConfig.aggNames...)
	if err != nil {
		return err
	}
	if offset == 0 {
		return sync.SyncFrom(streams.ReaderAt(streamName, 0))
	}

	names := sync.groupConfig.aggNames
	errs := make(chan error, len(names))
	for i := range names {
		go func(i int) {
			r, err := sync.Snapshots.Open(names[i], offset)
			if err != nil {
				errs <- err
				return
			}
			err = snaps[i].LoadFrom(r)
			r.Close()
			errs <- err
		}(i)
	}

	var errN int
	for range snaps {
		err := <-errs
		if err != nil {
			errN++
			log.Print(err)
		}
	}
	if errN != 0 {
		return fmt.Errorf("synchronisation halt on %d snapshot recovery error(s)", errN)
	}

	return sync.syncGroupFrom(streams.ReaderAt(streamName, offset), group, proxy, snaps)
}

func (sync *ReleaseSync[T]) syncGroupFrom(r stream.Reader, group *T, proxy *Proxy[stream.Entry], snaps []Snapshotable) error {
	// once live the Fix is offered to release
	var offerTimer *time.Timer // short-poll delay
	buf := make([]stream.Entry, 99)
	for {
		lastReadTime, err := Sync(proxy, r, buf)
		if err != nil {
			return err
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
			group, proxy, snaps, err = sync.forkGroup(r.Offset(), snaps)
			if err != nil {
				return err
			}
		case <-sync.interrupt:
			return ErrInterrupt
		}
	}
}

func (sync *ReleaseSync[T]) forkGroup(offset uint64, old []Snapshotable) (*T, *Proxy[stream.Entry], []Snapshotable, error) {
	group, err := sync.newGroup()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("aggregate synchronization halt on instantiation: %w", err)
	}
	proxy, snaps := sync.groupConfig.extract(group)

	// parallel Copy each aggregate
	done := make(chan error, len(snaps)) // buffer preserves order
	for i := range snaps {
		go func(i int) {
			var prod snapshot.Production
			if sync.Snapshots != nil {
				var err error
				prod, err = sync.Snapshots.Make(sync.aggNames[i], offset)
				if err != nil {
					log.Print("snapshot omitted: ", err)
				}
			}

			err := Copy(snaps[i], old[i], prod)
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
	for range snaps {
		err := <-done
		if err != nil {
			errs = append(errs, err)
		}
	}
	err = errors.Join(errs...)
	if err != nil {
		return nil, nil, nil, err
	}
	return group, proxy, snaps, nil
}

// ErrCancel signals a client abort.
var ErrCancel = errors.New("aggregate lookup canceled")

// ErrLiveFuture denies freshness.
var ErrLiveFuture = errors.New("aggregate from future not available")

// LiveSince returns a T which was live no older than notBefore. The notBefore
// range is protected with ErrLiveFuture. Cancel is optional as nil just blocks.
// Cancel receive causes an ErrCancel.
func (sync *ReleaseSync[T]) LiveSince(notBefore time.Time, cancel <-chan struct{}) (Fix[T], error) {
	tolerance := time.Since(notBefore)
	if tolerance < 0 {
		return Fix[T]{}, ErrLiveFuture
	}

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
	case <-cancel:
		return Fix[T]{}, ErrCancel
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
		case <-cancel:
			sync.live <- nil // unlock [waste for nothing]
			return Fix[T]{}, ErrCancel
		}
	}
}
