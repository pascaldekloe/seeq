package seeq

import (
	"errors"

	"github.com/pascaldekloe/seeq/stream"
)

// AddNextCall has the arguments of an Aggregate invocation.
type addNextCall[T any] struct {
	batch  []T
	offset uint64
}

// Proxy forwards Aggregate (AddNext) invocation to its delegates. Delagates
// from the constructor get invoked sequentially. AddParallel launches its own
// routines.
type Proxy[T any] struct {
	sequential []Aggregate[T]

	parallelN int
	// no buffer for low latency and more efficent handover
	parallelFeed chan addNextCall[T]
	parallelErrs chan error
	parallelDone chan struct{}
}

// NewProxy is the default constructor. The arguments, if any, get invoked
// consecutively in order of appearence for every AddNext.
func NewProxy[T any](sequential ...Aggregate[T]) *Proxy[T] {
	return &Proxy[T]{
		sequential:   sequential,
		parallelN:    0,
		parallelFeed: make(chan addNextCall[T]),
		parallelErrs: make(chan error),
		parallelDone: make(chan struct{}),
	}
}

// AddParallel launches a new goroutine dedicaded to the Aggregate. Use Halt to
// clean up if needed. Slow aggregation may bennefit from parallelism at the
// cost of two channel transfers per AddNext: slice plus 64 bits, and an error.
func (p *Proxy[T]) AddParallel(agg Aggregate[T]) {
	p.parallelN++

	go func() {
		for call := range p.parallelFeed {
			p.parallelErrs <- agg.AddNext(call.batch, call.offset)
		}
		p.parallelDone <- struct{}{}
	}()
}

// Halt removes all Aggregates, and it stops all parallel routines. Use only
// after aggregation stopped. Most usecases will run their aggregates for the
// entire lifespan of the application though.
func (p *Proxy[T]) Halt() {
	p.sequential = nil

	close(p.parallelFeed)
	for n := p.parallelN; n > 0; n-- {
		<-p.parallelDone
	}
	p.parallelFeed = nil
	p.parallelN = 0
}

// AddNext implements the Aggregate interface.
func (p *Proxy[T]) AddNext(batch []T, offset uint64) error {
	// any and all parallel routines should be waiting at this point
	for n := p.parallelN; n > 0; n-- {
		p.parallelFeed <- addNextCall[T]{batch, offset}
	}

	var errs []error

	// run sequential list
	for i := range p.sequential {
		err := p.sequential[i].AddNext(batch, offset)
		if err != nil {
			errs = append(errs, err)
		}
	}

	// await and collect parallel results
	for n := p.parallelN; n > 0; n-- {
		// each routine sends one error—nil or not
		err := <-p.parallelErrs
		if err != nil {
			errs = append(errs, err)
		}
	}
	// any and all parallel routines should be waiting again

	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

// UnmarshalProxy provides an Aggregate[stream.Entry] interface for an
// Aggregate[T] delegate.
type UnmarshalProxy[T any] struct {
	Aggregate[T]

	// UnmarshalInto sets pointer out to the interpretation of in. A false
	// return causes the stream entry to be skipped instead. Implementations
	// are encouraged to recycle (parts of) T.
	UnmarshalInto func(out *T, in stream.Entry, offset uint64) bool

	buf []T
}

// AddNext implements the Aggregate[stream.Entry] interface.
func (p *UnmarshalProxy[T]) AddNext(batch []stream.Entry, offset uint64) error {
	if len(batch) <= cap(p.buf) {
		p.buf = p.buf[:len(batch)]
	} else {
		p.buf = make([]T, len(batch))
	}

	var since uint64
	for i := range batch {
		ok := p.UnmarshalInto(&p.buf[i], batch[i], offset+uint64(uint(i)))
		if ok {
			continue
		}

		err := p.Aggregate.AddNext(p.buf[since:i], offset+since)
		if err != nil {
			return err
		}

		since = uint64(uint(i + 1)) // skip failed unmarshal
	}

	return p.Aggregate.AddNext(p.buf[since:], offset+since)
}
