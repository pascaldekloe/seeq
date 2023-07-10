package seeq

import "github.com/pascaldekloe/seeq/stream"

// UnmarshalProxy provides an Aggregate[stream.Entry] interface for a collection
// of delegates. Each stream.Entry does unmarshal once only.
type UnmarshalProxy[T any] struct {
	// Aggregate delegates get invoked sequentially in slice order.
	Aggs []Aggregate[T]

	// UnmarshalInto sets the out pointer to the interpretation of in. The
	// stream entry is dropped when the return is false. Implementations are
	// encouraged to recycle (parts of) T.
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

		err := p.addNextEach(p.buf[since:i], offset+since)
		if err != nil {
			return err
		}

		since = uint64(uint(i + 1)) // skip failed unmarshal
	}

	return p.addNextEach(p.buf[since:], offset+since)
}

func (p *UnmarshalProxy[T]) addNextEach(batch []T, offset uint64) error {
	for i := range p.Aggs {
		err := p.Aggs[i].AddNext(batch, offset)
		if err != nil {
			return err
		}
	}
	return nil
}
