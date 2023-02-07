# Seeq

Command–query separation (CQS) for the Go programming language.
The API enforces fool-proof architecture, with an eye for performance.

This is free and unencumbered software released into the
[public domain](https://creativecommons.org/publicdomain/zero/1.0).

[![Build](https://github.com/pascaldekloe/seeq/actions/workflows/go.yml/badge.svg)](https://github.com/pascaldekloe/seeq/actions/workflows/go.yml)


## Concept

*Streams* are append-only[^1] collections. As such, each entry inherits a fixed
*sequence number*—no gaps. *Live* streams grow over time with a shifting end, as
opposed to suspended streams, which are read-only.

#### Entry Attributes

* Payload
* Media Type
* Sequence Number

Read access is limited to chronological iteration. Reads may start at a sequence
number (offset) though.

*Aggregates* consume streams to collect information for one or more specific
questions/queries. The limited scope allows for more optimization on each case.
Design can pick the most suitable technology for each aggregate/query instead of
searching for generic components and reuse.

An aggregate is considdered to be live once it reaches the end of its stream,
i.e., a moment in which it includes all available information.


## Read-Only View

Querying on aggregates which are connected to a live stream burdens development
with concurrency issues. Instead, the synchronisation methods update aggregates
in isolation. Aggregates go offline on demand for queries.


         (start)
            │
    [ new aggregate ]<────────────────┐
            │                         │
    ¿ have snapshot ? ─── no ──┐      │
            │                  │      │
           yes                 │      │
            │                  │      │
    [ load snapshot ]          │      │
            │                  │      │
            ├<─────────────────┘      │
            │                         │
    [  read record  ] <────────┐      │
            │                  │      │
    ¿ end of stream ? ─── no ──┤      │
            │                  │      │
           yes                 │      │
            │                  │      │
    ¿ query request ? ─── no ──┘      │
            │                         │
           yes ── [ make snapshot ] ──┘
            │
    [ serve queries ] <────────┐
            │                  │
    ¿  still recent ? ── yes ──┘
            │
            no
            │
          (stop)



## Aggregate Synchronization

Aggregates generally build from streams in isolation. Eventual consistency can
get cumbersome when multiple aggregates are needed in conjunction. Seeq provides
an option to update aggregates in a group with just a `struct` container and its
contstuctor.

```go
// RFCSeries demonstrates the grouping of five custom aggregates.
type RFCSeries struct {
	Statuses  *RFCStatusIndex `aggregate`
	Reffs     *ReferenceGraph `aggregate`
	Authors   *people.NameDB  `aggregate`
	Chapters  ChapterRepo     `aggregate`
	Abstracts *fulltext.Index `aggregate`
}

// NewRFCSeries returns a new set of empty aggregates ready for use.
func NewRFCSeries() (*RFCSeries, error) {
	…
}
```

For example, a `seeq.NewLightGroup[RFCSeries](NewRFCSeries)` feeds each tagged
field. A live copy is aquired with a freshness constraint.

```go
	// get aggregates which were live no longer than a minute ago
	offline, err := group.LiveSince(ctx, time.Now().Add(-time.Minute))
	if err != nil {
		log.Print("context expired during aggregate aquire")
		return
	}

	log.Printf("all %T aggregates are at sequence № %d", offline.Set, offline.SeqNo)
	log.Print("got %d RFC authors", offline.Set.Authors.N)
	log.Print("got %d references to RFC 2616", len(offline.Set.Reffs.PerRFC[2616].Inbound))
}
```


## References

* https://martinfowler.com/bliki/CQRS.html


[^1]: Payloads can be deleted.
