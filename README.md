# Seeq

Command–query separation (CQS) for the Go programming language.
The API enforces fool-proof architecture, with an eye for performance.

This is free and unencumbered software released into the
[public domain](https://creativecommons.org/publicdomain/zero/1.0).

[![Build](https://github.com/pascaldekloe/seeq/actions/workflows/go.yml/badge.svg)](https://github.com/pascaldekloe/seeq/actions/workflows/go.yml)


## Concept

**Streams** are append-only[^1] collections. As such, each entry inherits a
fixed **sequence number**, starting with one—no gaps. Live streams grow over
time, as opposed to dead streams, which are read-only.

#### Entry Attributes

* Payload [blob]
* Media Type [MIME]
* Sequence Number

Read access is limited to chronological iteration. Reads may start at a sequence
number offset though.

**Aggregates** consume streams to collect information for one or more specific
questions/queries. The limited scope allows for more optimization on each case.
Design can pick the most suitable technology for each aggregate/query instead of
searching for generic components and reuse.

An aggregate is considdered to be live once it reaches the end of its stream,
i.e., the moment it includes all information available.


[^1]: Payloads can be deleted.


## Read-Only View

Querying on aggregates which are connected to a live stream burdens development
with concurrency issues. Instead, the synchronisation methods update aggregates
in isolation. Aggregates are taken offline for queries (on demand) once live.


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

Aggregates can build from streams in isolation. Eventual consistency can get
cumbersome when multiple aggregates are needed in conjunction. Seeq provides an
option to update aggregates in a group.

```go
// RFCSeries demonstrates an arbitrary custom aggregate set.
type RFCSeries struct {
	Statuses  *RFCStatusIndex `aggregate`
	Reffs     *ReferenceGraph `aggregate`
	Authors   *people.NameDB  `aggregate`
	Chapters  ChapterRepo     `aggregate`
	Abstracts *fulltext.Index `aggregate`
}

// NewRFCSeries returns a new set of empty aggregates ready for use.
func NewRFCSeries() (*PaperInedx, error) {
	…
}
```

The example above can be synchronised with just `seeq.NewFastGroup(NewRFCSeries)`.

```go
	// get aggregates which were live no longer than a minute ago
	offline, err := group.LiveSince(ctx, time.Now().Add(-time.Minute))
	if err != nil {
		log.Print("context expired during aggregate aquire")
		return
	}

	log.Print("all aggregates are at sequence number ", offline.SeqNo)
	log.Print("got ", offline.Set.Authors.N, " RFC authors")
	log.Print("got ", len(offline.Set.Reffs.PerRFC[2616].Inbound), " references to RFC 2616")
}
```


## References

* https://martinfowler.com/bliki/CQRS.html
