# Seeq

Command–query separation (CQS) for the Go programming language.
The API enforces fool-proof architecture, with an eye for performance.

This is free and unencumbered software released into the
[public domain](https://creativecommons.org/publicdomain/zero/1.0).

[![Go Doc](https://pkg.go.dev/badge/github.com/pascaldekloe/seeq.svg)](https://pkg.go.dev/github.com/pascaldekloe/seeq)
[![Build](https://github.com/pascaldekloe/seeq/actions/workflows/go.yml/badge.svg)](https://github.com/pascaldekloe/seeq/actions/workflows/go.yml)


## Concept

*Streams* are append-only[^1] collections. As such, each entry inherits a fixed
*sequence number*. Stream positions start at *offset* zero. The first entry gets
sequence number one, which sets the stream offset also to one, and so on.

#### Stream Attributes

* Name
* Offset

#### Entry Attributes

* Payload
* Media Type
* Sequence Number

*Live* streams are subject to contious growth, as opposed to suspended streams.
Read access is limited to chronological iteration. Reads may start at any offset
though.

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
	Statuses  *RFCStatusIndex `aggregate:"rfc-status"`
	Refs      *ReferenceGraph `aggregate:"rfc-ref"`
	Authors   *people.NameDB  `aggregate:"rfc-author"`
	Chapters  ChapterRepo     `aggregate:"rfc-chapter"`
	Abstracts *fulltext.Index `aggregate:"rfc-abstract"`
}

// NewRFCSeries returns a new set of aggregates.
func NewRFCSeries() (*RFCSeries, error) {
	// initialize/construct each aggregate
	…
}
```

The example above can be fed with `seeq.NewReleaseSync(NewRFCSeries)`.

```go
	// resolve aggregates no older than a minute ago
	fix, err := sync.LiveSince(ctx, time.Now().Add(-time.Minute))
	if err != nil {
		log.Print("context expired during aggregate aquire")
		return
	}

	// query the read-only fix; note the absense of errors
	log.Printf("all aggregates from %T are at offset %d", fix.Q, fix.Offset)
	log.Print("got %d RFC authors", fix.Q.Authors.N)
	log.Print("got %d references to RFC 2616", len(fix.Q.Refs.PerRFC[2616].Inbound))
}
```


## Zero-Copy

The stream Reader and Writer implementations don't buffer.

```
BenchmarkSimpleWriter/File/1KiB/Batch1-8         	  493830	      2420 ns/op	 423.14 MB/s
BenchmarkSimpleWriter/File/1KiB/Batch20-8        	 2849122	       415.2 ns/op	2466.26 MB/s
BenchmarkSimpleWriter/File/1KiB/Batch400-8       	 3836694	       371.1 ns/op	2759.25 MB/s
```


## References

* https://martinfowler.com/bliki/CQRS.html


[^1]: Payloads can be deleted.
