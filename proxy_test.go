package seeq_test

import (
	"fmt"
	"log"
	"math/big"

	"github.com/pascaldekloe/seeq"
	"github.com/pascaldekloe/seeq/stream"
)

// Counts is a demo aggregate.
type Counts struct {
	N   int64
	Sum big.Int
}

// Avg is a query method example.
func (c *Counts) Avg() *big.Rat {
	return new(big.Rat).SetFrac(&c.Sum, big.NewInt(c.N))
}

// AddNext implements the seeq.Aggregate interface.
func (c *Counts) AddNext(batch []big.Int, offset uint64) error {
	c.N += int64(len(batch))
	for i := range batch {
		c.Sum.Add(&c.Sum, &batch[i])
	}
	return nil
}

func ExampleUnmarshalProxy() {
	var c Counts

	proxy := seeq.UnmarshalProxy[big.Int]{
		Aggs: []seeq.Aggregate[big.Int]{&c},
		UnmarshalInto: func(out *big.Int, in stream.Entry, offset uint64) bool {
			t := stream.CachedMediaType(in.MediaType)
			switch {
			case t.Subtype == "octet-stream":
				out.SetBytes(in.Payload)
				return true

			case t.Type == "text":
				_, ok := out.SetString(string(in.Payload), 10)
				if !ok {
					log.Printf("malformed decimal at stream offset %d", offset)
					return false
				}
				return true
			}

			return false
		},
	}

	err := proxy.AddNext([]stream.Entry{
		{"application/octet-stream", []byte{1}},
		{"text/plain", []byte{'2'}},
	}, 9765625)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("got %d in total; average of %s\n", &c.Sum, c.Avg().FloatString(2))
	// Output:
	// got 3 in total; average of 1.50
}
