package seeq_test

import (
	"fmt"
	"log"
	"math/big"
	"slices"
	"strings"
	"testing"

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
		Aggregate:     &c,
		UnmarshalInto: unmarshalInto,
	}

	samples := []stream.Entry{
		{"application/octet-stream", []byte{1}},
		{"text/plain", []byte{'2'}},
	}
	err := proxy.AddNext(samples, 9765625)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("got %d in total; average of %s\n", &c.Sum, c.Avg().FloatString(2))
	// Output:
	// got 3 in total; average of 1.50
}

// UnmarshalInto sets the out pointer to the interpretation of in, confrom the
// UnmarshalProxy structure.
func unmarshalInto(out *big.Int, in stream.Entry, offset uint64) bool {
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
}

type intRecord struct {
	offsets []uint64
	values  []int64
}

func (r *intRecord) AddNext(batch []big.Int, offset uint64) error {
	for i := range batch {
		r.offsets = append(r.offsets, offset+uint64(i))
		r.values = append(r.values, batch[i].Int64())
	}
	return nil
}

func TestUnmarshalProxy(t *testing.T) {
	tests := []struct {
		feed        []string
		wantOffsets []uint64
		wantValues  []int64
	}{
		{
			feed:        []string{},
			wantOffsets: []uint64{},
			wantValues:  []int64{},
		},
		{
			feed:        []string{"1", "2", "7"},
			wantOffsets: []uint64{1000, 1001, 1002},
			wantValues:  []int64{1, 2, 7},
		},
		{
			feed:        []string{"1", "b", "7"},
			wantOffsets: []uint64{1000, 1002},
			wantValues:  []int64{1, 7},
		},
		{
			feed:        []string{"a", "2", "b"},
			wantOffsets: []uint64{1001},
			wantValues:  []int64{2},
		},
	}

	for _, test := range tests {
		t.Run(strings.Join(test.feed, ","), func(t *testing.T) {
			var r intRecord

			proxy := seeq.UnmarshalProxy[big.Int]{
				Aggregate:     &r,
				UnmarshalInto: unmarshalInto,
			}

			var batch []stream.Entry
			for _, s := range test.feed {
				batch = append(batch, stream.Entry{
					MediaType: "text/plain",
					Payload:   []byte(s),
				})
			}

			err := proxy.AddNext(batch, 1000)
			if err != nil {
				t.Error("AddNext got error:", err)
			}
			if !slices.Equal(r.offsets, test.wantOffsets) {
				t.Errorf("got offsets %d, want %d", r.offsets, test.wantOffsets)
			}
			if !slices.Equal(r.values, test.wantValues) {
				t.Errorf("got values %d, want %d", r.values, test.wantValues)
			}
		})
	}
}
