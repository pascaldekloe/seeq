package stream_test

import (
	"fmt"

	"github.com/pascaldekloe/seeq/stream"
	"github.com/pascaldekloe/seeq/stream/streamtest"
)

func ExampleCursor() {
	// demo input
	r := streamtest.NewFixedReader(
		stream.Entry{"text", []byte("Ⅰ")},
		stream.Entry{"text", []byte("Ⅱ")},
		stream.Entry{"text", []byte("Ⅲ")},
		stream.Entry{"text", []byte("Ⅳ")},
		stream.Entry{"text", []byte("Ⅴ")},
	)

	// cursor setup
	const batchSize = 3
	c := stream.Cursor{R: r, Batch: make([]stream.Entry, 0, batchSize)}

	// print input
	for {
		err := c.Next()
		fmt.Printf("№ %d: ", c.SeqNo)
		for i := range c.Batch {
			fmt.Printf("%s ", c.Batch[i].Payload)
		}
		fmt.Println(err)
		if err != nil {
			return
		}
	}
	// Output:
	// № 3: Ⅰ Ⅱ Ⅲ <nil>
	// № 5: Ⅳ Ⅴ EOF
}
