//go:build unix && !omitunix

package stream_test

import (
	"math/rand"
	"os"
	"testing"

	"github.com/pascaldekloe/seeq/stream"
)

func TestFramedFileWriter(t *testing.T) {
	dir := t.TempDir()
	f, err := os.CreateTemp(dir, "simplef.")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	w := stream.NewFramedFileWriter(f)
	err = w.Write([]stream.Entry{
		{},
		{"text", nil},
		{"", []byte{'A'}},
		{"text", []byte{'A', 'B'}},
	})
	if err != nil {
		t.Error("write error:", err)
	}

	bytes, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	const want = "\x00\x00\x00\x00" +
		"\x00\x00\x00\x04text" +
		"\x00\x00\x01\x00A" +
		"\x00\x00\x02\x04textAB"
	if string(bytes) != want {
		t.Errorf("got file content: %q\nwant file content: %q", bytes, want)
	}
}

func BenchmarkFramedFileWriter(b *testing.B) {
	const mediaType = "application/test+octet-stream;v=0.13;tag=true"
	bytes := make([]byte, 1024)
	rand.Read(bytes)

	f, err := os.OpenFile(b.TempDir()+"/FramedWriter.bench", os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	b.Run("1KiB", func(b *testing.B) {
		benchBatchN := func(b *testing.B, n int) {
			b.SetBytes(int64(len(bytes)))

			batch := make([]stream.Entry, n)
			for i := range batch {
				batch[i].MediaType = mediaType
				batch[i].Payload = bytes[:1024]
			}

			w := stream.NewFramedFileWriter(f)
			for n := b.N; n > 0; n -= len(batch) {
				if n < len(batch) {
					batch = batch[:n]
				}
				err := w.Write(batch)
				if err != nil {
					b.Fatal("write error:", err)
				}
			}
		}

		b.Run("Batch1", func(b *testing.B) {
			benchBatchN(b, 1)
		})
		b.Run("Batch20", func(b *testing.B) {
			benchBatchN(b, 20)
		})
		b.Run("Batch400", func(b *testing.B) {
			benchBatchN(b, 400)
		})
	})
}
