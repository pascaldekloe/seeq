//go:build unix && !omitunix

package stream

import (
	"encoding/binary"
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

// NewFramedFileWriter is like NewFramedWriter, but for regular files only.
func NewFramedFileWriter(f *os.File) Writer {
	return &fDWriter{fd: f.Fd()}
}

type fDWriter struct {
	fd      uintptr
	headers [][4]byte // reusable buffer
	vectors []syscall.Iovec
}

// Write implements the Writer interface.
func (w *fDWriter) Write(batch []Entry) error {
	if len(batch) > cap(w.headers) {
		w.headers = make([][4]byte, len(batch))
		w.vectors = make([]syscall.Iovec, 0, len(batch)*3)
	}

	// collect the buffers to be written
	v := w.vectors[:0]
	for i := range batch {
		if len(batch[i].MediaType) > 255 || len(batch[i].Payload) > 0xFF_FFFF {
			return ErrSizeMax
		}
		binary.BigEndian.PutUint32(w.headers[i][:], uint32(len(batch[i].Payload)<<8|len(batch[i].MediaType)))
		v = append(v, syscall.Iovec{Base: &w.headers[i][0], Len: 4})
		if batch[i].MediaType != "" {
			// go won't allow address of string content
			b := (*[]byte)(unsafe.Pointer(&batch[i].MediaType))
			v = append(v, syscall.Iovec{
				Base: &(*b)[0],
				Len:  uint64(uint(len(batch[i].MediaType))),
			})
		}
		if len(batch[i].Payload) != 0 {
			v = append(v, syscall.Iovec{
				Base: &batch[i].Payload[0],
				Len:  uint64(uint(len(batch[i].Payload))),
			})
		}
	}

	// write all of v
	max := vectorLimit()
	for len(v) != 0 {
		l := len(v)
		if l > max {
			l = max
		}
		wrote, _, errno := syscall.Syscall(syscall.SYS_WRITEV, w.fd, uintptr(unsafe.Pointer(&v[0])), uintptr(l))

		// apply write count; -1 means aborted
		if wrote != 0 && wrote != ^uintptr(0) {
			pass := uint64(wrote)
			for i := range v {
				switch l := v[i].Len; {
				case l < pass:
					pass -= l
					continue

				case l == pass:
					// v[i] fully written
					v = v[i+1:]
				case l > pass:
					// v[i] partial write
					v = v[i:]
					v[0].Len -= l
					v[0].Base = (*byte)(unsafe.Pointer((uintptr(unsafe.Pointer(v[0].Base)) + uintptr(l))))
				}
				break
			}
		}

		// handle error
		switch errno {
		case 0, syscall.EINTR:
			break
		case syscall.EAGAIN:
			// BUG(pascaldekloe): Non-blocking I/O left unchecked.
			fallthrough
		default:
			return errno
		}
	}

	return nil
}

// VectorLimit returns the maximum amount of iovec(2) allowed per writev(2).
func vectorLimit() int {
	// IOV_MAX from <limits.h> unavailable
	// https://github.com/golang/go/issues/58623
	max := 1024
	// copied from internal/poll/writev.go; watch for updates
	if runtime.GOOS == "aix" || runtime.GOOS == "solaris" {
		// IOV_MAX is set to XOPEN_IOV_MAX on AIX and Solaris.
		max = 16
	}
	return max
}
