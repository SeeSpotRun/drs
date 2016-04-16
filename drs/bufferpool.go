package drs

// this file provides the Bufferpool implementation

import (
	"fmt"
	"runtime"
)

// Buf is a reusable byte buffer
type Buf []byte

var bufc chan Buf
var reqc chan Token
var nbufs int

// InitPool initialises the buffer pool.  It takes arguments
// bufsize (buffer size in bytes) and maxbufs (a hard limit on
// the number of buffers created)
func InitPool(bufsize int, maxbufs int) {
	if bufc != nil {
		panic("hddreader/InitPool: Attempt to re-init pool")
	}
	bufc = make(chan (Buf), maxbufs)
	reqc = make(chan (Token))

	go func() {
		// if any requests received then make buffer, up to maxbufs limit
		for range reqc {
			if nbufs < maxbufs {
				nbufs++
				bufc <- make(Buf, bufsize)
			}
		}
	}()
}

// GetBuf gets a buffer from the pool
func GetBuf() Buf {
	select {
	case b := <-bufc:
		// recycled buffer
		return b
	default:
		// no buffers in pool; request new one
		reqc <- Token{}
		// wait for buffer to arrive
		return <-bufc
	}
}

// PutBuf returns a buffer to the pool
func PutBuf(b Buf) {
	select {
	// note: b is expanded to cap(b) before returning
	case bufc <- b[:cap(b)]: // should not block
	default:
		panic("Blocked returning buffer")
	}
}

// ClosePool closes the Bufferpool and returns the number of buffers used
func ClosePool() (used int, err error) {
	close(reqc)
	close(bufc)
	used = 0
	for range bufc {
		used++
	}
	runtime.GC()
	if used != nbufs {
		err = fmt.Errorf("Expected %d buffers, got %d", nbufs, used)
	}
	return
}
