/**
*  drs is free software: you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation, either version 3 of the License, or
*  (at your option) any later version.
*
*  drs is distributed in the hope that it will be useful,
*  but WITHOUT ANY WARRANTY; without even the implied warranty of
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*  GNU General Public License for more details.
*
*  You should have received a copy of the GNU General Public License
*  along with rmlint.  If not, see <http://www.gnu.org/licenses/>.
*
** Authors:
 *
 *  - Daniel <SeeSpotRun> T.   2016-2016 (https://github.com/SeeSpotRun)
 *
** Hosted on https://github.com/SeeSpotRun/drs
*
**/

// Package drs provides scheduling to optimise parallel reading of multiple
// files from rotational hard drives.  It does this by limiting number of open
// files, and by reading files in order of disk offsets.
// For ssd's, drs offers convenient limitation of the number of open
// file handle to prevent "too many open files" errors.
package drs

/*
 * TODO:
 * [ ] write drs_test
 * [ ] add ssd option
 * [ ] write some utilities to use this package:
 *     [ ] sums (fast checksum calculator)
 *     [ ] grep
 *     [ ] match (hash-based duplicate detector)
 *     [ ] exact (bytewise duplicate detector)
 *     [ ] retrofit to third-party utilities eg https://github.com/svent/sift
 * [ ] benchmarking & profiling
 */

import (
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"fmt"
)

var disks = make(map[uint64]*Disk)
var mx    sync.Mutex


// GetDisk gets the disk corresponding to a diskID, creating one if necessary
func GetDisk(id uint64, ssd bool) *Disk {
	mx.Lock()
	defer mx.Unlock()
	if d, ok := disks[id]; ok {
		return d
	}
	d := NewDisk(ssd)
	disks[id] = d
	d.Start(0)
	return d
}

// CloseDisks closes all disks
func CloseDisks() {
	for _, d := range(disks) {
		d.Close()
	}
}

// WaitDisks waits until there are no unfinished scheduled jobs
func WaitDisks() {
	for _, d := range(disks) {
		d.Wait()
	}
}


type DiskConfig struct {
	Read    int
	Window  int
	Process int
	Ahead   int64
	Behind  int64
	Lazy    float32
}

var HDD = DiskConfig{
	Read:    1,
	Window:  0,
	Process: 3 + runtime.NumCPU(),
	Ahead:   0,
	Behind:  0,
	Lazy:    0.5,
}

var Aggressive = DiskConfig{
	Read:    2,
	Window:  10,
	Process: 20 + runtime.NumCPU(),
	Ahead:   2 * 1024 * 1024,
	Behind:  512 * 1024,
	Lazy:    1.0,
}

var SSD = DiskConfig{
	Read:    runtime.NumCPU() * 2,
	Window:  0,
	Process: runtime.NumCPU() * 4,
	Ahead:   0,
	Behind:  0,
	Lazy:    -1.0,
}

const (
	defaultMaxRead   = 1 // number of files simultaneously reading
	defaultMaxWindow = 5 // number of file.File's simultaneously reading under ahead/behind clause
	// https://www.usenix.org/legacy/event/usenix09/tech/full_papers/vandebogart/vandebogart_html/index.html
	// suggests a window of opportunity for quick seeks in the +/- 0.5 MB range.  This article is
	// from Proc. USENIX (2009) when typical hard drive size was around 1TB; scaling to 4TB
	// drives suggests maybe a 2MB window
	defaultAhead  = 2 * 1024 * 1024
	defaultBehind = 1 * 1024 * 1024

	defaultBufSize    = 32 * 1024 // for buffering file.WriteTo() calls
	defaultBufCount   = 1024      // max number of buffers total
	defaultBufPerFile = 10        // number of buffers per file
)

// Token is a convenience type for signalling channels
type Token struct{}

type TokenReturn chan Token

func (t TokenReturn) Done() {
	if t != nil {
		t <- Token{}
	}
}



// A Disk schedules read operations for files.  It shoulds be created using NewDisk.  A call
// to Disk.Start() is required before the disk will allow associated files to start
// reading data.
//
// Example usage:
//  d := hddreader.NewDisk(1, 0, 0, 0, 100, 64)
//  for _, p := range(paths) {
//          wg.Add(1)
//          go read_stuff_from(p, &wg)
//          // calls hddreader.OpenFile() and file.Read() but will block until d.Start() called
//  }
//  d.Start()  // enables reading and will unblock pending Read() calls in disk offset order
//  wg.Wait()
//  d.Close()
type Disk struct {
	reqch   chan *request   // read job requests
	readch  TokenReturn     // signal that read job has finished reading and file has been closed
	donech  TokenReturn     // signal that read job has finished
	startch TokenReturn     // used by disk.Start() to signal start of reading
	wait    int             // how many pending reads to wait for before starting first read
	closech TokenReturn     // to signal disk close
	wg      sync.WaitGroup  // when closing disk, to wait for pending jobs to finish
	dirs    map[id]string   // used to check for path walk recursion
	mx      sync.Mutex      // for access to dirs
	ssd     bool
}



// NewDisk creates a new disk object to schedule read operations in order of increasing
// physical offset on the disk.
//
// Up to maxread files will be read concurrently.  An additional up to maxwindow files
// may be opened for reading if they are within (-behind,+ahead) bytes of
// the current head position. An additional up to maxopen files may be open for the
// sole purpose of reading disk offset.
//
// If bufkB > 0 then an internal buffer pool is created to buffer WriteTo() calls.  Note that while
// read order is preserved, buffering may change the order in which WriteTo(w) calls complete, depending
// on speed at which buffers are written to w.
func NewDisk(ssd bool) *Disk {
	config := HDD
	if ssd {
		config = SSD
	}

	if config.Read < 1 {
		panic("Need config.Read > 0")
	}

	if config.Process < 1 {
		config.Process = config.Read + config.Window + runtime.NumCPU()
	}

	d := &Disk{
		startch: make(TokenReturn),
		reqch:   make(chan (*request)),
		readch:  make(chan (Token), 10), // TODO: does buffer improve speed?
		closech: make(chan (Token)),
		donech:  make(chan (Token)),
		dirs:    make(map[id]string),
		ssd:     ssd,
	}

	//TODO: this is a bit hacky
	if bufc == nil {
		InitPool(defaultBufSize, defaultBufCount)
	}

	// start scheduler to prioritise Read() calls
	go d.scheduler(config)

	return d
}

// Start needs to be called once in order to enable file data to start reading.
// If wait > 0 then will wait until that many read requests have been registered before starting
func (d *Disk) Start(wait int) {
	d.wait = wait
	d.startch <- Token{}
}

// File wraps an os.File object with a custom Close() command that signals to disk
type File struct {
        *os.File
	disk    *Disk
        closed  bool
}

func (f *File) Close() error {
        if !f.closed {
                f.closed = true
		f.disk.readch <- Token{}
                return f.File.Close()
        }
        return fmt.Errorf("Warning: attempt to close a closed file")
}

func (r *request) Do(d *Disk) {
        f, e := os.Open(r.path)
	fi := &File{ f, d, e != nil }
	if e != nil {
		// signal finished reading
		fi.disk.readch <- Token{}
	}
	r.job.Go(fi, e)
	d.donech.Done()
}


// scheduler manages job requests and tries to process jobs in disk order
func (d *Disk) scheduler(config DiskConfig) {

	nReading := 0 // number jobs reading from disk
	nJobs := 0    // number jobs (goroutines) still running
	finishing := false

	var offset uint64 // estimate of current disk head position

	alljobs := make(jobqueues, PriorityCount)
	//for i:=0; i < int(PriorityCount); i++ {
	//	alljobs = append(alljobs, &jobqueue{})
	//}

	started := false // whether reading has been started yet by disk.Start

	release := func() {
		if !started {
			return
		}
		if d.wait > 0 && alljobs.Len() < d.wait {
			// start signal received but not enough pending reads yet
			return
		}
		d.wait = 0

		if finishing && nJobs == 0 && alljobs.Len() == 0 {
			finishing = false
			d.wg.Done()
		}

		for iq := range alljobs {
			for alljobs[iq].Len() > 0 && nReading < config.Read+config.Window {

				alljobs[iq].lazySort(config.Lazy)

				// find last file that is at-or-ahead of the disk head position
				// note files are sorted in reverse offset order
				if len(alljobs[iq].s) == 0 {
					panic("zero length sorted queue")
				}
				i := len(alljobs[iq].s) - 1
				if config.Lazy >= 0 {
					i = sort.Search(len(alljobs[iq].s), func(i int) bool { return alljobs[iq].s[i].offset < offset }) - 1
				}

				// launch() launches job i from the sorted queue
				launch := func(i int) {
					r := alljobs[iq].Pop(i)
					nReading++
					nJobs++
					// register the new disk offset
					// note: jobs under the ahead/behind window clause don't reset offset
					if nReading <= config.Read {
						offset = r.offset
					}
                                        go r.Do(d)
				}

				// gap returns the seek gap from current head position to file i
				gap := func(i int) int64 {
					return int64(alljobs[iq].s[i].offset) - int64(offset)
				}

				// release best match...
				if i == -1 { // all file are behind current offset
					i = 0
				}

				if gap(i) >= 0 && gap(i) <= config.Ahead {
					// found a job in ahead window; release it
					launch(i)
				} else if i+1 < len(alljobs[iq].s) && gap(i+1) <= 0 && gap(i+1) >= -config.Behind {
					// found a job in behind window; release it
					launch(i + 1)
				} else if gap(i) >= 0 && nReading < config.Read {
					// no jobs in ahead/behind window, but next job is ahead of disk head
					launch(i)
				} else {
					// we've reached the end of the disk; don't release
					// further jobs until all reading jobs have finished,
					// then release most-negative offset
					if nReading > 0 {
						return
					}
					launch(len(alljobs[iq].s) - 1)
				}
			}
		}
	}

	// main scheduler loop:
sched:
	for {
		select {
		case r := <-d.reqch:
			// request to read data from f
			if r == nil {
				// channel closed; we are done!
				break sched
			}

			// append to unsorted reqs:
			alljobs[r.priority].Add(r)

			// launch this (or other pending) job(s) as appropriate
			release()

		case <-d.donech:
			// a job's goroutine has finished reading; launch pending jobs as appropriate
			nJobs--
			release()

		case <-d.readch:
			// a job has finished reading; launch pending jobs as appropriate
			nReading--
			if nJobs < config.Process {
				release()
			}

		case <-d.startch:
			log.Println("Disk start; ssd:", d.ssd)
			started = true
			release()

		case <-d.closech:
			log.Println("Disk closing...")
			finishing = true
			release()
		}
	}

	// all done!
	// debug check that no pending jobs left behind:
	if alljobs.Len() > 0 {
		log.Printf("Unprocessed jobs: %d\n", alljobs.Len())
	}

}

// AddDir checks for the presence of a dir in d.dirs.  If notfound
// then adds it and returns true, else returns false.
func (d *Disk) AddDir(i id, path string) error {
	d.mx.Lock()
	defer d.mx.Unlock()
	match, has_match := d.dirs[i]
	if has_match {
		return fmt.Errorf("Duplicate dir: %s is same as %s", path, match)
	}
	d.dirs[i] = path
	return nil
}

// Job is the interface for the calling routine to process files scheduled for reading.
type Job interface {
	// Go is called by drs as a goroutine.  As soon as it has finished reading file
	// data, it should close the files and send read <- Token{}.  It can then continue
	// on with other tasks (eg processing the data).
	Go(f *File, err error)
}

// Priority can be used to prioritise jobs.  No lower priority jobs will be processed
// until all higher priority jobs are done.
type Priority int

const (
	// High is the highest priority
	High Priority = iota
	Normal
	Low
	PriorityCount // Sentinel
)

// request is used by drs to communicate with the scheduler goroutine
type request struct {
	job       Job
        path      string
	offset    uint64
	priority  Priority
}

// Schedule adds a job to the disk's queue.
func (d *Disk) Schedule(j Job, path string, offset uint64, priority Priority) {
	r := &request{j, path, offset, priority}
	d.reqch <- r
}

//////////////////////////////////////////////////////////////////////
// Copy

// CopyN is similar to io.CopyN except that it closes src and signals
// done as soon as reading has finished.
// It copies n bytes (or until an error) from src to dst.
// It returns the number of bytes copied and the earliest error
// encountered while copying.  On return, written == n if
// only if err == nil.
func CopyN(dst io.Writer, src *File, n int64) (written int64, err error) {

	var werr error                            // last error during writing
	ch := make(chan (Buf), defaultBufPerFile) // TODO: revisit buffer count
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := range ch {
			if werr != nil {
				PutBuf(b)
				continue
			}
			var nw int
			nw, werr = dst.Write(b)

			written += int64(nw)
			if nw != len(b) && werr == nil {
				werr = io.ErrShortWrite
			}
			PutBuf(b)
		}
	}()

	for err == nil && n > 0 {
		b := GetBuf()
		if int64(len(b)) > n {
			b = b[:n]
		}
		nr, er := src.Read(b)
		if nr > 0 {
			b := b[:nr]
			ch <- b
			n -= int64(nr)
		} else {
			PutBuf(b)
		}
		if er == io.EOF {
			break
		}

		err = er
	}
	src.Close()
	close(ch)
	wg.Wait()

	if err == nil {
		err = werr
	}

	return written, err
}

// Copy is similar to io.Copy except that it signals done
// as soon as reading has finished.
// It copies from src to dst until either EOF is reached
// on src or an error occurs.  It returns the number of bytes
// copied and the first error encountered while copying, if any.
//
// A successful Copy returns err == nil, not err == EOF.
// Because Copy is defined to read from src until EOF, it does
// not treat an EOF from Read as an error to be reported.
//
// Copy always uses src.Read() and dst.Write() so as to be able
// to identify the end of reading.
func Copy(dst io.Writer, src *File) (written int64, err error) {
	const maxInt64 int64 = 1<<63 - 1

	return CopyN(dst, src, maxInt64)
}


// Wait waits until there are not unfinished or scheduled jobs.
func (d *Disk) Wait() {
	d.wg.Add(1)
	d.closech <- Token{}
	d.wg.Wait()
}

// Close process all pending requests and then closes the disk scheduler
// and frees buffer memory.
func (d *Disk) Close() {
	d.Wait()
	// close bufpool
	if bufc != nil {
		n, err := ClosePool()
		// TODO: clean up debug logging
		log.Printf("Buffers used: %d", n)
		if err != nil {
			log.Println(err)
		}
	}
}

///////////////////////////////////////////////////////////////
//
// job queues
//

type reqs []*request

// implements sort.Interface for sorting by decreasing Offset.
func (r reqs) Len() int           { return len(r) }
func (r reqs) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r reqs) Less(i, j int) bool { return r[i].offset > r[j].offset }

func (r *request) Less(s *request) bool { return r.offset > s.offset }

// jobqueue is a lazily sorted queue.  Adds are faster than a sorted
// insert, and sorting is fast because it takes advantage of items
// already in the correct order.
type jobqueue struct {
	s reqs // sorted reqs
	u reqs
}

// Add adds a job to the unsorted list
func (j *jobqueue) Add(r *request) {
	j.u = append(j.u, r)
}

// Len returns total jobqueue length
func (j *jobqueue) Len() int {
	return len(j.s) + len(j.u)
}

// Pop pops the ith element from the sorted queue
func (j *jobqueue) Pop(i int) *request {
	r := j.s[i]
	// GC-friendly delete from slice:
	copy(j.s[i:], j.s[i+1:])
	j.s[len(j.s)-1] = nil
	j.s = j.s[:len(j.s)-1]
	return r
}

// LazySort waits until there are enough unsorted jobs relative to sorted jobs,
// then does a sort.  Trigger is when len(unsorted) >= len(sorted) * laziness.
func (j *jobqueue) lazySort(laziness float32) {
	if laziness < 0 {
		if len(j.u) > 0 {
			j.s = append(j.s, j.u...)
			j.u = nil
		}
	} else if len(j.u) >= int(laziness*float32(len(j.s))) {
		j.Sort()
	}
}

// Sort sorts jobs into decreasing disk offset
func (j *jobqueue) Sort() {

	if len(j.u) == 0 {
		return
	}

	sort.Sort(reqs(j.u))

	if len(j.s) == 0 {
		j.s = j.u
		j.u = nil
		return
	}

	// merge the sorted file queues
	l := j.Len()
	merged := make(reqs, 0, l)

	if j.u[0].Less(j.s[0]) {
		j.s, j.u = j.u, j.s
	}

	for {
		if len(j.u) == 0 {
			merged = append(merged, j.s...)
			break
		}
		// pull as many as possible from j.s...
		// find first item in j.s for which j.u[0] is less than j.s[i]
		i := sort.Search(len(j.s), func(i int) bool { return j.u[0].Less(j.s[i]) })
		merged = append(merged, j.s[:i]...)
		j.u, j.s = j.s[i:], j.u
	}
	j.s = merged

}

type jobqueues []jobqueue

func (j *jobqueues) Len() int {
	l := 0
	for _, q := range *j {
		l += q.Len()
	}
	return l
}
