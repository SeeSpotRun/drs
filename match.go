/**
*  This file is part of drs.
*
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
** Copyright 2016 the drs Authors:
 *
 *  - Daniel <SeeSpotRun> T.   2016-2016 (https://github.com/SeeSpotRun)
 *
** Hosted on https://github.com/SeeSpotRun/go-disk-utils
*
**/

// match is a demo main for the drs package.  It finds duplicate files based on checksums.  It is
// more or less a golang partial port of rmlint (https://github.com/sahib/rmlint)
package main

/*
 * TODO:
 * [ ] write sha1_test
 * [ ] benchmarking & profiling
 */

import (
	"crypto"
	_ "crypto/sha1"
	_ "crypto/sha256"
	_ "crypto/sha512"
	_ "crypto/md5"
	"hash"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"runtime"
	"strings"
	"github.com/docopt/docopt-go"
	"github.com/SeeSpotRun/coerce"
	"github.com/SeeSpotRun/drs/drs"
)

// tuning parameters
const (
	bufSize = 32 * 1024        // read buffer size
	firstN = int64(bufSize)    // how many bytes in first hash increment
	mult   = int64(7)          // acceleration factor
	maxN   = int64(1) << 27    // maximum increment size = 128M
	defaultHash = crypto.SHA1  // note: if change this then also change in usage string
)

//////////////////////////////////////////////////////////////////

// File implements the drs.Job interface
type File struct {
	path     string
	size     uint64
	hashed   uint64
	offset   uint64
	hash     hash.Hash
	disk     *drs.Disk
	group    *Group
	err      error
}

func (f *File) Schedule() {
	f.disk.Schedule(f, f.path, f.offset, drs.Normal)
}


// Go hashes the file contents and adds the job to results[]
func (f *File) Go(fd *drs.File, err error) {
	if err != nil {
		f.err = err
	} else {
		// how many bytes to hash?
		n := firstN
		if f.hashed > 0 {
			fd.Seek(int64(f.hashed), 0)
			n = int64(f.hashed) * mult
			if n > maxN {
				n = maxN
			}
		}

		if uint64(n) + f.hashed > f.size {
			n = int64(f.size - f.hashed)
		}

		// hash file
		_, f.err = drs.CopyN(f.hash, fd, n)
		// Note: Copy closes f TODO: rename to CopyClose?

		if f.err == nil {
			f.hashed += uint64(n)
			//copy(f.hash[:], h.Sum(nil))
		}
	}
	f.group.grouper.filech <- f
}

func (f *File) isFinal() bool {
	return f.size == f.hashed
}

// Group status constants
const (
	Dormant = iota
	Hashing
	ReadError
	Duplicates
)

type Group struct {
	status   int
	refs     int      // reference count (1 for incoming channel and 1 for each pending hash)
	children map[string]*Group
	files    []*File
	reportch chan <- *Group
	grouper  *Grouper
}

func NewGroup(f *File, reportch chan <- *Group, grouper *Grouper) *Group {
	g := &Group{
		refs: 1,
		children: make(map[string]*Group, 1),
		reportch: reportch,
		grouper:  grouper,
	}
	g.add(f)
	return g
}

func (g *Group) qualifies() bool {
	return len(g.files) > 1
}

func (g *Group) isFinal() bool {
	return g.files[0].isFinal()
}
type Grouper struct {
	filech chan *File
}

func (g *Group) add(f *File) {
	g.files = append(g.files, f)
	f.group = g
	if g.status == Duplicates {
		return
	}
	if g.status == Hashing {
		g.refs++
		f.Schedule()
		return
	}
	if !g.qualifies() {
		return
	}
	if g.isFinal() {
		g.status = Duplicates
		return
	}
	g.status = Hashing
	for _, gf := range g.files {
		g.refs++
		gf.Schedule() // TODO - custom priority?
	}
}

// unref decreases the reference count for Group; if this reaches zero then the Group unrefs
// children and sends itself, if appropriate, to the Reporter via reporterc
func (g *Group) unref() {
	if g == nil {
		return
	}
	g.refs--
	if g.refs < 0 {
		panic("Group.unref yields negative reference count")
	}

	if g.refs > 0 {
		return
	}

	if g.status == Hashing {
		// process children
		for _, child := range g.children {
			child.unref()
		}
		g.children = nil
		return
	}
	// Group which didn't launch; this will be either single files with
	// no duplicates, or groups of fully-hashed duplicates.  Reporter() will
	// decide what to do in each case, then will dispose of the files
	g.reportch <- g
}


func NewGrouper() *Grouper {
	return &Grouper { make(chan *File, 10) }
}

// mapable encodes []byte b to a uniquely mapable string.
// Since strings can't contain 0 values it maps byte values as follows:
// 0: {1, 1}
// 1: {1, 2}
// Encoding efficiency should average 99.2% (vs hex encoding 50%)
func mapable(b[]byte) string {
	j := 0 // start of current subslice
	result := ""
	for i := 0; i < len(b); i++ {
		if b[i] <= 1 {
			result = result + string(b[j:i]) + string(1) + string(b[i] + 1)
			j = i + 1
		}
	}
	return result + string(b[j:])
}

func (gr *Grouper) group() {
	for f := range gr.filech {
		g := f.group
		if f.err != nil {
			g.reportch <- &Group{files: []*File {f}, status: ReadError  }
		} else {
			h := mapable( f.hash.Sum(nil) ) // []byte is not mapable but string is
			if fmt.Sprintf("%x", h) != fmt.Sprintf("%x", f.hash.Sum(nil)) {
				fmt.Printf("%x\n%x\n", f.hash.Sum(nil), h)
			}
			c, ok := g.children[h]
			if !ok {
				// create new group
				g.children[h] = NewGroup(f, g.reportch, g.grouper)
			} else {
				c.add(f)
			}
		}
		g.unref()
	}
}

type Reporter struct {
	reportch   chan *Group
	wg         sync.WaitGroup
	dupes      int
	originals  int
	wasted     uint64
}

type reporter func(r *Reporter, g *Group)

func report_unique(r *Reporter, g *Group) { }

func report_hashing(r *Reporter, g *Group) {
	panic("Report on hashing group")
}

func report_error(r *Reporter, g *Group) {
	if len(g.files) != 1 {
		panic("Report read error with group len > 1")
	}
	fmt.Printf("Read Error: %s\n", g.files[0].err)
}

func report_dupes(r *Reporter, g *Group) {
	fmt.Printf("Duplicates (size %d, hash %x):\n", g.files[0].size, g.files[0].hash.Sum(nil))
	for _, f := range g.files {
		fmt.Println(f.path)
		if f.err != nil {
			panic("Error on duplicate: " + f.err.Error())
		}
	}
	r.originals ++
	r.dupes += len(g.files) - 1
	r.wasted += g.files[0].size * uint64 (len(g.files) - 1)
}

func NewReporter() *Reporter {

	r := &Reporter {reportch: make(chan *Group, 10) }

	go func() {
		var reporters = []reporter {
			report_unique,   // Dormant
			report_hashing,  // Hashing
			report_error,    // ReadError
			report_dupes,    // Duplicates
		}
		for g := range r.reportch {
			reporters[g.status](r, g)
			r.wg.Add(-len(g.files))
			g.files = nil
		}
	}()

	return r

}

// bysize implements sort.Interface()
type bysize []*File
// implements sort.Interface for sorting by increasing path.
func (b bysize) Len() int           { return len(b) }
func (b bysize) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b bysize) Less(i, j int) bool { return b[i].size < b[j].size }


func main() {

	// parse args:
	usage := `Usage:
    match -h | --help
    match [options] [--ssd=path]... [<paths>...]
Options:
  -h --help              Show this screen
rmlint-like options:
  --maxDepth=N, -d N     Specify max traversal depth [default: -1]
  --algorithm=A, -a A    Choose hash algorithm [default: SHA1]
  --hidden, -r           Find hidden files and files in hidden folders
  --followlinks, -f      Follow symlinks
  --seelinks             Treat symlinks as little files and look for duplicate symlink
  --xdev, -x             Do not cross mounpoints
  --version              Show version
  --readbuf=N            Use up to N bytes of read buffer [default: 32M]
variations on rmlint options:
  --minsize=m,  -s m     Specify minimum file size [default: 1]
  --maxsize=M            Specify maximum file size [default: -1]
non-rmlint options:
  --ssd=path             Identify path as belonging to an SSD
tuning options:
  --aggressive           Use more aggressive HDD scheduler
  --groupers=N           Use N grouper goroutines [default: 1]
`

	type options struct {
		maxDepth	int
		minsize		int64
		maxsize		int64
		algorithm	string
		hidden		bool
		followlinks	bool
		xdev		bool
		seelinks	bool
		readbuf         int64
		aggressive	bool
		groupers        int
		ssd		[]string
		paths		[]string
	}


	args, _ := docopt.Parse(usage, os.Args[1:], true, "sums 0.1", false, true)

	var opts options
	err := coerce.Struct(&opts, args, "--%s", "-%s", "<%s>")
	if err != nil {
		fmt.Println(err)
		fmt.Println("args:", args)
		fmt.Println(usage)
		return
	}

	hashtype := defaultHash
	// hash type?
	switch strings.ToUpper(opts.algorithm) {
	case "SHA1":
	case "SHA256":
		hashtype = crypto.SHA256
	case "SHA512":
		hashtype = crypto.SHA512
	case "MD5":
		hashtype = crypto.MD5
	default:
		fmt.Println("Error: unrecognized hash algorithm:", opts.algorithm)
		return
	}

	// use more aggressive scheduler for HDD's ?
	if opts.aggressive {
		drs.HDD = drs.Aggressive
	}

	drs.InitPool(bufSize, int( 1 + (opts.readbuf - 1) / bufSize ) )


	// configure walk options
	walkopts := &drs.WalkOptions{
		Priority: drs.Normal,
		Errs: make(chan error),
		MaxDepth: opts.maxDepth + 1,
                HiddenFiles: opts.hidden,
                HiddenDirs: opts.hidden,
		SeeLinks: opts.seelinks || opts.followlinks,
                FollowLinks: opts.followlinks,
		OneDevice: opts.xdev,
	}

	// error reporting during walk:
	go func() {
		for err := range walkopts.Errs {
			log.Printf("Walk error: %s\n", err)
		}
	}()

	// register user-identified ssd's:
	err = drs.RegisterSSDs(opts.ssd)
	if err != nil {
		fmt.Println("Error registering ssd's: %s", err)
	}

	opts.paths = append(opts.paths, opts.ssd...)
	if len(opts.paths) == 0 {
		opts.paths = append(opts.paths, ".")
	}

	if len(opts.paths) == 0 {
		fmt.Println("Error: no paths to hash")
		return
	}
	// walk paths:
	var files []*File
	for p := range drs.Walk(opts.paths, walkopts) {
		if opts.minsize > 0 && p.Info.Size() < opts.minsize {
			continue
		}
		if opts.maxsize >= 0 && p.Info.Size() > opts.maxsize {
			continue
		}
		files = append(files,
			&File{
				path:   p.Name,
				offset: p.Offset,
				size:   uint64(p.Info.Size()),
				disk:   p.Disk,
				hash:   hashtype.New(),
			},
		)
	}

	sort.Sort(bysize(files))

	groupers := make([]*Grouper, opts.groupers)
	for i:=0; i<opts.groupers; i++ {
		groupers[i] = NewGrouper()
	}
	reporter := NewReporter()
	reporter.wg.Add(len(files))

	//drs.Pause()
	var g *Group
	for i, f := range(files) {
		if g == nil || f.size != g.files[0].size {
			// file doesn't fit current group
			g.unref()
			g = NewGroup(f, reporter.reportch, groupers[i % opts.groupers])
		} else {
			g.add(f)
		}
	}
	// unref final group
	g.unref()

	runtime.GC()

	for _, g := range groupers {
		go g.group()
	}

	//drs.Resume()

	//go grouper.group()

	// wait for all jobs to finish
	drs.Wait()

	// wait for printout to finish
	reporter.wg.Wait()
	fmt.Printf("Total %d duplicates of %d originals; total wasted bytes: %.2fGB\n", reporter.dupes, reporter.originals, float64(reporter.wasted) / 1024 / 1024 / 1024)

	// TODO: tidy up
}

