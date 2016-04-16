/**
*  This file is part of go-disk-utils.
*
*  go-disk-utils is free software: you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation, either version 3 of the License, or
*  (at your option) any later version.
*
*  go-disk-utils are distributed in the hope that it will be useful,
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
** Hosted on https://github.com/SeeSpotRun/go-disk-utils
*
**/

// sum is a demo main for the hddreader package.
// Various optimisations can be tuned or turned off.
package main

/*
 * TODO:
 * [ ] write sum_test
 * [ ] benchmarking & profiling
 * [x] switch from flag to docopt :-)
 * [x] copyright etc
 * [x] support multiple hashes
 * [ ] reflect settings in run summary
 */

import (
	"crypto"
	_ "crypto/md5"
	_ "crypto/sha1"
	_ "crypto/sha256"
	_ "crypto/sha512"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
	//
	"github.com/docopt/docopt-go"
	// local packages:
	"github.com/SeeSpotRun/coerce"
	"github.com/SeeSpotRun/drs/drs"
	"github.com/SeeSpotRun/go-disk-utils/walk"
)

// map hashname to crypto.Hash
var hashtypes []crypto.Hash

const defaulthash = crypto.SHA1

var hashnames = []string{
	crypto.MD4:        "MD4",        // import golang.org/x/crypto/md4crypto.MD4
	crypto.MD5:        "MD5",        // import crypto/md5ypto.MD5
	crypto.SHA1:       "SHA1",       // import crypto/sha1ypto.SHA1
	crypto.SHA224:     "SHA224",     // import crypto/sha256
	crypto.SHA256:     "SHA256",     // import crypto/sha256
	crypto.SHA384:     "SHA384",     // import crypto/sha512
	crypto.SHA512:     "SHA512",     // import crypto/sha512
	crypto.MD5SHA1:    "MD5SHA1",    // no implementation; MD5+SHA1 used for TLS RSA
	crypto.RIPEMD160:  "RIPEMD160",  // import golang.org/x/crypto/ripemd160
	crypto.SHA3_224:   "SHA3_224",   // import golang.org/x/crypto/sha3
	crypto.SHA3_256:   "SHA3_256",   // import golang.org/x/crypto/sha3
	crypto.SHA3_384:   "SHA3_384",   // import golang.org/x/crypto/sha3
	crypto.SHA3_512:   "SHA3_512",   // import golang.org/x/crypto/sha3
	crypto.SHA512_224: "SHA512_224", // import crypto/sha512
	crypto.SHA512_256: "SHA512_256", // import crypto/sha512
}

type options struct {
	cpuprofile string
	procs      int
	read       int
	process    int
	window     int
	ahead      int64
	behind     int64
	buffer     int
	path       []string
	maxsize    int64
	minsize    int64
	whilewalk  bool
	hidden     bool
	recurse    bool
	limit      int64
	ls         bool
}

// (cough) globals

var opts options
var wg sync.WaitGroup

//////////////////////////////////////////////////////////////////

// job implements the drs.Job interface
type job struct {
	path   string
	offset uint64
}

// Go opens the file, reads its contents, signals done, hashes the
// contents and prints the results
func (j *job) Go(read chan<- drs.Token) {
	defer wg.Done()
	f, err := os.Open(j.path)
	if err != nil {
		read <- drs.Token{}
		log.Println(err)
		return
	}

	// build a multiwriter to hash the file contents
	w := make([]io.Writer, 0, len(hashtypes))
	for _, t := range hashtypes {
		w = append(w, t.New())
	}
	m := io.MultiWriter(w...)

	if opts.limit > 0 {
		_, err = drs.CopyN(m, f, opts.limit, read)
	} else {
		_, err = drs.Copy(m, f, read)
	}
	// Note: Copy[N] above take care of signalling chan read and of
	// closing f

	if err != nil {
		log.Printf("Failed hashing %s: %s", j.path, err)
	} else {
		// build a single line for output to avoid stdout race word salad
		var results string
		for _, s := range w {
			sum, ok := s.(hash.Hash)
			if !ok {
				panic("Can't cast io.Writer back to hash.Hash")
			}
			results = results + fmt.Sprintf("%x : ", sum.Sum(nil))
		}
		fmt.Printf("%s %12d %s\n", results, j.offset, j.path)
	}
}

func main() {

	// start timer...
	t1 := time.Now()

	usage := `Usage:
    sum -h | --help
    sum [options] [hashtypes] <path>...
`
	useOptions := `
Options:
  -h --help      Show this screen
  --version      Show version
  --limit=N      Only hash the first N bytes of each file
Walk options:
  -r --recurse   Recurse paths if they are folders
  --hidden       Include hidden files and folders
  --whilewalk    Don't wait for folder walk to finish before starting hashing
  --minsize=N    Ignore files smaller than N bytes [default: 1]
  --maxsize=N    Ignore files larger than N bytes [default: -1]
  --ls           Just list the files, don't hash
System options:
  -p --procs=N   Limit number of simultaneous processes (defaults to NumCPU)
  --cpuprofile=<file>  Write cpu profile to file
  --open=N       Limit number of open file handles to N [default: 10]
HDD options:
  --read=N       Limit number of files reading simultaneously [default: 1]
  --process=N    Limit number of files processing [default: 0]
  --ahead=<kB>   Files within this many kB ahead of disk head ignore 'read' limit [default: 1024]
  --behind=<kB>  Files within this many kB behind disk head ignore 'read' limit [default: 0]
  --window=N     Limit number of ahead/behind file exceptions [default: 5]
  --handles=N    Limit number of open file handles to N [default: 100]
  --buffer=<kB>  Use a bufferpool to buffer disk reads
`
	useHash := `Hashtype options:
`
	// add all available hash types to usage string
	useHash = useHash + fmt.Sprintf("  --%-10s  Calculate %s hash (default)\n", hashnames[defaulthash], hashnames[defaulthash])
	for i, n := range hashnames {
		if i != int(defaulthash) && crypto.Hash(i).Available() {
			useHash = useHash + fmt.Sprintf("  --%-10s  Calculate %s hash\n", n, n)
		}
	}

	// parse args
	args, err := docopt.Parse(usage+useOptions+useHash, os.Args[1:], false, "sums 0.1", false, false)
	if err != nil || args["--help"] == true {
		fmt.Println(useOptions + useHash)
		return
	}
	if len(args) == 0 {
		return
	} // workaround for docopt not parsing other args if --version passed

	err = coerce.Struct(&opts, args, "--%s", "-%s", "<%s>")
	if err != nil {
		log.Println(err)
		return
	}

	if opts.ls {
		opts.whilewalk = true
	}

	if opts.cpuprofile != "" {
		f, err := os.Create(opts.cpuprofile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// set hash type[s]
	for i := range hashnames {
		if y, _ := args["--"+hashnames[i]].(bool); y {
			hashtypes = append(hashtypes, crypto.Hash(i))
			fmt.Printf(fmt.Sprintf("%%%ds : ", crypto.Hash(i).Size()*2), hashnames[i])
		}
	}
	if len(hashtypes) == 0 {
		hashtypes = append(hashtypes, defaulthash)
		fmt.Printf(fmt.Sprintf("%%%ds :", defaulthash.Size()*2), hashnames[defaulthash])
	}
	fmt.Println()

	// set number of processes

	if opts.procs > 0 {
		runtime.GOMAXPROCS(opts.procs)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	disk := drs.NewDisk(opts.read, opts.window, opts.ahead*1024, opts.behind*1024, opts.process, opts.buffer)

	// set up for walk...
	walkopts := walk.Defaults
	if !opts.recurse {
		walkopts |= walk.NoRecurse
	}
	if opts.hidden {
		walkopts |= walk.HiddenDirs | walk.HiddenFiles
	}

	// error reporting during walk:
	errc := make(chan error)
	go func() {
		for err := range errc {
			log.Printf("Walk error: %s\n", err)
		}
	}()

	// do the actual walk
	for f := range walk.FileCh(nil, errc, opts.path, walkopts) {
		// filter based on size
		if opts.maxsize >= 0 && f.Info.Size() > opts.maxsize {
			continue
		}
		if f.Info.Size() < opts.minsize {
			continue
		}

		if opts.ls {
			fmt.Println(f.Path)
		} else {
			j := &job{path: f.Path}
			j.offset, _, _, _ = drs.OffsetOf(j.path, 0, os.SEEK_SET)
			wg.Add(1)
			disk.Schedule(j, j.offset, drs.Normal)
		}
	}

	close(errc)
	log.Println("Walk done")

	disk.Start(0)
	wg.Wait()
	disk.Close()

	log.Printf("Total time %d ms\n", time.Now().Sub(t1)/time.Millisecond)

}
