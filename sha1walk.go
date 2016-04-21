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

// sha1 is a demo main for the drs package.
package main

/*
 * TODO:
 * [ ] write sha1_test
 * [ ] benchmarking & profiling
 */

import (
	"crypto/sha1"
	"fmt"
	"log"
	"os"
	"strings"
	"sort"
	"github.com/docopt/docopt-go"
	"github.com/SeeSpotRun/drs/drs"
)

//////////////////////////////////////////////////////////////////

type results []*job
var res results
// implements sort.Interface for sorting by increasing path.
func (r results) Len() int           { return len(r) }
func (r results) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r results) Less(i, j int) bool { return r[i].path < r[j].path }

// job implements the drs.Job interface
type job struct {
	path   string
	offset uint64
	hash   string
}

// Go opens the file, reads its contents, signals done, then finishes hashing the
// contents and prints the results
func (j *job) Go(f *drs.File, err error) {
	if err != nil {
		log.Println(err)
		return
	}

	h := sha1.New()
	_, err = drs.Copy(h, f)
	// Note: Copy closes f TODO: rename to CopyClose?

	if err != nil {
		if !strings.HasSuffix(err.Error(), "is a directory") {
			log.Printf("Failed hashing %s: %s", j.path, err)
		}
	} else {
		j.hash = fmt.Sprintf("%x", h.Sum(nil))
		res = append(res, j)
	}
}

func main() {
	// parse args:
	usage := `Usage:
    sha1walk -h | --help
    sha1walk [options] <path>...
Options:
  -h --help     Show this screen
  --version     Show version
  -r --recurse  Recurse paths if they are folders
  --walkfirst   Finish walk before starting hashing
  --hashfirst   Hash files as soon as they are found
  --aggressive  Use more aggressive HDD scheduler settings
  --sort        Sort results
`

	args, _ := docopt.Parse(usage, os.Args[1:], true, "sums 0.1", false, true)

	if args["--aggressive"] == true {
		drs.HDD = drs.Aggressive
	}
	hashPriority := drs.Normal
	if args["--hashfirst"] == true {
		hashPriority = drs.High
	} else if args["--walkfirst"] == true {
		hashPriority = drs.Low
	}

	walkopts := &drs.WalkOptions{Priority: drs.Normal, Errs: make(chan error)}
	walkopts.NoRecurse = args["--recurse"] != true
	paths := args["<path>"].([]string)
	// error reporting during walk:
	go func() {
		for err := range walkopts.Errs {
			log.Printf("Walk error: %s\n", err)
		}
	}()

	for p := range drs.Walk(paths, walkopts) {
		j := &job{path: p.Name, offset: p.Offset}
		// TODO
		p.Disk.Schedule(j, j.path, j.offset, hashPriority)
	}

	drs.CloseDisks()

	if args["--sort"] == true {
		sort.Sort(results(res))
	}

	for _, j := range res {
		fmt.Printf("%s:  %s\n", j.hash, j.path)
	}

}
