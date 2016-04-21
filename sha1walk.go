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

// sha1walk is a demo main for the drs package.
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
	"sort"
	"github.com/docopt/docopt-go"
	"github.com/SeeSpotRun/drs/drs"
)

//////////////////////////////////////////////////////////////////

// sortRes defines whether to sort results by alphabetical order of path
var sortRes bool

// job implements the drs.Job interface
type job struct {
	path   string
	offset uint64
	hash   string
}

// Go hashes the file contents and adds the job to results[]
func (j *job) Go(f *drs.File, err error) {
	if err != nil {
		log.Println(err)
		return
	}

	h := sha1.New()
	_, err = drs.Copy(h, f)
	// Note: Copy closes f TODO: rename to CopyClose?

	if err != nil {
		log.Printf("Failed hashing %s: %s", j.path, err)
	} else {
		j.hash = fmt.Sprintf("%x", h.Sum(nil))
		if sortRes {
			res = append(res, j)
		} else {
			fmt.Printf("%s:  %s\n", j.hash, j.path)
		}
	}
}

type results []*job
var res results
// implements sort.Interface for sorting by increasing path.
func (r results) Len() int           { return len(r) }
func (r results) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r results) Less(i, j int) bool { return r[i].path < r[j].path }


func main() {

	// parse args:
	usage := `Usage:
    sha1walk -h | --help
    sha1walk [options] [--ssd=path]... <paths>...
Options:
  -h --help     Show this screen
  --version     Show version
  -r --recurse  Recurse paths if they are folders
  --walkfirst   Finish walk before starting hashing
  --hashfirst   Hash files as soon as they are found
  --aggressive  Use more aggressive HDD scheduler settings
  --ssd=path    Identify path as belonging to an SSD
  --sort        Sort results before printing
`
	args, _ := docopt.Parse(usage, os.Args[1:], true, "sums 0.1", false, true)
	fmt.Println(args)

	// sort results before printing?
	sortRes := args["--sort"] == true

	// use more aggressive scheduler for HDD's ?
	if args["--aggressive"] == true {
		drs.HDD = drs.Aggressive
	}

	// do hashing ASAP, during walking or after walking?
	hashPriority := drs.Normal
	if args["--hashfirst"] == true {
		hashPriority = drs.High
	} else if args["--walkfirst"] == true {
		hashPriority = drs.Low
	}

	// configure walk options
	walkopts := &drs.WalkOptions{
		Priority: drs.Normal,
		Errs: make(chan error),
		NoRecurse: args["--recurse"] != true,
	}

	// error reporting during walk:
	go func() {
		for err := range walkopts.Errs {
			log.Printf("Walk error: %s\n", err)
		}
	}()

	// register user-identified ssd's:
	if ssds, ok := args["--ssd"].([]string); ok {
		err := drs.RegisterSSDs(ssds)
		if err != nil {
			fmt.Println(err)
		}
	}

	// walk paths:
	paths, ok := args["<paths>"].([]string)
	if !ok {
		fmt.Println("Error: no paths to hash")
		return
	}

	for p := range drs.Walk(paths, walkopts) {
		j := &job{path: p.Name, offset: p.Offset}
		// shedule for hashing
		p.Disk.Schedule(j, j.path, j.offset, hashPriority)
	}

	// wait for all jobs to finish
	drs.WaitDisks()

	// print results
	if sortRes {
		sort.Sort(results(res))
		for _, j := range res {
			fmt.Printf("%s:  %s\n", j.hash, j.path)
		}
	}

}
