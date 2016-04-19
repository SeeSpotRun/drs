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
	"github.com/SeeSpotRun/drs/drs"
	"log"
	"os"
	"strings"
	"time"
)

//////////////////////////////////////////////////////////////////

// job implements the drs.Job interface
type job struct {
	path   string
	offset uint64
	ssd    bool
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
	} else if j.ssd {
		fmt.Printf("%x %s\n", h.Sum(nil), j.path)
	} else {
		fmt.Printf("%x (%12d) %s\n", h.Sum(nil), j.offset, j.path)
	}
}

func main() {
	// start timer...
	t1 := time.Now()

	// parse args:
	usage := "Usage: sha1 --[hdd|ssd|aggressive] <files>..."

	if len(os.Args) < 3 {
		fmt.Println(usage)
		return
	}

	var diskconfig drs.DiskConfig
	ssd := false
	switch os.Args[1] {
	case "--hdd":
		diskconfig = drs.HDD
	case "--aggressive":
		diskconfig = drs.Aggressive
	case "--ssd":
		diskconfig = drs.SSD
		ssd = true
	default:
		fmt.Println(usage)
		return
	}

	// set up disk
	disk := drs.NewDisk(diskconfig)
	disk.Start(0)

	// error reporting during walk:
	errc := make(chan error)
	go func() {
		for err := range errc {
			log.Printf("Walk error: %s\n", err)
		}
	}()

	walkopts := &drs.WalkOptions{Errs: errc}

	for p := range drs.Walk(os.Args[2:], walkopts, disk) {
		j := &job{path: p.Name, ssd: ssd, offset: p.Offset}
		disk.Schedule(j, j.path, j.offset, drs.Low)
	}

	disk.Close()

	log.Printf("Total time %d ms\n", time.Now().Sub(t1)/time.Millisecond)
}
