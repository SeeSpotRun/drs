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
*  along with drs.  If not, see <http://www.gnu.org/licenses/>.
*
** Authors:
 *
 *  - Daniel <SeeSpotRun> T.   2016-2016 (https://github.com/SeeSpotRun)
 *
** Hosted on https://github.com/SeeSpotRun/drs
*
**/

// sha1 is a simple demo main for the drs package.
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
)

//////////////////////////////////////////////////////////////////

// job implements the drs.Job interface and contains userdata for
// carrying out the job
type job struct {
	path   string
	offset uint64
	ssd    bool
}

// Go opens the file, reads its contents, signals done, then finishes hashing the
// contents and prints the results
func (j *job) Go(read chan<- drs.Token) {
	f, err := os.Open(j.path)
	if err != nil {
		if read != nil {
			read <- drs.Token{}
		}
		log.Println(err)
		return
	}

	h := sha1.New()
	_, err = drs.Copy(h, f, read)
	// Note: Copy above takes care of signalling chan read and of closing f

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

	// create disk
	disk := drs.NewDisk(diskconfig)

	// load it up with jobs
	for _, p := range os.Args[2:] {
		j := &job{path: p, ssd: ssd}
		if !ssd {
			// lookup file offset
			j.offset, _, _, _ = drs.OffsetOf(p, 0, os.SEEK_SET)
		}
		disk.Schedule(j, j.offset, drs.Normal)
	}

	// go!
	disk.Start(0)
	disk.Close()

}
