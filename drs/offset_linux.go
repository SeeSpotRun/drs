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

package drs

import (
	"github.com/SeeSpotRun/go-fibmap" // forked from https://github.com/frostschutz/go-fibmap
	// TODO: pull request to merge changes
	"fmt"
	"os"
	"syscall"
)

// offsetof returns the physical offset (relative to disk start) of
// the data at the specified absolute position in an open file
func offsetof(f *os.File, logical uint64) (uint64, error) {
	extents, errno := fibmap.NewFibmapFile(f).FiemapAt(1, logical)
	if errno == 0 {
		if len(extents) == 0 {
			// there is no data for this range of the file - it's a hole?
			return 0, nil
		}
		// get result from first extent with adjustment for logical position relative to start of extent
		return extents[0].Physical + logical - extents[0].Logical, nil
	}
	return 0, errno // converts errno to go err
}

// not used in linux
func bps(path string) uint64 {
	return 1
}

func getID(path string, info os.FileInfo) (id, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return id{}, fmt.Errorf("Can't get file id for %s", path)
	}
	return id{stat.Dev, stat.Ino}, nil
}
