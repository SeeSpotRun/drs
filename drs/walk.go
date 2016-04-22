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
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the
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

// file walk.go provides filesystem walking using the drs disk scheduler

package drs

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"fmt"
)

const winLongPathLimit = 259
const winLongPathHack = "\\\\?\\" // workaround prefix for windows 260-character path limit

// WalkOptions provides convenient commonly used filters for file walks.  The struct is designed
// so that zero-value defaults gives a fairly typical walk configuration
type WalkOptions struct {
	SeeRootLinks bool  // if false, if root paths are symlinks they will be ignored
	SeeLinks     bool  // if false, symlinks will be ignored (except root paths)
	FollowLinks  bool  // if false, walk returns symlinks as symlinks; if true it returns their targets
	SeeDotFiles  bool  // if false, ignores "." and ".." entries // TODO:
	HiddenDirs   bool  // if false, won't descend into folders starting with '.'
	HiddenFiles  bool  // if false, won't return files starting with '.'
	ReturnDirs   bool  // if false, doesn't return dir paths
	NoRecurse    bool  // if false, walks passed root dirs recursively
	OneDevice    bool  // if false, walk will cross filesystem boundaries TODO
	MaxDepth     int   // if recursing dirs, limit depth (1 = files in root dirs; 0 = no limit).
	Priority     Priority	// drs schedule priority for walk takss
	Errs	     chan error // optional channel to return walk errors; if nil then ignores errors
	results	     chan<- *Path  // channel to which to send results
	wg	     sync.WaitGroup // waitgroup to signal end of walks
}


// id is a unique identifier for a file
// TODO: windows flavour
type id struct {
	Dev uint64
	Ino uint64
}

// Path implements the drs.Job interface
type Path struct {
	Name   string
	Offset uint64
	Depth  int
	Info   os.FileInfo
	dev    uint64
	Disk   *Disk
	opts   *WalkOptions
	parent *Path
}

// process processes a path, sending results and scheduling recursion if appropriate
func (p *Path) process(isHidden bool) {
	defer p.opts.wg.Done()

	var err error
	p.Info, err = os.Lstat(p.Name)
	if err != nil {
		p.Report(err)
		return
	}

	if p.Info.Mode() & os.ModeSymlink != 0 {
		if !p.opts.SeeLinks {
		return
		}
		if p.opts.FollowLinks {
			p.Name, err = filepath.EvalSymlinks(unfixpath(p.Name))
			if err != nil {
				fmt.Println("Symlink error")
				p.Report(err)
				return
			}
			p.Name, err = fixpath(p.Name)
			if err != nil {
				fmt.Println("Symlink fixpath error")
				p.Report(err)
				return
			}
			p.Disk = nil
			// TODO: test for encounter of file already walked?
		}
	}

	if p.Disk == nil {
		id, _ := getID(p.Name, p.Info)
		p.Disk = GetDisk(id.Dev, false /*TODO: test for SSD*/ )
	}

	if !p.Disk.ssd {
		p.Offset, _, _, _ = OffsetOf(p.Name, 0, os.SEEK_SET)
	}

	if p.Info.IsDir() {
		if !p.opts.HiddenDirs && isHidden {
			// skip hidden dir
			return
		}

		// map inode to prevent recursion and path doubles
		id, err := getID(p.Name, p.Info)
		if err != nil {
			p.Report(fmt.Errorf("%s: %s", err, p.Name))
			return
		}

		if p.parent != nil && p.parent.dev != id.Dev {
			if p.opts.OneDevice {
				p.Report(fmt.Errorf("Not recursing into %s because it is different filesystem", p.Name))
				return
			}
			p.Disk = GetDisk(id.Dev, false /*TODO: test for SSD*/ )
		}

		// inode recursion check:
		err = p.Disk.AddDir(id, p.Name)
		if err != nil {
			p.Report(err)
			return
		}
		 if p.opts.ReturnDirs {
			p.Send()
		}
		if !p.opts.NoRecurse && ( p.opts.MaxDepth <= 0 || p.Depth < p.opts.MaxDepth ){
			p.opts.wg.Add(1)
			// schedule path.Go() to recurse dir
			p.Disk.Schedule(p, p.Name, p.Offset, Normal)
		}
		return
	}

	if !p.opts.HiddenFiles && isHidden {
		// skip hidden file
		return
	}
	p.Send()

}

// Go recurses into a directory, sending any files found and recursing any directories
func (p *Path) Go(dir *File, err error) {
	defer p.opts.wg.Done()

	if err != nil {
		p.Report(err)
		return
	}
	names, err := dir.Readdirnames(-1)
	dir.Close()
	if err != nil {
		p.Report(err)
		return
	}

	// recurse into next level:
	// TODO: depth check
	for _, name := range names {
		// rule out any combinations which don't requre a stat() call:
		if !p.opts.HiddenDirs && !p.opts.HiddenFiles && strings.HasPrefix(name, ".") {
			continue
		}

		filename := filepath.Join(p.Name, name)
		// TODO: check if not in roots
		path := &Path{
			Name:	filename,
			Depth:	p.Depth + 1,
			Disk:	p.Disk, // note: may get updated by p.process()
			opts:	p.opts,
			parent: p,
		}
		p.opts.wg.Add(1)
		go path.process(strings.HasPrefix(name, "."))
	}
}

// Report sends erros to error channel
func (p *Path) Report(e error) {
	if p.opts.Errs != nil {
		p.opts.Errs <- e
	}
}


// Send sends path to results channel
func (p *Path) Send() {
	if p.opts.results != nil {
		p.opts.results <- p
	}
}

// Walk returns a path channel and walks (concurrently) all paths under
// each root folder in []roots, sending all regular files encountered to the
// channel, which is closed once all walks have completed.
// Walking the same file or folder twice is avoided, so for example
//  ch := Walk(e, {"/foo", "/foo/bar"}, options, disk)
// will only walk the files in /foo/bar once.
// Errors are returned via errc (if provided).
// TODO: use filepath/Walk compatible callback instead of errc (need to make threadsafe)
func Walk( roots []string, opts *WalkOptions) <-chan *Path {

	// canonicalise root dirs, removing duplicate paths
	rmap := make(map[string]bool)
	for _, root := range roots {
		r, err := fixpath(filepath.Clean(root))
		if err != nil {
			opts.Errs <- err
		}
		if _, ok := rmap[r]; !ok {
			// no match; add root to set
			rmap[r] = true
		}
	}

	// create result channel
	results := make(chan *Path)
	opts.results = results

	for root := range rmap {

		path := &Path{
			Name:  root,
			Depth: 0,
			opts:  opts,
		}
		opts.wg.Add(1)
		go path.process(false)
	}

	go func() {
		// wait until all roots have been walked, then close channels
		opts.wg.Wait()
		close(opts.results)
	}()

	return results
}

func RegisterSSDs(roots []string) error {
	for _, r:= range roots {
		info, err := os.Lstat(r)
		if err != nil {
			return err
		}
		id, _ := getID(r, info)
		GetDisk( id.Dev, true )
	}
	return nil
}

func fixpath(p string) (string, error) {
	var err error
	// clean path
	p, err = filepath.Abs(filepath.Clean(p))
	// Workaround for archaic Windows path length limit
	if runtime.GOOS == "windows" && !strings.HasPrefix(p, winLongPathHack) {
		p = winLongPathHack + p
	}
	return p, err
}

func unfixpath(p string) string {
	if runtime.GOOS == "windows" {
		return strings.TrimPrefix(p, winLongPathHack)
	}
	return p
}
