# drs
coming soon... `drs` is a golang package to speed up reading from hard disk drives

![drs](https://github.com/SeeSpotRun/drs/blob/master/doc/DiskReadScheduler.png)

No more disk thrashing when reading large numbers of files from your HDD.
No more "too many open files" when reading from an SSD.
`drs` is a hdd-aware disk read scheduler.  Read jobs can be added batch-wise or
concurrently; the scheduler tries to minimise seek times and disk thrash by
processing the jobs according to physical location of the files on the disk.

## Installation
```
go get github.com/SeeSpotRun/drs
```

## API
See [godoc](https://godoc.org/github.com/SeeSpotRun/drs)

## Example

```
TODO
```

[![Go Report Card](https://goreportcard.com/badge/github.com/SeeSpotRun/drs)](https://goreportcard.com/report/github.com/SeeSpotRun/drs)
