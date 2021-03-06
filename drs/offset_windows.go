// offset returns information about the physical location of a file on a disk.	It is part of the hddreader package.

package drs

import (
	//"log"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
	//"fmt"
	//"reflect"
)

const (
	fsctl_get_retrieval_pointers   uint32 = 0x00090073 //https://msdn.microsoft.com/en-us/library/cc246805.aspx
	extentsize                            = 16         // sizeof(Extent)
	retrieval_pointers_buffer_size        = 12         // sizeof(Retrieval_pointers_buffer)
	starting_vcn_input_buffer_size uint32 = 8          // sizeof(LARGE_INTEGER)
	failbps                               = 4096       // guess value to use if syscall fails
)

// disks maps volume names to bytes-per-sector
var bpsmap map[string]uint64

// large_integer represents a 64-bit signed integer in windows
// Surprisingly this is not in https://golang.org/src/syscall/syscall_windows.go
type large_integer struct {
	upper int32
	lower uint32
}

func (i *large_integer) get() int64 {
	return int64(i.upper)<<32 | int64(i.lower)
}

func (i *large_integer) set(v int64) {
	i.lower = uint32(v & 0xffffffff)
	i.upper = int32(v >> 32)
}

type extent struct {
	nextvcn large_integer
	lcn     large_integer
}

type retrieval_pointers_buffer struct {
	extentcount uint32 // DWORD;
	startingvcn large_integer
	//&extents[1]	     // array of mapped extents (out)
}

type starting_vcn_input_buffer struct {
	startingvcn large_integer // LARGE_INTEGER
}

// offsetof returns the physical offset (relative to disk start) of
// the data at the specified absolute position in an open file
func offsetof(f *os.File, logical uint64) (uint64, error) {

	fd := syscall.Handle(f.Fd())

	extentcount := 1
	extents := make([]extent, extentcount+2) // not sure why we need one extra here but we do

	ptr := unsafe.Pointer(uintptr(unsafe.Pointer(&extents[1])) - retrieval_pointers_buffer_size)
	lpOutBuffer := (*retrieval_pointers_buffer)(ptr)
	lpOutBuffer.startingvcn.set(0)
	nOutBufferSize := uint32(retrieval_pointers_buffer_size + (extentcount+1)*extentsize)

	var bytesreturned uint32
	var startingvcn starting_vcn_input_buffer
	bps, _ := bytesPerSector(f.Name())
	startingvcn.startingvcn.set(int64(logical / bps))

	err := syscall.DeviceIoControl(fd,
		fsctl_get_retrieval_pointers,
		(*byte)(unsafe.Pointer(&startingvcn)), // A pointer to the input buffer, a STARTING_VCN_INPUT_BUFFER structure. (LPVOID)
		starting_vcn_input_buffer_size,        // The size of the input buffer, in bytes. (DWORD)
		(*byte)(ptr),                          // A pointer to the output buffer, a RETRIEVAL_POINTERS_BUFFER variably sized structure (LPVOID)
		nOutBufferSize,                        // The size of the output buffer, in bytes. (DWORD)
		&bytesreturned,                        // A pointer to a variable that receives the size of the data stored in the output buffer, in bytes. (LPDWORD)
		nil)                                   // lpOverlapped	//A pointer to an OVERLAPPED structure; if fd is opened without specifying FILE_FLAG_OVERLAPPED, lpOverlapped is ignored.(LPOVERLAPPED)

	if err != nil && err.Error() != "More data is available." {
		return 0, err
	}

	return uint64(extents[1].lcn.get()) * bps, nil
}

// bps uses syscall to get disk bytes per sector.
// Credit to https://github.com/StalkR/goircbot/blob/master/lib/disk/space_windows.go
func bytesPerSector(path string) (uint64, error) {
	volume := filepath.VolumeName(path)
	bps, ok := bpsmap[volume]
	if ok {
		return bps, nil
	}

	kernel32, err := syscall.LoadLibrary("Kernel32.dll")
	if err != nil {
		return failbps, err
	}
	defer syscall.FreeLibrary(kernel32)
	GetDiskFreeSpace, err := syscall.GetProcAddress(syscall.Handle(kernel32), "GetDiskFreeSpaceW")
	if err != nil {
		return failbps, err
	}

	sectorsPerCluster := int64(0)
	bytesPerSector := int64(0)
	numberOfFreeClusters := int64(0)
	totalNumberOfClusters := int64(0)

	r1, _, e1 := syscall.Syscall6(uintptr(GetDiskFreeSpace), 4,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(volume))),
		uintptr(unsafe.Pointer(&sectorsPerCluster)),
		uintptr(unsafe.Pointer(&bytesPerSector)),
		uintptr(unsafe.Pointer(&numberOfFreeClusters)),
		uintptr(unsafe.Pointer(&totalNumberOfClusters)), 0)

	if r1 == 0 {
		if e1 != 0 {
			return failbps, error(e1)
		} else {
			return failbps, syscall.EINVAL
		}
	}

	return uint64(sectorsPerCluster * bytesPerSector), nil

}

var dummy uint64

func getID(path string, info os.FileInfo) (id, error) {
	var id id
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return id, err
	}
	h, err := syscall.CreateFile(pathp, 0, 0, nil, syscall.OPEN_EXISTING, syscall.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return id, err
	}
	defer syscall.CloseHandle(h)
	var i syscall.ByHandleFileInformation
	err = syscall.GetFileInformationByHandle(syscall.Handle(h), &i)
	if err != nil {
		return id, err
	}
	id.Dev = uint64(i.VolumeSerialNumber)
	id.Ino = uint64(i.FileIndexHigh)<<32 | uint64(i.FileIndexLow)
	return id, nil
}
