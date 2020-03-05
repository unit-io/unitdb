// +build windows

package fs

import (
	"os"
	"syscall"
)

var (
	modkernel32    = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx = modkernel32.NewProc("LockFileEx")
)

const (
	errorLockViolation = 0x21
)

type windowsFileLock struct {
	fd   syscall.Handle
	name string
}

func (fl *windowsFileLock) Unlock() error {
	if err := os.Remove(fl.name); err != nil {
		return err
	}
	return syscall.Close(fl.fd)
}

func newLockFile(name string) (LockFile, error) {
	path, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}
	fd, err := syscall.CreateFile(path,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.CREATE_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0)
	if err != nil {
		return nil, os.ErrExist
	}
	return &windowsFileLock{fd, name}, nil
}
