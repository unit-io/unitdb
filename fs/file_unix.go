// +build !windows

package fs

import (
	"os"
	"syscall"
)

type unixFileLock struct {
	f    *os.File
	name string
}

func (fl *unixFileLock) Unlock() error {
	if err := os.Remove(fl.name); err != nil {
		return err
	}
	return fl.f.Close()
}

func lockFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if err == syscall.EWOULDBLOCK {
			err = os.ErrExist
		}
		return err
	}
	return nil
}

func newLockFile(name string) (LockFile, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	if err := lockFile(f); err != nil {
		f.Close()
		return nil, err
	}
	return &unixFileLock{f, name}, nil
}
