// +build windows

/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32    = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx = modkernel32.NewProc("LockFileEx")
)

const (
	errorLockViolation    = 0x21
	lockfileExclusiveLock = 3
)

type windowsFileLock struct {
	fd   syscall.Handle
	name string
}

// Unlock removes the lock from file.
func (fl *windowsFileLock) Unlock() error {
	if err := os.Remove(fl.name); err != nil {
		return err
	}
	return syscall.Close(fl.fd)
}

func lockFile(h syscall.Handle, flags, reserved, locklow, lockhigh uint32, ol *syscall.Overlapped) error {
	r1, _, err := syscall.Syscall6(procLockFileEx.Addr(), 6, uintptr(h), uintptr(flags), uintptr(reserved), uintptr(locklow), uintptr(lockhigh), uintptr(unsafe.Pointer(ol)))
	if r1 == 0 && (err == syscall.ERROR_FILE_EXISTS || err == errorLockViolation) {
		return os.ErrExist
	}
	return nil
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
	defer func() {
		if err != nil {
			syscall.Close(fd)
		}
	}()
	var ol syscall.Overlapped
	err = lockFile(fd, lockfileExclusiveLock, 0, 1, 0, &ol)
	if err != nil {
		return nil, err
	}
	return &windowsFileLock{fd, name}, nil
}
