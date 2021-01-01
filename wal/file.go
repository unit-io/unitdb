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

package wal

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"

	"github.com/unit-io/bpool"
)

type (
	_FileStore struct {
		sync.RWMutex
		dirName string
		opened  bool
	}
	_FileInfos []os.FileInfo
)

func openFile(dirName string, bufferSize int64) (*_FileStore, error) {
	fs := &_FileStore{
		dirName: dirName,
		opened:  false,
	}

	// if no store directory was specified, by default use the current working directory.
	if dirName == "" {
		fs.dirName, _ = os.Getwd()
	}

	// if store dir does not exists then create it.
	if !exists(dirName) {
		perms := os.FileMode(0770)
		if err := os.MkdirAll(fs.dirName, perms); err != nil {
			return nil, err
		}
	}
	fs.opened = true

	return fs, nil
}

func (fs *_FileStore) close() {
	fs.Lock()
	defer fs.Unlock()
	fs.opened = false
}

func (fs *_FileStore) put(info _LogInfo, data *bpool.Buffer) error {
	fs.Lock()
	defer fs.Unlock()
	if !fs.opened {
		return errors.New("Trying to use file store, but not open")
	}
	tmp := tmpPath(fs.dirName, info.timeID)
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	buf, err := info.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(buf, 0); err != nil {
		return err
	}
	if _, err := f.WriteAt(data.Bytes(), int64(logHeaderSize)); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	log := logPath(fs.dirName, info.timeID)

	if err := os.Rename(tmp, log); err != nil {
		return err
	}

	if !exists(log) {
		return errors.New(fmt.Sprintf("file not created, %s", log))
	}

	return nil
}

func (fs *_FileStore) read(timeID int64, data *bpool.Buffer) _LogInfo {
	fs.RLock()
	defer fs.RUnlock()

	info := _LogInfo{}

	if !fs.opened {
		// trying to use file store, but not open.
		return info
	}

	log := logPath(fs.dirName, timeID)
	if !exists(log) {
		return info
	}

	f, err := os.Open(log)
	if err != nil {
		return info
	}

	buf := make([]byte, uint32(logHeaderSize))
	if _, err := f.ReadAt(buf, 0); err != nil {
		f.Close()
		os.Rename(log, corruptPath(fs.dirName, timeID))

		// log was unreadable, return nil
		return info
	}

	if err := info.UnmarshalBinary(buf); err != nil {
		f.Close()
		os.Rename(log, corruptPath(fs.dirName, timeID))

		// log was unreadable, return nil
		return info
	}

	if _, err := data.Extend(int64(info.size)); err != nil {
		return info
	}

	if _, err := f.ReadAt(data.Internal(), int64(logHeaderSize)); err != nil {
		f.Close()
		os.Rename(log, corruptPath(fs.dirName, timeID))

		// log was unreadable, return nil
		return info
	}
	f.Close()

	return info
}

// all provides a list of all time IDs currently stored in the file store.
func (fs *_FileStore) all() []int64 {
	var timeIDs []int64
	var files _FileInfos

	if !fs.opened {
		// trying to use file store, but not open.
		return nil
	}

	files, err := ioutil.ReadDir(fs.dirName)
	if err != nil {
		return nil
	}

	sort.Slice(files[:], func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	for _, f := range files {
		name := f.Name()
		if name[len(name)-4:] != logExt {
			// skipping file, doesn't have right extension.
			continue
		}

		timeID, _ := strconv.ParseInt(name[:len(name)-4], 10, 64) // remove file extension
		timeIDs = append(timeIDs, timeID)
	}

	return timeIDs
}

func (fs *_FileStore) del(timeID int64) {
	fs.Lock()
	defer fs.Unlock()

	if !fs.opened {
		// trying to use file store, but not open.
		return
	}

	log := logPath(fs.dirName, timeID)
	if !exists(log) {
		return
	}

	os.Remove(log)
}

// reset removes all persisted logs from file store.
func (fs *_FileStore) reset() {
	for _, timeID := range fs.all() {
		fs.del(timeID)
	}
}

func logPath(dirName string, timeID int64) string {
	suffix := strconv.FormatInt(timeID, 10) + logExt
	return path.Join(dirName, suffix)
}

func tmpPath(dirName string, timeID int64) string {
	suffix := strconv.FormatInt(timeID, 10) + tmpExt
	return path.Join(dirName, suffix)
}

func corruptPath(dirName string, timeID int64) string {
	suffix := strconv.FormatInt(timeID, 10) + corruptExt
	return path.Join(dirName, suffix)
}

func exists(file string) bool {
	if _, err := os.Stat(file); err != nil {
		if os.IsNotExist(err) {
			return false
		}
		panic(err)
	}
	return true
}
