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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
)

// LogStatus represents the state of log, written to applied
type LogStatus uint16

const (
	// logStatusWritten indicates that the log has been written,
	// but not completed. During recovery, logs with this status
	// should be loaded and their updates should be provided to the user.
	logStatusWritten LogStatus = iota

	// logStatusApplied indicates that the logs has been written and
	// applied. Logs with this status can be ignored during recovery,
	// and their associated blocks can be reclaimed.
	logStatusApplied

	// logStatusReleased indicates that the logs has been applied and merged with freeblocks.
	// Logs with this status are removed from the WAL.
	logStatusReleased

	defaultLogReleaseInterval = 15 * time.Second
	defaultBufferSize         = 1 << 27
	version                   = 1 // file format version
)

type logs []logInfo
type (
	// WALInfo provides WAL stats
	WALInfo struct {
		logCountWritten int64
		logCountApplied int64
	}
	// WAL write ahead logs to recover db commit failure dues to db crash or other unexpected errors
	WAL struct {
		// wg is a WaitGroup that allows us to wait for the syncThread to finish to
		// ensure a clean shutdown
		wg sync.WaitGroup
		mu sync.RWMutex

		WALInfo
		logs
		pendingLogs map[int64]logs // map[id]logs

		bufPool *bpool.BufferPool
		logFile file

		opts Options

		// close
		closed uint32
		closeC chan struct{}
	}

	// Options wal options to create new WAL. WAL logs uses cyclic rotation to avoid fragmentation.
	// It allocates free blocks only when log reaches target size
	Options struct {
		Path       string
		TargetSize int64
		BufferSize int64
		Reset      bool
	}
)

func newWal(opts Options) (wal *WAL, needsRecovery bool, err error) {
	// Create a new WAL.
	if opts.BufferSize == 0 {
		opts.BufferSize = defaultBufferSize
	}
	wal = &WAL{
		pendingLogs: make(map[int64]logs),
		bufPool:     bpool.NewBufferPool(opts.TargetSize, nil),
		opts:        opts,
		// close
		closeC: make(chan struct{}, 1),
	}
	wal.logFile, err = openFile(opts.Path, opts.TargetSize)
	if err != nil {
		return wal, false, err
	}
	if opts.Reset {
		if err := wal.logFile.reset(); err != nil {
			return wal, false, err
		}
	}
	if wal.logFile.size == 0 {
		if _, err = wal.logFile.allocate(headerSize); err != nil {
			return nil, false, err
		}
		wal.logFile.segments = newSegments()
		if err := wal.Sync(); err != nil {
			return nil, false, err
		}
	} else {
		if err := wal.readHeader(); err != nil {
			if err := wal.Close(); err != nil {
				return nil, false, errors.New("newWal error: unable to read wal header")
			}
			return nil, false, err
		}
		if err := wal.recoverWal(); err != nil {
			return nil, false, err
		}
	}

	wal.releaser(defaultLogReleaseInterval)

	return wal, len(wal.logs) != 0, nil
}

func (wal *WAL) writeHeader() error {
	h := header{
		signature: signature,
		version:   version,
		segments:  wal.logFile.segments,
	}
	return wal.logFile.writeMarshalableAt(h, 0)
}

func (wal *WAL) readHeader() error {
	h := &header{}
	if err := wal.logFile.readUnmarshalableAt(h, headerSize, 0); err != nil {
		return err
	}
	wal.logFile.segments = h.segments
	return nil
}

func (wal *WAL) recoverLogHeaders() error {
	offset := int64(headerSize)
	l := logInfo{}
	for {
		offset = wal.logFile.segments.recoveryOffset(offset)
		if err := wal.logFile.readUnmarshalableAt(&l, uint32(logHeaderSize), offset); err != nil {
			if err == io.EOF {
				// Expected error.
				return nil
			}
			return err
		}
		if l.offset < 0 || l.status > logStatusReleased {
			fmt.Println("wal.recoverLogHeader: logInfo ", wal.logFile.segments, l)
			return nil
		}
		if l.status == logStatusWritten {
			wal.logs = append(wal.logs, l)
		}
		offset = l.offset + int64(l.size)
	}
}

// recoverWal recovers a WAL for the log written but not released. It also updates free blocks
func (wal *WAL) recoverWal() error {
	// Truncate log file.
	if err := wal.logFile.Truncate(wal.logFile.size); err != nil {
		return err
	}

	return wal.recoverLogHeaders()
}

func (wal *WAL) put(id int64, log logInfo) error {
	log.version = version
	wal.logCountWritten++
	if _, ok := wal.pendingLogs[id]; ok {
		wal.pendingLogs[id] = append(wal.pendingLogs[id], log)
	} else {
		wal.pendingLogs[id] = logs{log}
	}
	return nil
}

func (wal *WAL) logMerge(log logInfo) error {
	if log.status == logStatusWritten {
		return nil
	}
	released := wal.logFile.segments.free(log.offset, log.size)
	wal.logFile.segments.swap(wal.opts.TargetSize)
	if released {
		log.status = logStatusReleased
	}
	return nil
}

// SignalLogApplied informs the WAL that it is safe to reuse blocks.
func (wal *WAL) SignalLogApplied(id int64) error {
	wal.wg.Add(1)
	wal.mu.Lock()
	defer func() {
		wal.mu.Unlock()
		wal.wg.Done()
	}()

	var err1 error
	logs := wal.pendingLogs[id]
	for i := range logs {
		logs[i].status = logStatusApplied
		if err := wal.logMerge(logs[i]); err != nil {
			err1 = err
			continue
		}
		if logs[i].status == logStatusReleased {
			continue
		}
		if err := wal.logFile.writeMarshalableAt(logs[i], logs[i].offset); err != nil {
			err1 = err
			continue
		}
	}
	if err := wal.writeHeader(); err != nil {
		return err
	}
	return err1
}

// Reset resets log file and log segments
func (wal *WAL) Reset() error {
	wal.pendingLogs = make(map[int64]logs)
	if err := wal.logFile.reset(); err != nil {
		return err
	}
	if _, err := wal.logFile.allocate(headerSize); err != nil {
		return err
	}
	wal.logFile.segments = newSegments()
	if err := wal.Sync(); err != nil {
		return err
	}
	return nil
}

//Sync syncs log entries to disk
func (wal *WAL) Sync() error {
	wal.writeHeader()
	return wal.logFile.Sync()
}

// Close closes the wal, frees used resources and checks for active
// logs.
func (wal *WAL) Close() error {
	if !wal.setClosed() {
		return errors.New("wal is closed")
	}
	close(wal.closeC)

	// Make sure sync thread isn't running
	wal.wg.Wait()

	return wal.logFile.Close()
}

// Set closed flag; return true if not already closed.
func (wal *WAL) setClosed() bool {
	if wal == nil {
		return false
	}
	return atomic.CompareAndSwapUint32(&wal.closed, 0, 1)
}

// Check whether WAL was closed.
func (wal *WAL) isClosed() bool {
	return atomic.LoadUint32(&wal.closed) != 0
}

// Check read ok status.
func (wal *WAL) ok() error {
	if wal.isClosed() {
		return errors.New("wal is closed.")
	}
	return nil
}

func (wal *WAL) releaseLogs() error {
	wal.wg.Add(1)
	wal.mu.Lock()
	defer func() {
		wal.mu.Unlock()
		wal.wg.Done()
	}()

	for id, logs := range wal.pendingLogs {
		l := len(logs)
		for i := 0; i < l; i++ {
			if logs[i].status == logStatusReleased {
				// Remove log from wal
				wal.pendingLogs[id] = wal.pendingLogs[id][:i+copy(wal.pendingLogs[id][i:], wal.pendingLogs[id][i+1:])]
				l -= 1
				i--
			}
		}
	}

	return nil
}

func (wal *WAL) releaser(interval time.Duration) {
	releaserTicker := time.NewTicker(interval)
	go func() {
		defer func() {
			releaserTicker.Stop()
		}()
		for {
			select {
			case <-wal.closeC:
				return
			case <-releaserTicker.C:
				wal.releaseLogs()
			}
		}
	}()
}

// New will open a WAL. If the previous run did not shut down cleanly, a set of
// log entries will be returned which got committed successfully to the WAL, but
// were never signaled as fully completed.
//
// If no WAL exists, a new one will be created.
//
func New(opts Options) (*WAL, bool, error) {
	// Create a wal
	return newWal(opts)
}
