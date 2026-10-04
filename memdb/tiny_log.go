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

package memdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Default settings
const (
	defaultBlockDuration = 1 * time.Second
	defaultWriteInterval = 100 * time.Millisecond
	defaultTimeout       = 2 * time.Second
	defaultPoolCapacity  = 27
	defaultLogCount      = 1
)

type _TinyLog struct {
	mu rwMutex[tinyLogRank]
	id _TimeID
	_TimeID

	managed  bool
	doneChan chan struct{}
	// err is the error of the log's write to the WAL, set before doneChan
	// is closed.
	err error
}

func (l *_TinyLog) ID() _TimeID {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.id
}

func (l *_TinyLog) timeID() _TimeID {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l._TimeID
}

func (b *_TinyLog) abort() {
	close(b.doneChan)
}

type (
	_TinyLogOptions struct {
		// writeInterval default value is 100ms, setting writeInterval to zero disables writing the log to the WAL.
		writeInterval time.Duration

		// timeout controls how often log pool kill idle jobs.
		//
		// Default value is 2 seconds
		timeout time.Duration

		// blockDuration is used to create new timeID.
		//
		// Default value is defaultBlockDuration.
		blockDuration time.Duration

		// poolCapacity controls size of pre-allocated log queue.
		//
		// Default value is defaultPoolCapacity.
		poolCapacity int

		// logCount controls number of goroutines commiting the log to the WAL.
		//
		// Default value is 1, so logs are sent from single goroutine, this
		// value might need to be bumped under high load.
		logCount int
	}
	_TinyLogManager struct {
		mu rwMutex[managerRank]
		// rotateMu is held for reading by Put from reading timeID until its entry
		// is written, and for writing while the tiny log rotates. This keeps a Put
		// from writing into a time block after its last tiny log was queued to the WAL.
		rotateMu rwMutex[rotateRank]
		// current is the time ID of the current tiny log, read without a
		// lock: the commit loop reads it, and rotation holds mu waiting on
		// the commit loop (see the lock order).
		current    atomic.Int64
		db         *DB
		opts       *_TinyLogOptions
		tinyLog    *_TinyLog
		writeQueue chan *_TinyLog
		logQueue   chan *_TinyLog
		stop       chan struct{}
		stopOnce   sync.Once
		stopWg     sync.WaitGroup
	}
)

func (src *_TinyLogOptions) withDefaultOptions() *_TinyLogOptions {
	opts := _TinyLogOptions{}
	if src != nil {
		opts = *src
	}
	if opts.poolCapacity < 1 {
		opts.poolCapacity = 1
	}
	if opts.writeInterval == 0 {
		opts.writeInterval = defaultWriteInterval
	}
	if opts.timeout == 0 {
		opts.timeout = defaultTimeout
	}
	if opts.blockDuration == 0 {
		opts.blockDuration = defaultBlockDuration
	}
	if opts.logCount < 1 {
		opts.logCount = defaultLogCount
	}

	return &opts
}

func (p *_TinyLogManager) newTinyLog() {
	id := p.db.newLogID()
	timeID := _TimeID(time.Unix(0, int64(id)).UTC().Truncate(p.opts.blockDuration).UnixNano())
	p.db.addTimeBlock(timeID)
	p.db.internal.timeMark.add(timeID)
	p.tinyLog = &_TinyLog{id: id, _TimeID: timeID, managed: false, doneChan: make(chan struct{})}
	p.current.Store(int64(timeID))
}

func (db *DB) newLogManager(opts *_TinyLogOptions) {
	opts = opts.withDefaultOptions()
	logManager := &_TinyLogManager{
		db:         db,
		opts:       opts,
		tinyLog:    &_TinyLog{},
		writeQueue: make(chan *_TinyLog, 1),
		logQueue:   make(chan *_TinyLog, opts.poolCapacity),
		stop:       make(chan struct{}),
	}

	logManager.newTinyLog()
	// The loops read it: the commit loop asks for the current block.
	db.internal.logManager = logManager

	// start the write loop
	go logManager.writeLoop(opts.writeInterval)

	// start the commit loop
	logManager.stopWg.Add(1)
	go logManager.commitLoop()

	// start the dispacther
	for i := 0; i < opts.logCount; i++ {
		logManager.stopWg.Add(1)
		go logManager.dispatch(opts.timeout)
	}
}

// timeID returns tinyLog timeID.
func (p *_TinyLogManager) timeID() _TimeID {
	return _TimeID(p.current.Load())
}

// size returns maximum number of concurrent jobs.
func (p *_TinyLogManager) size() int {
	return p.opts.poolCapacity
}

// close tells dispatcher to exit, and wether or not complete queued jobs.
func (p *_TinyLogManager) close(wait bool) {
	p.stopOnce.Do(func() {
		// Close write queue and wait for currently running jobs to finish.
		close(p.stop)
	})
	p.stopWg.Wait()
}

// closeWait stops worker pool and wait for all queued jobs to complete.
func (p *_TinyLogManager) closeWait() {
	p.close(true)
}

// rotate makes a new tiny log current and enqueues the last to write, in
// that order: the commit loop releases a written log's block if it is past
// (releaseEmpty), and enqueued first, a log could be written while its
// block was still current, and its empty block was never released. The
// caller holds rotateMu and mu.
func (p *_TinyLogManager) rotate() {
	last := p.tinyLog
	p.newTinyLog()
	if last != nil {
		p.writeQueue <- last
	}
}

// write enqueues a log to write.
func (p *_TinyLogManager) write() {
	if p.tinyLog != nil {
		p.writeQueue <- p.tinyLog
	}
}

// flush rotates the current log, and waits until it is written to the WAL.
// Logs are written in order, so every entry put before is written too.
func (p *_TinyLogManager) flush() error {
	p.rotateMu.Lock()
	select {
	case <-p.stop:
		p.rotateMu.Unlock()
		return errClosed
	default:
	}
	p.mu.Lock()
	tinyLog := p.tinyLog
	p.rotate()
	p.mu.Unlock()
	p.rotateMu.Unlock()
	<-tinyLog.doneChan
	return tinyLog.err
}

// writeWait enqueues the log and waits for it to be executed.
func (p *_TinyLogManager) writeWait(tinyLog *_TinyLog) {
	if tinyLog == nil {
		return
	}
	p.writeQueue <- tinyLog
	<-tinyLog.doneChan
}

// writeLoop enqueue the tiny log to the log pool.
func (p *_TinyLogManager) writeLoop(interval time.Duration) {
	var writeC <-chan time.Time

	if interval > 0 {
		writeTicker := time.NewTicker(interval)
		defer writeTicker.Stop()
		writeC = writeTicker.C
	}

	for {
		select {
		case <-p.stop:
			p.rotateMu.Lock()
			p.write()
			p.rotateMu.Unlock()
			close(p.writeQueue)

			return
		case <-writeC:
			// check buffer pool backoff and capacity for excess memory usage
			// before writing tiny log to the WAL.
			switch {
			case p.db.cap() > 0.7:
				block, ok := p.db.timeBlock(p.db.timeID())
				if !ok {
					break
				}
				block.RLock()
				size := block.size()
				block.RUnlock()
				if size < 1<<20 {
					break
				}
				fallthrough
			default:
				p.rotateMu.Lock()
				p.mu.Lock()
				p.rotate()
				p.mu.Unlock()
				p.rotateMu.Unlock()
			}
		}
	}
}

// dispatch handles tiny log commit for the jobs in queue.
func (p *_TinyLogManager) dispatch(timeout time.Duration) {
	for {
		select {
		case tinyLog, ok := <-p.writeQueue:
			// Get a buffer from the queue
			if !ok {
				close(p.logQueue)
				p.stopWg.Done()
				return
			}

			// Wait for room rather than drop the log: a dropped log never
			// completes, so its writer waits forever, and its block's
			// entries reach the WAL only with a later log of the block.
			p.logQueue <- tinyLog
		}
	}
}

// commitLoop commits the tiny log to the WAL.
func (p *_TinyLogManager) commitLoop() {
	for {
		select {
		case <-p.stop:
			// run queued jobs from the log queue until the dispatcher
			// closes it.
			for tinyLog := range p.logQueue {
				if err := p.db.tinyCommit(tinyLog); err != nil {
					fmt.Println("logPool.tinyCommit: error ", err)
				}
			}
			p.stopWg.Done()
			return
		case tinyLog := <-p.logQueue:
			if tinyLog != nil {
				if err := p.db.tinyCommit(tinyLog); err != nil {
					fmt.Println("logPool.tinyCommit: error ", err)
				}
			}
		}
	}
}
