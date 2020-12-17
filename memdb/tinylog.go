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

type _TinyLog struct {
	sync.RWMutex
	id      int64
	managed bool

	entryCount uint32

	doneChan chan struct{}
}

func (b *_TinyLog) ID() _TimeID {
	return _TimeID(atomic.LoadInt64(&b.id))
}

func (b *_TinyLog) setID(ID _TimeID) {
	atomic.StoreInt64(&b.id, int64(ID))
}

func (b *_TinyLog) len() uint32 {
	return atomic.LoadUint32(&b.entryCount)
}

func (b *_TinyLog) incount() uint32 {
	return atomic.AddUint32(&b.entryCount, 1)
}

func (b *_TinyLog) reset() {
	b.Lock()
	defer b.Unlock()
	atomic.StoreUint32(&b.entryCount, 0)
}

func (b *_TinyLog) abort() {
	b.reset()
	close(b.doneChan)
}

type _WorkerPool struct {
	db           *DB
	maxSize      int
	writeQueue   chan *_TinyLog
	logQueue     chan *_TinyLog
	waitingQueue _Queue
	stoppedChan  chan struct{}
	stopOnce     sync.Once
	stopped      int32
	waiting      int32
	wait         bool
}

// size returns maximum number of concurrent jobs.
func (p *_WorkerPool) size() int {
	return p.maxSize
}

// stop tells dispatcher to exit, and wether or not complete queued jobs.
func (p *_WorkerPool) stop(wait bool) {
	p.stopOnce.Do(func() {
		atomic.StoreInt32(&p.stopped, 1)
		p.wait = wait
		// Close write queue and wait for currently running jobs to finish
		close(p.writeQueue)
	})
	<-p.stoppedChan
}

// stopWait stops worker pool and wait for all queued jobs to complete.
func (p *_WorkerPool) stopWait() {
	p.stop(true)
}

// stopped returns true if worker pool has been stopped.
func (p *_WorkerPool) isStopped() bool {
	return atomic.LoadInt32(&p.stopped) != 0
}

// waitQueueSize returns count of jobs in waitingQueue.
func (p *_WorkerPool) waitQueueSize() int {
	return int(atomic.LoadInt32(&p.waiting))
}

// write enqueues a log to write.
func (p *_WorkerPool) write(tinyLog *_TinyLog) {
	if tinyLog != nil {
		p.writeQueue <- tinyLog
	}
}

// writeWait enqueues the log and waits for it to be executed.
func (p *_WorkerPool) writeWait(tinyLog *_TinyLog) {
	if tinyLog == nil {
		return
	}
	p.writeQueue <- tinyLog
	<-tinyLog.doneChan
}

// dispatch handles tiny log commit for the jobs in queue.
func (p *_WorkerPool) dispatch() {
	defer close(p.stoppedChan)
	timeout := time.NewTimer(2 * time.Second)
	var logCount int
	var idle bool
Loop:
	for {
		// As long as jobs are in waiting queue, incoming
		// job are put into the waiting queueand jobs to run are taken from waiting queue.
		if p.waitingQueue.len() != 0 {
			if !p.processWaitingQueue() {
				break Loop
			}
			continue
		}

		select {
		case tinyLog, ok := <-p.writeQueue:
			if !ok {
				break Loop
			}
			select {
			case p.logQueue <- tinyLog:
			default:
				if logCount < nPoolSize {
					go p.commit(tinyLog, p.logQueue)
					logCount++
				} else {
					// Enqueue job to be executed later.
					p.waitingQueue.push(tinyLog)
					atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.len()))
				}
			}
			idle = false
		case <-timeout.C:
			if idle && logCount > 0 {
				if p.killIdleJob() {
					logCount--
				}
			}
			idle = true
			timeout.Reset(2 * time.Second)
		}
	}

	// If instructed to wait, then run jobs that are already in queue.
	if p.wait {
		p.runQueuedJobs()
	}

	// Stop all remaining jobs as it become ready.
	for logCount > 0 {
		p.logQueue <- nil
		logCount--
	}
	timeout.Stop()
}

// commit run initial tiny log commit, then start tiny log waiting for more.
func (p *_WorkerPool) commit(tinyLog *_TinyLog, queue chan *_TinyLog) {
	if err := p.db.tinyCommit(tinyLog); err != nil {
		fmt.Println("workerPool.tinyCommit: error ", err)
	}

	go p.tinyCommit(queue)
}

// tinyCommit commits log and stops when it receive a nil log.
func (p *_WorkerPool) tinyCommit(queue chan *_TinyLog) {
	for tinyLog := range queue {
		if tinyLog == nil {
			return
		}

		if err := p.db.tinyCommit(tinyLog); err != nil {
			fmt.Println("workerPool.tinyCommit: error ", err)
		}
	}
}

// processWaiting queue puts new jobs onto the waiting queue,
// removes jobs from the waiting queue. Returns false if workerPool is stopped.
func (p *_WorkerPool) processWaitingQueue() bool {
	select {
	case p.logQueue <- p.waitingQueue.front():
		p.waitingQueue.pop()
	}
	atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.len()))
	return true
}

func (p *_WorkerPool) killIdleJob() bool {
	select {
	case p.logQueue <- nil:
		return true
	default:
		return false
	}
}

// runQueuedJobs removes each job from the waiting queue and
// process it until queue is empty.
func (p *_WorkerPool) runQueuedJobs() {
	for p.waitingQueue.len() != 0 {
		p.logQueue <- p.waitingQueue.pop()
		atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.len()))
	}
}

type _Queue struct {
	buf   []*_TinyLog
	head  int
	tail  int
	count int
}

// len returns the number of elements currently stored in the queue.
func (q *_Queue) len() int {
	return q.count
}

// push appends an element to the back of the queue.
func (q *_Queue) push(elem *_TinyLog) {
	q.grow()

	q.buf[q.tail] = elem
	// calculate new tail position.
	q.tail = (q.tail + 1) & (len(q.buf) - 1) // bitwise modulus
	q.count++
}

// pop removes and return an element from front of the queue.
func (q *_Queue) pop() *_TinyLog {
	if q.count <= 0 {
		panic("Queue: pop called on empty queue")
	}
	elem := q.buf[q.head]
	q.buf[q.head] = nil
	// Calculate new head position.
	q.head = (q.head + 1) & (len(q.buf) - 1) // bitwise modulus
	q.count--
	q.shrink()

	return elem
}

// front returns an element from front of the queue.
func (q *_Queue) front() *_TinyLog {
	if q.count <= 0 {
		panic("Queue: pop called on empty queue")
	}
	return q.buf[q.head]
}

// at returns element at index i in the queue without removing element from the queue.
// at(0) refers to first element and is same as front(). at(len()0-1) refers to the last element.
func (q *_Queue) at(i int) *_TinyLog {
	if i < 0 || i > q.count {
		panic("Queue: at called with index out of range")
	}
	// bitwise modulus
	return q.buf[(q.head+i)&(len(q.buf)-1)]
}

// grow resizes the queue to fit exactly twice its current content.
func (q *_Queue) grow() {
	if len(q.buf) == 0 {
		q.buf = make([]*_TinyLog, nPoolSize)
		return
	}
	if q.count == len(q.buf) {
		q.resize()
	}
}

// shrink resizes the queue down if bugger if 1/4 full.
func (q *_Queue) shrink() {
	if len(q.buf) > nPoolSize && (q.count<<2) == len(q.buf) {
		q.resize()
	}
}

// resize resizes the queue to fit exactly twice its current content.
func (q *_Queue) resize() {
	newBuf := make([]*_TinyLog, q.count<<1)
	if q.tail > q.head {
		copy(newBuf, q.buf[q.head:q.tail])
	} else {
		n := copy(newBuf, q.buf[q.head:])
		copy(newBuf[n:], q.buf[:q.tail])
	}

	q.head = 0
	q.tail = q.count
	q.buf = newBuf
}
