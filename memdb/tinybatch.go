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

type _TinyBatch struct {
	sync.RWMutex
	ID      int64
	managed bool

	entryCount uint32

	doneChan chan struct{}
}

func (b *_TinyBatch) timeID() _TimeID {
	return _TimeID(atomic.LoadInt64(&b.ID))
}

func (b *_TinyBatch) setTimeID(timeID _TimeID) {
	atomic.StoreInt64(&b.ID, int64(timeID))
}

func (b *_TinyBatch) len() uint32 {
	return atomic.LoadUint32(&b.entryCount)
}

func (b *_TinyBatch) incount() uint32 {
	return atomic.AddUint32(&b.entryCount, 1)
}

func (b *_TinyBatch) reset() {
	b.Lock()
	defer b.Unlock()
	atomic.StoreUint32(&b.entryCount, 0)
}

func (b *_TinyBatch) abort() {
	b.reset()
	close(b.doneChan)
}

// setManaged sets batch is managed.
func (b *_TinyBatch) setManaged() {
	b.managed = true
}

// unsetManaged sets batch to unmanaged.
func (b *_TinyBatch) unsetManaged() {
	b.managed = false
}

type _BatchPool struct {
	db           *DB
	maxBatches   int
	writeQueue   chan *_TinyBatch
	batchQueue   chan *_TinyBatch
	waitingQueue _Queue
	stoppedChan  chan struct{}
	stopOnce     sync.Once
	stopped      int32
	waiting      int32
	wait         bool
}

// size returns maximum number of concurrent batches.
func (p *_BatchPool) size() int {
	return p.maxBatches
}

// stop tells dispatcher to exit, and wether or not complete queued batches.
func (p *_BatchPool) stop(wait bool) {
	// Acquire tinyBatch write lock
	p.db.internal.writeLockC <- struct{}{}
	defer func() {
		<-p.db.internal.writeLockC
	}()
	p.stopOnce.Do(func() {
		atomic.StoreInt32(&p.stopped, 1)
		p.wait = wait
		// Close write queue and wait for currently running batches to finish
		close(p.writeQueue)
	})
	<-p.stoppedChan
}

// stopWait stops batch pool and wait for all queued batches to complete.
func (p *_BatchPool) stopWait() {
	p.stop(true)
}

// stopped returns true if batch pool has been stopped.
func (p *_BatchPool) isStopped() bool {
	return atomic.LoadInt32(&p.stopped) != 0
}

// waitQueueSize returns count of batches in waitingQueue.
func (p *_BatchPool) waitQueueSize() int {
	return int(atomic.LoadInt32(&p.waiting))
}

// write enqueues a batch to write.
func (p *_BatchPool) write(tinyBatch *_TinyBatch) {
	if tinyBatch != nil {
		p.writeQueue <- tinyBatch
	}
}

// writeWait enqueues the given batch and waits for it to be executed.
func (p *_BatchPool) writeWait(tinyBatch *_TinyBatch) {
	if tinyBatch == nil {
		return
	}
	p.writeQueue <- tinyBatch
	<-tinyBatch.doneChan
}

// dispatch handles tiny batch commit for the batches in queue.
func (p *_BatchPool) dispatch() {
	defer close(p.stoppedChan)
	timeout := time.NewTimer(2 * time.Second)
	var batchCount int
	var idle bool
Loop:
	for {
		// As long as batches are in waiting queue, incoming
		// batch are put into the waiting queueand batches to run are taken from waiting queue.
		if p.waitingQueue.len() != 0 {
			if !p.processWaitingQueue() {
				break Loop
			}
			continue
		}

		select {
		case tinyBatch, ok := <-p.writeQueue:
			if !ok {
				break Loop
			}
			select {
			case p.batchQueue <- tinyBatch:
			default:
				if batchCount < nPoolSize {
					go p.commit(tinyBatch, p.batchQueue)
					batchCount++
				} else {
					// Enqueue batch to be executed later.
					p.waitingQueue.push(tinyBatch)
					atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.len()))
				}
			}
			idle = false
		case <-timeout.C:
			if idle && batchCount > 0 {
				if p.killIdleBatch() {
					batchCount--
				}
			}
			idle = true
			timeout.Reset(2 * time.Second)
		}
	}

	// If instructed to wait, then run batches that are already in queue.
	if p.wait {
		p.runQueuedBatches()
	}

	// Stop all remaining tiny batch as it become ready.
	for batchCount > 0 {
		p.batchQueue <- nil
		batchCount--
	}
	fmt.Println("batchPool.dispatch: stop")
	timeout.Stop()
}

// commit run initial tiny batch commit, then start tiny batch waiting for more.
func (p *_BatchPool) commit(tinyBatch *_TinyBatch, batchQueue chan *_TinyBatch) {
	if err := p.db.tinyCommit(tinyBatch); err != nil {
		// p.db.rollback(tinyBatch)
	}

	go p.tinyCommit(batchQueue)
}

// tinyCommit commits batch and stops when it receive a nil batch.
func (p *_BatchPool) tinyCommit(batchQueue chan *_TinyBatch) {
	// abort time window entries
	// defer p.db.abort()

	for tinyBatch := range batchQueue {
		if tinyBatch == nil {
			return
		}

		if err := p.db.tinyCommit(tinyBatch); err != nil {
			// p.db.rollback(tinyBatch)
		}
	}
}

// processWaiting queue puts new batches onto the waiting queue,
// removes batches from the waiting queue. Returns false if batchPool is stopped.
func (p *_BatchPool) processWaitingQueue() bool {
	select {
	case b, ok := <-p.writeQueue:
		if !ok {
			return false
		}
		p.waitingQueue.push(b)
	case p.batchQueue <- p.waitingQueue.front():
		p.waitingQueue.pop()
	}
	atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.len()))
	return true
}

func (p *_BatchPool) killIdleBatch() bool {
	select {
	case p.batchQueue <- nil:
		return true
	default:
		return false
	}
}

// runQueuedBatches removes each batch from the waiting queue and
// process it until queue is empty.
func (p *_BatchPool) runQueuedBatches() {
	for p.waitingQueue.len() != 0 {
		p.batchQueue <- p.waitingQueue.pop()
		atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.len()))
	}
}

type _Queue struct {
	buf   []*_TinyBatch
	head  int
	tail  int
	count int
}

// len returns the number of elements currently stored in the queue.
func (q *_Queue) len() int {
	return q.count
}

// push appends an element to the back of the queue.
func (q *_Queue) push(elem *_TinyBatch) {
	q.grow()

	q.buf[q.tail] = elem
	// calculate new tail position.
	q.tail = (q.tail + 1) & (len(q.buf) - 1) // bitwise modulus
	q.count++
}

// pop removes and return an element from front of the queue.
func (q *_Queue) pop() *_TinyBatch {
	if q.count <= 0 {
		panic("batchPool: pop called on empty queue")
	}
	elem := q.buf[q.head]
	q.buf[q.head] = nil
	// Calculate new head position.
	q.head = (q.head + 1) & (len(q.buf) - 1) // bitwise modulus
	q.count--
	q.shrink()
	return elem
}

// front returns element at the front of the queue. This is the element
// that would be returned by pop().
func (q *_Queue) front() *_TinyBatch {
	if q.count <= 0 {
		panic("batchPool: pop called on empty queue")
	}
	return q.buf[q.head]
}

// at returns element at index i in the queue without removing element from the queue.
// at(0) refers to first element and is same as front(). at(len()0-1) refers to the last element.
func (q *_Queue) at(i int) *_TinyBatch {
	if i < 0 || i > q.count {
		panic("batchPool: at called with index out of range")
	}
	// bitwise modulus
	return q.buf[(q.head+i)&(len(q.buf)-1)]
}

// grow resizes the queue to fit exactly twice its current content.
func (q *_Queue) grow() {
	if len(q.buf) == 0 {
		q.buf = make([]*_TinyBatch, nPoolSize)
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
	newBuf := make([]*_TinyBatch, q.count<<1)
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
