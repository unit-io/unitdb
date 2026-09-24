package internal

import (
	"sync"
	"time"

	"github.com/unit-io/unitdb/server/utp"
)

const (
	defaultPoolCapacity = 27
	// publish request (containing a batch of messages) in bytes. Must be lower
	// than the gRPC limit of 4 MiB.
	maxPubBytes          = 3.5 * 1024 * 1024
	maxPubCount          = 1000
	defaultBatchDuration = 100 * time.Millisecond
	publishWaitTimeout   = 30 * time.Second
)

type (
	timeID       int64
	batchOptions struct {
		batchDuration       time.Duration
		batchCountThreshold int
		batchByteThreshold  int
	}
	batch struct {
		count       int
		size        int
		pubMessages []*utp.PublishMessage
	}
	batchManager struct {
		mu           sync.RWMutex
		opts         *batchOptions
		batchGroup   map[timeID]*batch
		publishQueue chan *batch
		send         chan *batch
		stop         chan struct{}
		stopOnce     sync.Once
		stopWg       sync.WaitGroup
	}
)

func (m *batchManager) newBatch(timeID timeID) *batch {
	b := &batch{
		pubMessages: make([]*utp.PublishMessage, 0),
	}
	m.batchGroup[timeID] = b

	return b
}

func (c *_Conn) newBatchManager(opts *batchOptions) {
	m := &batchManager{
		opts:         opts,
		batchGroup:   make(map[timeID]*batch),
		publishQueue: make(chan *batch, 1),
		send:         make(chan *batch, defaultPoolCapacity),
		stop:         make(chan struct{}),
	}

	m.newBatch(m.TimeID(0))

	// start the publish loop
	go m.publishLoop(defaultBatchDuration)

	// start the commit loop
	m.stopWg.Add(1)
	go m.publish(c, publishWaitTimeout)

	// start the dispacther
	m.stopWg.Add(1)
	go m.dispatch()

	c.batchManager = m
}

// close tells dispatcher to exit, and wether or not complete queued jobs.
func (m *batchManager) close() {
	if m == nil {
		return
	}
	m.stopOnce.Do(func() {
		// Close write queue and wait for currently running jobs to finish.
		close(m.stop)
	})
	m.stopWg.Wait()
}

// add adds a publish message to a batch in the batch group.
func (m *batchManager) add(delay int32, pubMsg *utp.PublishMessage) {
	m.mu.Lock()
	timeID := m.TimeID(delay)
	b, ok := m.batchGroup[timeID]
	if !ok {
		b = m.newBatch(timeID)
	}
	b.pubMessages = append(b.pubMessages, pubMsg)
	b.count++
	b.size += len(pubMsg.Payload)
	full := b.count > m.opts.batchCountThreshold || b.size > m.opts.batchByteThreshold
	if full {
		delete(m.batchGroup, timeID)
	}
	m.mu.Unlock()

	// Push outside the lock: it waits for the dispatcher.
	if full {
		m.push(b)
	}
}

// takeDue removes and returns the batches whose time has come.
func (m *batchManager) takeDue() []*batch {
	m.mu.Lock()
	defer m.mu.Unlock()
	timeNow := timeID(TimeNow().UnixNano())
	var due []*batch
	for timeID, batch := range m.batchGroup {
		if timeID < timeNow {
			due = append(due, batch)
			delete(m.batchGroup, timeID)
		}
	}
	return due
}

// push enqueues a batch to publish.
func (m *batchManager) push(b *batch) {
	if len(b.pubMessages) != 0 {
		m.publishQueue <- b
	}
}

// publishLoop enqueue the publish batches.
func (m *batchManager) publishLoop(interval time.Duration) {
	var publishC <-chan time.Time

	if interval > 0 {
		publishTicker := time.NewTicker(interval)
		defer publishTicker.Stop()
		publishC = publishTicker.C
	}

	for {
		select {
		case <-m.stop:
			for _, batch := range m.takeDue() {
				m.push(batch)
			}
			close(m.publishQueue)

			return
		case <-publishC:
			for _, batch := range m.takeDue() {
				m.push(batch)
			}
		}
	}
}

// dispatch hands the queued batches to the publisher. When the publisher's
// pool is full it waits instead of dropping the batch.
func (m *batchManager) dispatch() {
	for b := range m.publishQueue {
		m.send <- b
	}
	close(m.send)
	m.stopWg.Done()
}

// publish publishes the messages.
func (m *batchManager) publish(c *_Conn, publishWaitTimeout time.Duration) {
	for {
		select {
		case <-m.stop:
			// run queued messges from the publish queue and
			// process it until queue is empty.
			b, ok := <-m.send
			if !ok {
				m.stopWg.Done()
				return
			}
			m.publishBatch(c, b, publishWaitTimeout)
		case b := <-m.send:
			if b != nil {
				m.publishBatch(c, b, publishWaitTimeout)
			}
		}
	}
}

// publishBatch sends a batch to the connection, unless the connection closes
// or does not take it in time.
func (m *batchManager) publishBatch(c *_Conn, b *batch, publishWaitTimeout time.Duration) {
	pub := &utp.Publish{Messages: b.pubMessages}
	pub.MessageID = uint16(c.MessageIds.NextID(utp.PUBLISH))
	select {
	case c.pub <- pub:
	case <-c.closeC:
	case <-time.After(publishWaitTimeout):
	}
}

func (m *batchManager) TimeID(delay int32) timeID {
	return timeID(TimeNow().Add(m.opts.batchDuration + (time.Duration(delay) * time.Millisecond)).Truncate(m.opts.batchDuration).UnixNano())
}
