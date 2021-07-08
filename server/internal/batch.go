package internal

import (
	"sync"
	"time"

	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/store"
)

const (
	defaultTimeout      = 2 * time.Second
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
		count int
		size  int
		msgs  []*lp.PublishMessage
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
		msgs: make([]*lp.PublishMessage, 0),
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
	go m.dispatch(defaultTimeout)

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
func (m *batchManager) add(delay int32, p *lp.PublishMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	timeID := m.TimeID(delay)
	b, ok := m.batchGroup[timeID]
	if !ok {
		b = m.newBatch(timeID)
	}
	b.msgs = append(b.msgs, p)
	b.count++
	b.size += len(p.Payload)
	if b.count > m.opts.batchCountThreshold || b.size > m.opts.batchByteThreshold {
		m.push(b)
		delete(m.batchGroup, timeID)
	}
}

// push enqueues a batch to publish.
func (m *batchManager) push(p *batch) {
	if len(p.msgs) != 0 {
		m.publishQueue <- p
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
			timeNow := timeID(TimeNow().UnixNano())
			for timeID, batch := range m.batchGroup {
				if timeID < timeNow {
					m.mu.Lock()
					m.push(batch)
					delete(m.batchGroup, timeID)
					m.mu.Unlock()
				}
			}
			close(m.publishQueue)

			return
		case <-publishC:
			timeNow := timeID(TimeNow().UnixNano())
			for timeID, batch := range m.batchGroup {
				if timeID < timeNow {
					m.mu.Lock()
					m.push(batch)
					delete(m.batchGroup, timeID)
					m.mu.Unlock()
				}
			}
		}
	}
}

// dispatch handles publishing messages for the batches in queue.
func (m *batchManager) dispatch(timeout time.Duration) {
LOOP:
	b, ok := <-m.publishQueue
	if !ok {
		close(m.send)
		m.stopWg.Done()
		return
	}

	select {
	case m.send <- b:
	default:
		// pool is full, let GC handle the batches
		goto WAIT
	}

WAIT:
	// Wait for a while
	time.Sleep(timeout)
	goto LOOP
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
			pub := &lp.Publish{Messages: b.msgs}
			pub.MessageID = c.MessageIds.NextID(lp.PUBLISH.Value())

			// persist outbound
			store.Log.PersistOutbound(c.adp, uint32(c.connID), pub)

			select {
			case c.pub <- pub:
			case <-time.After(publishWaitTimeout):
				// b.r.setError(errors.New("publish timeout error occurred"))
			}
		case b := <-m.send:
			if b != nil {
				pub := &lp.Publish{Messages: b.msgs}
				pub.MessageID = c.MessageIds.NextID(lp.PUBLISH.Value())

				// persist outbound
				store.Log.PersistOutbound(c.adp, uint32(c.connID), pub)
				select {
				case c.pub <- pub:
				case <-time.After(publishWaitTimeout):
					// b.r.setError(errors.New("publish timeout error occurred"))
				}
			}
		}
	}
}

func (m *batchManager) TimeID(delay int32) timeID {
	return timeID(TimeNow().Add(m.opts.batchDuration + (time.Duration(delay) * time.Millisecond)).Truncate(m.opts.batchDuration).UnixNano())
}
