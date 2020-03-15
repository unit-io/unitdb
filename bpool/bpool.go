package bpool

import (
	"math/rand"
	"sync"
	"time"
)

const (
	maxPoolSize = 271
	// maxBufferSize value to limit maximum memory for the buffer.
	maxBufferSize = (int64(1) << 34) - 1

	// The duration for waiting in the queue due to system memory surge operations
	DefaultInitialInternal     = 500 * time.Millisecond
	DefaultRandomizationFactor = 0.5
	// DefaultMultiplier          = 1.5
	// DefaultReleaseFactor       = 0.85
)

type Buffer struct {
	currCap      *Capacity
	internal     buffer
	sync.RWMutex // Read Write mutex, guards access to internal buffer.
}

// Get returns buffer if any in the pool or creates a new buffer
func (pool *BufferPool) Get() (buf *Buffer) {
	if pool.Capacity() > 1 {
		time.Sleep(pool.NextBackOff(pool.Capacity()))
	}
	select {
	case buf = <-pool.buf:
	default:
		buf = &Buffer{currCap: pool.cap}
	}
	return
}

// Put resets the buffer and put it to the pool
func (pool *BufferPool) Put(buf *Buffer) {
	buf.Reset()
	if pool.Capacity() < 1 {
		pool.Reset()
	}
	select {
	case pool.buf <- buf:
	default:
	}
}

// Write writes to the buffer
func (buf *Buffer) Write(p []byte) (int, error) {
	buf.Lock()
	defer buf.Unlock()
	off, err := buf.internal.allocate(uint32(len(p)))
	if err != nil {
		return 0, err
	}
	if _, err := buf.internal.writeAt(p, off); err != nil {
		return 0, err
	}
	buf.currCap.size += int64(len(p))
	return len(p), nil
}

// Bytes gets data from internal buffer
func (buf *Buffer) Bytes() []byte {
	buf.RLock()
	defer buf.RUnlock()
	data, _ := buf.internal.bytes()
	return data
}

// Reset resets the buffer
func (buf *Buffer) Reset() (ok bool) {
	buf.Lock()
	defer buf.Unlock()
	buf.currCap.size -= buf.internal.size
	return buf.internal.reset()
}

// Size internal buffer size
func (buf *Buffer) Size() int64 {
	buf.RLock()
	defer buf.RUnlock()
	return buf.internal.size
}

// BufferPool represents the thread safe buffer pool.
// All BufferPool methods are safe for concurrent use by multiple goroutines.
type (
	Capacity struct {
		size       int64
		targetSize int64
	}
	BufferPool struct {
		sync.RWMutex
		buf chan *Buffer

		// Capacity
		cap                 *Capacity
		InitialInterval     time.Duration
		RandomizationFactor float64
		currentInterval     time.Duration

		// close
		closeC chan struct{}
	}
)

// NewBufferPool creates a new buffer pool.
func NewBufferPool(size int64) *BufferPool {
	if size > maxBufferSize {
		size = maxBufferSize
	}

	pool := &BufferPool{
		buf: make(chan *Buffer, maxPoolSize),

		// Capacity
		cap:                 &Capacity{targetSize: size},
		InitialInterval:     DefaultInitialInternal,
		RandomizationFactor: DefaultRandomizationFactor,

		// close
		closeC: make(chan struct{}, 1),
	}

	pool.Reset()
	go pool.drain()

	return pool
}

func (pool *BufferPool) Capacity() float64 {
	pool.RLock()
	defer pool.RUnlock()
	return float64(pool.cap.size) / float64(pool.cap.targetSize)
}

// Reset the interval back to the initial interval.
// Reset must be called before using pool.
func (pool *BufferPool) Reset() {
	pool.currentInterval = pool.InitialInterval
}

// NextBackOff calculates the next backoff interval using the formula:
// 	Randomized interval = RetryInterval * (1 ± RandomizationFactor)
func (pool *BufferPool) NextBackOff(multiplier float64) time.Duration {
	defer pool.incrementCurrentInterval(multiplier)
	return getRandomValueFromInterval(pool.RandomizationFactor, rand.Float64(), pool.currentInterval)
}

// Increments the current interval by multiplying it with the multiplier.
func (pool *BufferPool) incrementCurrentInterval(multiplier float64) {
	pool.currentInterval = time.Duration(float64(pool.currentInterval) * multiplier)
}

// Decrements the current interval by diving it with factor.
func (pool *BufferPool) decrementCurrentInterval(multiplier float64) {
	pool.currentInterval = time.Duration(float64(pool.currentInterval) * multiplier)
}

// Returns a random value from the following interval:
// [currentInterval - randomizationFactor * currentInterval, currentInterval + randomizationFactor * currentInterval].
func getRandomValueFromInterval(randomizationFactor, random float64, currentInterval time.Duration) time.Duration {
	var delta = randomizationFactor * float64(currentInterval)
	var minInterval = float64(currentInterval) - delta
	var maxInterval = float64(currentInterval) + delta

	// Get a random value from the range [minInterval, maxInterval].
	// The formula used below has a +1 because if the minInterval is 1 and the maxInterval is 3 then
	// we want a 33% chance for selecting either 1, 2 or 3.
	return time.Duration(minInterval + (random * (maxInterval - minInterval + 1)))
}

func (pool *BufferPool) Done() {
	close(pool.closeC)
}

func (pool *BufferPool) drain() {
	ticker := time.NewTicker(1 * time.Minute)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case <-ticker.C:
			select {
			case <-pool.closeC:
				return
			case <-pool.buf:
			default:
			}
		}
	}
}
