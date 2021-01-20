// Package Meter yields summarized data
// describing a series of timed events.
package stats

import (
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds Meter initialization
// parameters. Size defines the sample capacity.
// Meter is thread safe.
type Config struct {
	Size  int
	HBins int // Histogram bins.
	Addr  string
}

// Meter holds event durations
// and counts.
type Stats struct {
	sync.Mutex
	trans        *transport
	Count        uint64
	metricPrefix string
	defaultTags  []Tag
}

type transport struct {
	maxPacketSize int
	tagFormat     *TagFormat

	bufPool   chan []byte
	buf       []byte
	bufSize   int
	bufLock   sync.Mutex
	sendQueue chan []byte

	shutdown     chan struct{}
	shutdownOnce sync.Once
	shutdownWg   sync.WaitGroup

	lostPacketsPeriod  int64
	lostPacketsOverall int64
}

// New initializes a new Meter.
func New(c *Config, options ...Option) *Stats {
	opts := ClientOptions{
		Addr:              c.Addr,
		MetricPrefix:      DefaultMetricPrefix,
		MaxPacketSize:     DefaultMaxPacketSize,
		FlushInterval:     DefaultFlushInterval,
		ReconnectInterval: DefaultReconnectInterval,
		ReportInterval:    DefaultReportInterval,
		RetryTimeout:      DefaultRetryTimeout,
		Logger:            log.New(os.Stderr, DefaultLogPrefix, log.LstdFlags),
		BufPoolCapacity:   DefaultBufPoolCapacity,
		SendQueueCapacity: DefaultSendQueueCapacity,
		SendLoopCount:     DefaultSendLoopCount,
		TagFormat:         TagFormatInfluxDB,
	}

	s := &Stats{
		trans: &transport{
			shutdown: make(chan struct{}),
		},
	}

	// 1024 is room for overflow metric
	s.trans.bufSize = opts.MaxPacketSize + 1024

	for _, option := range options {
		option(&opts)
	}

	s.metricPrefix = opts.MetricPrefix
	s.defaultTags = opts.DefaultTags

	s.trans.tagFormat = opts.TagFormat
	s.trans.maxPacketSize = opts.MaxPacketSize
	s.trans.buf = make([]byte, 0, s.trans.bufSize)
	s.trans.bufPool = make(chan []byte, opts.BufPoolCapacity)
	s.trans.sendQueue = make(chan []byte, opts.SendQueueCapacity)

	go s.trans.flushLoop(opts.FlushInterval)

	for i := 0; i < opts.SendLoopCount; i++ {
		s.trans.shutdownWg.Add(1)
		go s.trans.sendLoop(opts.Addr, opts.ReconnectInterval, opts.RetryTimeout, opts.Logger)
	}

	if opts.ReportInterval > 0 {
		s.trans.shutdownWg.Add(1)
		go s.trans.reportLoop(opts.ReportInterval, opts.Logger)
	}

	return s
}

// flushLoop makes sure metrics are flushed every flushInterval
func (t *transport) flushLoop(flushInterval time.Duration) {
	var flushC <-chan time.Time

	if flushInterval > 0 {
		flushTicker := time.NewTicker(flushInterval)
		defer flushTicker.Stop()
		flushC = flushTicker.C
	}

	for {
		select {
		case <-t.shutdown:
			t.bufLock.Lock()
			if len(t.buf) > 0 {
				t.flushBuf(len(t.buf))
			}
			t.bufLock.Unlock()

			close(t.sendQueue)
			return
		case <-flushC:
			t.bufLock.Lock()
			if len(t.buf) > 0 {
				t.flushBuf(len(t.buf))
			}
			t.bufLock.Unlock()
		}
	}
}

// sendLoop handles packet delivery over UDP and periodic reconnects
func (t *transport) sendLoop(addr string, reconnectInterval, retryTimeout time.Duration, log SomeLogger) {
	var (
		sock       net.Conn
		err        error
		reconnectC <-chan time.Time
	)

	if reconnectInterval > 0 {
		reconnectTicker := time.NewTicker(reconnectInterval)
		defer reconnectTicker.Stop()
		reconnectC = reconnectTicker.C
	}

RECONNECT:
	// Attempt to connect
	sock, err = net.Dial("udp", addr)
	if err != nil {
		log.Printf("[STATSD] Error connecting to server: %s", err)
		goto WAIT
	}

	for {
		select {
		case buf, ok := <-t.sendQueue:
			// Get a buffer from the queue
			if !ok {
				_ = sock.Close() // nolint: gosec
				t.shutdownWg.Done()
				return
			}

			if len(buf) > 0 {
				// cut off \n in the end
				_, err := sock.Write(buf[0 : len(buf)-1])
				if err != nil {
					log.Printf("[STATSD] Error writing to socket: %s", err)
					_ = sock.Close() // nolint: gosec
					goto WAIT
				}
			}

			// return buffer to the pool
			select {
			case t.bufPool <- buf:
			default:
				// pool is full, let GC handle the buf
			}
		case <-reconnectC:
			_ = sock.Close() // nolint: gosec
			goto RECONNECT
		}
	}

WAIT:
	// Wait for a while
	time.Sleep(retryTimeout)
	goto RECONNECT
}

// reportLoop reports periodically number of packets lost
func (t *transport) reportLoop(reportInterval time.Duration, log SomeLogger) {
	defer t.shutdownWg.Done()

	reportTicker := time.NewTicker(reportInterval)
	defer reportTicker.Stop()

	for {
		select {
		case <-t.shutdown:
			return
		case <-reportTicker.C:
			lostPeriod := atomic.SwapInt64(&t.lostPacketsPeriod, 0)
			if lostPeriod > 0 {
				log.Printf("[STATSD] %d packets lost (overflow)", lostPeriod)
			}
		}
	}
}

func (s *Stats) Unregister() {
	s.Lock()
	s.trans.close()
	s.Unlock()
}

func (t *transport) close() {
	t.shutdownOnce.Do(func() {
		close(t.shutdown)
	})
	t.shutdownWg.Wait()
}

// Incr increments a counter metric
//
// Often used to note a particular event, for example incoming web request.
func (s *Stats) Incr(stat string, count int64, tags ...Tag) {
	if 0 != count {
		s.trans.bufLock.Lock()
		lastLen := len(s.trans.buf)

		s.trans.buf = append(s.trans.buf, []byte(s.metricPrefix)...)
		s.trans.buf = append(s.trans.buf, []byte(stat)...)
		if s.trans.tagFormat.Placement == TagPlacementName {
			s.trans.buf = s.formatTags(s.trans.buf, tags)
		}
		s.trans.buf = append(s.trans.buf, ':')
		s.trans.buf = strconv.AppendInt(s.trans.buf, count, 10)
		s.trans.buf = append(s.trans.buf, []byte("|c")...)
		if s.trans.tagFormat.Placement == TagPlacementSuffix {
			s.trans.buf = s.formatTags(s.trans.buf, tags)
		}
		s.trans.buf = append(s.trans.buf, '\n')

		s.trans.checkBuf(lastLen)
		s.trans.bufLock.Unlock()
	}
}

// Decr decrements a counter metri
//
// Often used to note a particular event
func (s *Stats) Decr(stat string, count int64, tags ...Tag) {
	s.Incr(stat, -count, tags...)
}

// Timing tracks a duration event, the time delta must be given in milliseconds
func (s *Stats) Timing(stat string, delta int64, tags ...Tag) {
	s.trans.bufLock.Lock()
	lastLen := len(s.trans.buf)

	s.trans.buf = append(s.trans.buf, []byte(s.metricPrefix)...)
	s.trans.buf = append(s.trans.buf, []byte(stat)...)
	if s.trans.tagFormat.Placement == TagPlacementName {
		s.trans.buf = s.formatTags(s.trans.buf, tags)
	}
	s.trans.buf = append(s.trans.buf, ':')
	s.trans.buf = strconv.AppendInt(s.trans.buf, delta, 10)
	s.trans.buf = append(s.trans.buf, []byte("|ms")...)
	if s.trans.tagFormat.Placement == TagPlacementSuffix {
		s.trans.buf = s.formatTags(s.trans.buf, tags)
	}
	s.trans.buf = append(s.trans.buf, '\n')

	s.trans.checkBuf(lastLen)
	s.trans.bufLock.Unlock()
}

// PrecisionTiming track a duration event, the time delta has to be a duration
//
// Usually request processing time, time to run database query, etc. are used with
// this metric type.
func (s *Stats) PrecisionTiming(stat string, delta time.Duration, tags ...Tag) {
	s.trans.bufLock.Lock()
	lastLen := len(s.trans.buf)

	s.trans.buf = append(s.trans.buf, []byte(s.metricPrefix)...)
	if s.trans.tagFormat.Placement == TagPlacementName {
		s.trans.buf = s.formatTags(s.trans.buf, tags)
	}
	s.trans.buf = append(s.trans.buf, ' ')
	s.trans.buf = append(s.trans.buf, []byte(stat)...)
	s.trans.buf = append(s.trans.buf, s.trans.tagFormat.KeyValueSeparator)
	s.trans.buf = strconv.AppendFloat(s.trans.buf, float64(delta), 'f', -1, 64)
	//s.trans.buf = append(s.trans.buf, []byte("|ms")...)
	if s.trans.tagFormat.Placement == TagPlacementSuffix {
		s.trans.buf = s.formatTags(s.trans.buf, tags)
	}
	s.trans.buf = append(s.trans.buf, '\n')

	s.trans.checkBuf(lastLen)
	s.trans.bufLock.Unlock()
}

// SetAdd adds unique element to a set
//
// Statsd server will provide cardinality of the set over aggregation period.
func (s *Stats) SetAdd(stat string, value string, tags ...Tag) {
	s.trans.bufLock.Lock()
	lastLen := len(s.trans.buf)

	s.trans.buf = append(s.trans.buf, []byte(s.metricPrefix)...)
	s.trans.buf = append(s.trans.buf, []byte(stat)...)
	if s.trans.tagFormat.Placement == TagPlacementName {
		s.trans.buf = s.formatTags(s.trans.buf, tags)
	}
	s.trans.buf = append(s.trans.buf, ':')
	s.trans.buf = append(s.trans.buf, []byte(value)...)
	s.trans.buf = append(s.trans.buf, []byte("|s")...)
	if s.trans.tagFormat.Placement == TagPlacementSuffix {
		s.trans.buf = s.formatTags(s.trans.buf, tags)
	}
	s.trans.buf = append(s.trans.buf, '\n')

	s.trans.checkBuf(lastLen)
	s.trans.bufLock.Unlock()
}
