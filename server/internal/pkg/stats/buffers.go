package stats

import "sync/atomic"

// checkBuf checks current buffer for overflow, and flushes buffer up to lastLen bytes on overflow
//
// overflow part is preserved in flushBuf
func (t *transport) checkBuf(lastLen int) {
	if len(t.buf) > t.maxPacketSize {
		t.flushBuf(lastLen)
	}
}

// flushBuf sends buffer to the queue and initializes new buffer
func (t *transport) flushBuf(length int) {
	sendBuf := t.buf[0:length]
	tail := t.buf[length:len(t.buf)]

	// get new buffer
	select {
	case t.buf = <-t.bufPool:
		t.buf = t.buf[0:0]
	default:
		t.buf = make([]byte, 0, t.bufSize)
	}

	// copy tail to the new buffer
	t.buf = append(t.buf, tail...)

	// flush current buffer
	select {
	case t.sendQueue <- sendBuf:
	default:
		// flush failed, we lost some data
		atomic.AddInt64(&t.lostPacketsPeriod, 1)
		atomic.AddInt64(&t.lostPacketsOverall, 1)
	}

}
