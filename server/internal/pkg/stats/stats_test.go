package stats

import (
	"net"
	"testing"
	"time"
)

func TestUnregisterWithoutServer(t *testing.T) {
	// A UDP port nobody listens on: writes fail and the transport waits to retry.
	l, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.LocalAddr().String()
	l.Close()

	s := New(&Config{Addr: addr, Size: 50}, FlushInterval(10*time.Millisecond), RetryTimeout(time.Minute))
	for i := 0; i < 20; i++ {
		s.Incr("test", 1)
		time.Sleep(5 * time.Millisecond)
	}

	done := make(chan struct{})
	go func() {
		s.Unregister()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Unregister waited for the retry timeout")
	}
}
