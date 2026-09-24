package internal

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/utp"
)

// TestBatchManagerConcurrentAdd adds messages from several goroutines while
// the publish loop flushes batches; run it with -race.
func TestBatchManagerConcurrentAdd(t *testing.T) {
	c := &_Conn{
		pub:        make(chan *utp.Publish, 1024),
		MessageIds: message.NewMessageIds(),
	}
	c.newBatchManager(&batchOptions{batchDuration: 10 * time.Millisecond, batchCountThreshold: 5, batchByteThreshold: 1 << 20})

	const writers, perWriter = 4, 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				c.batchManager.add(0, &utp.PublishMessage{Topic: "t", Payload: []byte(fmt.Sprintf("%d-%d", w, i))})
			}
		}(w)
	}
	wg.Wait()

	got := 0
	deadline := time.After(10 * time.Second)
	for got < writers*perWriter {
		select {
		case pub := <-c.pub:
			got += len(pub.Messages)
		case <-deadline:
			t.Fatalf("received %d of %d batched messages", got, writers*perWriter)
		}
	}
	c.batchManager.close()
}
