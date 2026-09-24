package internal

import (
	"io"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingService counts the calls it receives. Slow blocks until released.
type countingService struct {
	calls   int32
	started chan struct{}
	release chan struct{}
}

func (s *countingService) Fast(_ int, _ *int) error {
	atomic.AddInt32(&s.calls, 1)
	return nil
}

func (s *countingService) Slow(_ int, _ *int) error {
	atomic.AddInt32(&s.calls, 1)
	s.started <- struct{}{}
	<-s.release
	return nil
}

// rpcNode serves countingService on a fixed address and can be restarted
// there, modelling a cluster node process going away and coming back.
type rpcNode struct {
	t    *testing.T
	addr string
	svc  *countingService

	mu    sync.Mutex
	l     net.Listener
	conns []net.Conn
}

func startRPCNode(t *testing.T) *rpcNode {
	n := &rpcNode{t: t, addr: "127.0.0.1:0", svc: &countingService{started: make(chan struct{}, 1), release: make(chan struct{})}}
	n.start()
	n.addr = n.l.Addr().String()
	t.Cleanup(n.stop)
	return n
}

func (n *rpcNode) start() {
	l, err := net.Listen("tcp", n.addr)
	if err != nil {
		n.t.Fatal(err)
	}
	srv := rpc.NewServer()
	if err := srv.RegisterName("Test", n.svc); err != nil {
		n.t.Fatal(err)
	}
	n.mu.Lock()
	n.l = l
	n.mu.Unlock()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			n.mu.Lock()
			n.conns = append(n.conns, c)
			n.mu.Unlock()
			go srv.ServeConn(c)
		}
	}()
}

func (n *rpcNode) stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.l.Close()
	for _, c := range n.conns {
		c.Close()
	}
	n.conns = nil
}

func connectedNode(t *testing.T, addr string) *ClusterNode {
	endpoint, conn, err := dialNode(addr)
	if err != nil {
		t.Fatal(err)
	}
	n := &ClusterNode{name: "peer", address: addr, endpoint: endpoint, conn: conn, connected: true, done: make(chan bool, 1)}
	t.Cleanup(func() { n.done <- true; endpoint.Close() })
	return n
}

// TestClusterCallAfterRestart checks the first call after the node restarted
// while the connection was idle reaches the node, exactly once.
func TestClusterCallAfterRestart(t *testing.T) {
	srv := startRPCNode(t)
	node := connectedNode(t, srv.addr)
	var unused int
	if err := node.call("Test.Fast", 0, &unused); err != nil {
		t.Fatal(err)
	}

	srv.stop()
	srv.start()
	time.Sleep(100 * time.Millisecond) // let the client see its connection close

	if err := node.call("Test.Fast", 0, &unused); err != nil {
		t.Fatalf("first call after restart: %v", err)
	}
	if got := atomic.LoadInt32(&srv.svc.calls); got != 2 {
		t.Fatalf("node received %d calls; want 2", got)
	}
}

// eofConn reports io.EOF from reads once closed, as when the node's side of
// the connection ends at the moment the client is closed.
type eofConn struct {
	net.Conn
	closed int32
}

func (c *eofConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil && atomic.LoadInt32(&c.closed) == 1 {
		err = io.EOF
	}
	return n, err
}

func (c *eofConn) Close() error {
	atomic.StoreInt32(&c.closed, 1)
	return c.Conn.Close()
}

// TestClusterCallNotRepeated checks a call in flight when the client is closed
// fails without being sent again: the node may already have processed it.
// net/rpc fails such a call with ErrShutdown, the same error it returns for a
// call made on an already shut down client, which was never sent.
func TestClusterCallNotRepeated(t *testing.T) {
	srv := startRPCNode(t)
	raw, err := net.Dial("tcp", srv.addr)
	if err != nil {
		t.Fatal(err)
	}
	conn := &watchedConn{Conn: &eofConn{Conn: raw}}
	endpoint := rpc.NewClient(conn)
	node := &ClusterNode{name: "peer", address: srv.addr, endpoint: endpoint, conn: conn, connected: true, done: make(chan bool, 1)}
	t.Cleanup(func() { node.done <- true })

	errc := make(chan error, 1)
	go func() {
		var unused int
		errc <- node.call("Test.Slow", 0, &unused)
	}()
	<-srv.svc.started
	endpoint.Close() // as another failed call or reconnect would
	close(srv.svc.release)
	if err := <-errc; err == nil {
		t.Fatal("call on a closed client succeeded")
	}
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt32(&srv.svc.calls); got != 1 {
		t.Fatalf("node received the call %d times; want 1", got)
	}
}
