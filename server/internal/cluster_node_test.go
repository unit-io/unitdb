package internal

import (
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/peerwire"
)

// testReq is a test call's request: it names its sender, as every call must.
type testReq struct{ Node string }

// countingService counts the calls it receives. Slow blocks until released.
type countingService struct {
	calls   int32
	started chan struct{}
	release chan struct{}
}

func (s *countingService) Fast(_ *testReq, _ *int) error {
	atomic.AddInt32(&s.calls, 1)
	return nil
}

func (s *countingService) Slow(_ *testReq, _ *int) error {
	atomic.AddInt32(&s.calls, 1)
	s.started <- struct{}{}
	<-s.release
	return nil
}

// testPeer serves methods as a peer node on a fixed address, and can be
// restarted there, modelling a node's process going away and coming back.
type testPeer struct {
	t       *testing.T
	addr    string
	methods map[string]peerMethod

	mu    sync.Mutex
	l     net.Listener
	conns []net.Conn
}

func startTestPeer(t *testing.T, methods map[string]peerMethod) *testPeer {
	p := &testPeer{t: t, addr: "127.0.0.1:0", methods: methods}
	p.start()
	p.addr = p.l.Addr().String()
	t.Cleanup(p.stop)
	return p
}

func (p *testPeer) start() {
	l, err := net.Listen("tcp", p.addr)
	if err != nil {
		p.t.Fatal(err)
	}
	p.mu.Lock()
	p.l = l
	p.mu.Unlock()
	incarnation := time.Now().UnixNano()
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.conns = append(p.conns, conn)
			p.mu.Unlock()
			go peerwire.Serve(conn, func(h *peerwire.Hello) (peerwire.Hello, peerwire.Dispatcher, error) {
				answer := peerwire.Hello{Protocol: clusterProtocolVersion, From: h.To, To: h.From, Incarnation: incarnation}
				return answer, (&Cluster{}).dispatcher(h.From, p.methods), nil
			})
		}
	}()
}

func (p *testPeer) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.l.Close()
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
}

// testNode returns peer name at addr, of a cluster of one node, "this".
func testNode(t *testing.T, name, addr string) *ClusterNode {
	c := &Cluster{thisNodeName: "this", allNodes: []string{name, "this"}, nodes: map[string]*ClusterNode{}, quit: make(chan struct{}), departing: map[string]bool{}, proxies: map[proxyKey]*standIn{}}
	n := &ClusterNode{name: name, address: addr, owner: c}
	c.nodes[name] = n
	t.Cleanup(c.shutdown)
	return n
}

func countingPeer(t *testing.T) (*testPeer, *countingService) {
	svc := &countingService{started: make(chan struct{}, 1), release: make(chan struct{})}
	p := startTestPeer(t, map[string]peerMethod{
		"Test.Fast": method(svc.Fast),
		"Test.Slow": method(svc.Slow),
	})
	return p, svc
}

// TestClusterCallAfterRestart checks the first call after the node restarted
// while the connection was idle reaches the node, exactly once.
func TestClusterCallAfterRestart(t *testing.T) {
	srv, svc := countingPeer(t)
	node := testNode(t, "peer", srv.addr)
	var unused int
	if err := node.call("Test.Fast", &testReq{Node: "this"}, &unused); err != nil {
		t.Fatal(err)
	}

	srv.stop()
	srv.start()
	time.Sleep(100 * time.Millisecond) // let the client see its connection close

	if err := node.call("Test.Fast", &testReq{Node: "this"}, &unused); err != nil {
		t.Fatalf("first call after restart: %v", err)
	}
	if got := atomic.LoadInt32(&svc.calls); got != 2 {
		t.Fatalf("node received %d calls; want 2", got)
	}
}

// TestClusterCallNotRepeated checks a call in flight when its connection
// fails fails as sent, without being sent again: the node may already have
// processed it.
func TestClusterCallNotRepeated(t *testing.T) {
	srv, svc := countingPeer(t)
	node := testNode(t, "peer", srv.addr)

	errc := make(chan error, 1)
	go func() {
		var unused int
		errc <- node.call("Test.Slow", &testReq{Node: "this"}, &unused)
	}()
	<-svc.started
	node.closeLink() // as a transport failure would
	close(svc.release)
	err := <-errc
	if err == nil {
		t.Fatal("call on a failed connection succeeded")
	}
	if notSent(err) || retryable(err) {
		t.Fatalf("a call in flight when its connection failed was taken as not sent: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt32(&svc.calls); got != 1 {
		t.Fatalf("node received the call %d times; want 1", got)
	}
}

// TestClusterErrorAnswerKeepsConnection checks that a call the node answers
// with an error, such as a method it lacks, does not close the connection
// and fail the calls in flight on it.
func TestClusterErrorAnswerKeepsConnection(t *testing.T) {
	srv, svc := countingPeer(t)
	n := testNode(t, "peer", srv.addr)

	slow := make(chan error, 1)
	go func() {
		var r int
		slow <- n.call("Test.Slow", &testReq{Node: "this"}, &r)
	}()
	<-svc.started
	link := n.currentLink()

	var r int
	if err := n.call("Test.Missing", &testReq{Node: "this"}, &r); !missingMethod(err, "") {
		t.Fatalf("call of a missing method: %v, want no such method", err)
	}
	svc.release <- struct{}{}
	if err := <-slow; err != nil {
		t.Fatalf("a call in flight failed after another was answered with an error: %v", err)
	}
	if l := n.currentLink(); l == nil || l != link {
		t.Fatal("the connection was dropped by an error answer")
	}
	if err := n.call("Test.Fast", &testReq{Node: "this"}, &r); err != nil {
		t.Fatalf("a call after the error answer: %v", err)
	}
}

// TestCallErrorKinds checks how failed calls are told apart: not sent
// (may be sent again), sent and failed (may have been processed), and
// answered.
func TestCallErrorKinds(t *testing.T) {
	srv, svc := countingPeer(t)
	n := testNode(t, "peer", srv.addr)
	var r int

	// Answered: the connection works, the call was processed or refused.
	err := n.call("Test.Missing", &testReq{Node: "this"}, &r)
	if !peerwire.Answered(err) || notSent(err) || retryable(err) {
		t.Errorf("answer: %v: answered %v, not sent %v, retryable %v", err, peerwire.Answered(err), notSent(err), retryable(err))
	}

	// Sent and not answered in time: it may still complete.
	err = n.callTimeout("Test.Slow", &testReq{Node: "this"}, &r, 50*time.Millisecond)
	if err == nil || !errors.Is(err, peerwire.ErrTimeout) || notSent(err) || retryable(err) {
		t.Errorf("timeout: %v: not sent %v, retryable %v", err, notSent(err), retryable(err))
	}
	<-svc.started
	close(svc.release)

	// On a connection closed before: not sent.
	l := n.currentLink()
	l.Close()
	if err := l.Call("Test.Fast", &testReq{Node: "this"}, &r, 0); !notSent(err) || !retryable(err) {
		t.Errorf("call on a closed connection: %v: not sent %v", err, notSent(err))
	}

	// To a node that isn't there: not sent.
	down := testNode(t, "down", "127.0.0.1:1")
	if err := down.call("Test.Fast", &testReq{Node: "this"}, &r); !notSent(err) || !retryable(err) {
		t.Errorf("call to a node that is down: %v: not sent %v", err, notSent(err))
	}
	// Rejected by the receiver's ring: not processed.
	if !retryable(errRejected) || notSent(errRejected) {
		t.Error("a rejected request should be retryable, as sent")
	}
	if retryable(nil) || notSent(nil) {
		t.Error("no error is neither")
	}
}

// TestPeerHelloNamesBothEnds checks that a connection is refused when its
// hello names the wrong node, or a sender its certificate does not name.
func TestPeerHelloNamesBothEnds(t *testing.T) {
	c := &Cluster{thisNodeName: "one", allNodes: []string{"one", "three", "two"}, nodes: map[string]*ClusterNode{}}
	for _, name := range []string{"two", "three"} {
		c.nodes[name] = &ClusterNode{name: name, owner: c}
	}
	for _, tc := range []struct {
		name, cert string
		hello      peerwire.Hello
		refused    string
	}{
		{"plain", "", peerwire.Hello{Protocol: clusterProtocolVersion, From: "two", To: "one"}, ""},
		{"own certificate", "two", peerwire.Hello{Protocol: clusterProtocolVersion, From: "two", To: "one"}, ""},
		{"another node's name", "two", peerwire.Hello{Protocol: clusterProtocolVersion, From: "three", To: "one"}, `names "three"`},
		{"not a node", "", peerwire.Hello{Protocol: clusterProtocolVersion, From: "intruder", To: "one"}, "not a peer"},
		{"another node", "", peerwire.Hello{Protocol: clusterProtocolVersion, From: "two", To: "three"}, "not"},
		{"old protocol", "", peerwire.Hello{Protocol: 2, From: "two", To: "one"}, "protocol"},
	} {
		_, d, err := c.acceptPeer(tc.cert)(&tc.hello)
		switch {
		case tc.refused == "" && (err != nil || d == nil):
			t.Errorf("%s: refused: %v", tc.name, err)
		case tc.refused != "" && (err == nil || !strings.Contains(err.Error(), tc.refused)):
			t.Errorf("%s: %v, want it refused (%s)", tc.name, err, tc.refused)
		}
	}
}
