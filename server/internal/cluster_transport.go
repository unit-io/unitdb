/*
 * Copyright 2026 Saffat Technologies, Ltd.
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

package internal

// Node-to-node calls: an independent implementation of
// docs/design/cluster-spec.md, section 1.5, over the peerwire protocol.
//
// Each node dials every peer and keeps one connection to it for its own
// calls; the peer dials back for its calls. A connection starts with a hello
// that names both ends; the accepting node binds the connection to the
// dialer's name (over TLS, the name its certificate gives), and every call
// on it must name that node as its sender, or is refused. Requests all
// carry a Node field for this: the check is made once, for every call, in
// dispatch.

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/unitdb/server/internal/peerwire"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
)

// minPeerProtocol is the oldest protocol a peer may speak.
const minPeerProtocol = 3

// errNotConnected is a call to a peer with no connection up: not sent.
var errNotConnected = &peerwire.CallError{Sent: false, Err: errors.New("peer not connected")}

// ClusterNode is a peer.
type ClusterNode struct {
	name       string
	address    string
	tlsAddress string
	owner      *Cluster

	// caps is what this node knows the peer can do.
	caps peerCapabilities

	// handoffMu is held while hints go to the peer, or are dropped.
	handoffMu sync.Mutex

	// repl is the peer's replication queue, sent by replicateLoop until
	// replDone closes.
	repl     chan replicaItem
	replDone chan struct{}

	linkMu   sync.Mutex
	link     *peerwire.Link
	dialing  chan struct{}
	lastFail time.Time
	// incarnation is the peer's run last seen in a hello.
	incarnation int64

	// callsFailed counts calls to the peer that failed in transport.
	callsFailed atomic.Uint64
}

// counted counts err if it is a transport failure of a call (not sent,
// timed out, or lost with its connection), not the peer's own answer.
func (n *ClusterNode) counted(err error) error {
	if err != nil && !peerwire.Answered(err) {
		n.callsFailed.Add(1)
	}
	return err
}

// notSent reports whether err is a call that certainly did not reach the
// peer.
func notSent(err error) bool {
	return peerwire.NotSent(err)
}

// retryable reports whether a request was not processed: not sent, or not
// taken by the receiver's ring. It may be sent again.
func retryable(err error) bool {
	return notSent(err) || errors.Is(err, errRejected)
}

// currentLink returns the peer's connection if it is up.
func (n *ClusterNode) currentLink() *peerwire.Link {
	n.linkMu.Lock()
	defer n.linkMu.Unlock()
	if n.link != nil && n.link.Alive() {
		return n.link
	}
	return nil
}

// closeLink closes the connection to the peer.
func (n *ClusterNode) closeLink() {
	n.linkMu.Lock()
	l := n.link
	n.linkMu.Unlock()
	if l != nil {
		l.Close()
	}
}

// redial returns the connection to the peer, dialing it if it is down. With
// wait unset, as for a call, it does not wait for a dial another goroutine
// makes, nor dial again within reconnectEvery of a failed dial: the call
// fails at once as not sent.
func (n *ClusterNode) redial(wait bool) (*peerwire.Link, error) {
	n.linkMu.Lock()
	if l := n.link; l != nil && l.Alive() {
		n.linkMu.Unlock()
		return l, nil
	}
	if n.dialing != nil {
		ch := n.dialing
		n.linkMu.Unlock()
		if !wait {
			return nil, errNotConnected
		}
		<-ch
		if l := n.currentLink(); l != nil {
			return l, nil
		}
		return nil, errNotConnected
	}
	if n.owner == nil || n.owner.stopped.Load() || (!wait && time.Since(n.lastFail) < reconnectEvery) {
		n.linkMu.Unlock()
		return nil, errNotConnected
	}
	ch := make(chan struct{})
	n.dialing = ch
	n.linkMu.Unlock()

	l, err := n.open()

	n.linkMu.Lock()
	n.dialing = nil
	if err == nil {
		n.link = l
	} else {
		n.lastFail = time.Now()
	}
	n.linkMu.Unlock()
	close(ch)
	if err != nil {
		return nil, &peerwire.CallError{Sent: false, Err: err}
	}
	if c := n.owner; c.started.Load() {
		go c.linkUp(n)
	}
	return l, nil
}

// open dials the peer and says hello.
func (n *ClusterNode) open() (*peerwire.Link, error) {
	c := n.owner
	conn, err := n.dial()
	if err != nil {
		return nil, err
	}
	l, err := peerwire.Dial(conn, c.hello(n.name), dialTimeout)
	if err != nil {
		var re *peerwire.RemoteError
		if errors.As(err, &re) && strings.Contains(re.Message, "protocol") {
			c.incompatible(n, fmt.Errorf("it refused this node: %s", re.Message))
		}
		return nil, err
	}
	if l.Peer.Protocol < minPeerProtocol || l.Peer.From != n.name {
		l.Close()
		err := fmt.Errorf("it speaks cluster protocol %d as %q; this node needs %d or later from %q", l.Peer.Protocol, l.Peer.From, minPeerProtocol, n.name)
		c.incompatible(n, err)
		return nil, err
	}
	n.learn(&l.Peer)
	return l, nil
}

// incompatible reports a peer this node can't work with. While Start dials
// the peers the first time, it stops this node: a cluster must run one
// build (or compatible ones).
func (c *Cluster) incompatible(n *ClusterNode, err error) {
	if c.starting.Load() {
		log.Fatal("cluster.Start", fmt.Sprintf("node %s runs an incompatible cluster protocol: start every node on this build", n.name), err)
	}
	log.ErrLogger.Error().Err(err).Str("peer", n.name).Msg("cluster: peer runs an incompatible cluster protocol")
}

// learn takes what a peer's hello says: what it can do, and whether it is
// a new run of it.
func (n *ClusterNode) learn(h *peerwire.Hello) {
	var nc NodeCapabilities
	if len(h.Info) > 0 && peerwire.Decode(h.Info, &nc) == nil {
		n.setCapabilities(nc)
	}
	n.linkMu.Lock()
	prev := n.incarnation
	n.incarnation = h.Incarnation
	n.linkMu.Unlock()
	if prev != 0 && prev != h.Incarnation && n.owner != nil {
		n.owner.peerRestarted(n)
	}
}

// hello is what this node says to node to.
func (c *Cluster) hello(to string) peerwire.Hello {
	info, _ := peerwire.Encode(ownNodeCapabilities())
	return peerwire.Hello{Protocol: clusterProtocolVersion, From: c.thisNodeName, To: to, Incarnation: c.incarnation, Info: info}
}

// reconnectLoop keeps a connection to the peer up until shutdown.
func (n *ClusterNode) reconnectLoop() {
	c := n.owner
	for !c.stopped.Load() {
		l, err := n.redial(true)
		if err != nil {
			select {
			case <-c.quit:
				return
			case <-time.After(reconnectEvery):
			}
			continue
		}
		select {
		case <-c.quit:
			return
		case <-l.Done():
			log.ErrLogger.Info().Err(l.Err()).Str("peer", n.name).Msg("cluster: connection to peer lost")
		}
	}
}

// linkUp catches a peer up once a connection to it is up: its clients'
// subscriptions are sent to it again, it gets its hints and the security
// state.
func (c *Cluster) linkUp(n *ClusterNode) {
	c.handoff(n)
	c.pushRevocations(n)
	c.rebalance(map[string]bool{n.name: true})
}

// call calls the peer, waiting for its answer as long as the connection
// lasts.
func (n *ClusterNode) call(method string, req, resp interface{}) error {
	return n.callTimeout(method, req, resp, 0)
}

// callTimeout calls the peer and waits up to d for its answer (no limit if
// 0). A call that timed out may still complete on the peer. A connection
// found down is dialed again first, so that the first call after the peer
// restarted reaches it; a call that failed after it was sent is not sent
// again. Transport failures count in unitdb_cluster_peer_calls_failed_total.
func (n *ClusterNode) callTimeout(method string, req, resp interface{}, d time.Duration) error {
	l, err := n.redial(false)
	if err != nil {
		return n.counted(err)
	}
	return n.counted(l.Call(method, req, resp, d))
}

// goCall calls the peer without waiting; done gets the outcome.
func (n *ClusterNode) goCall(method string, req, resp interface{}, d time.Duration, done func(error)) {
	l, err := n.redial(false)
	if err != nil {
		done(n.counted(err))
		return
	}
	l.Go(method, req, resp, d, func(err error) { done(n.counted(err)) })
}

// oneShot calls the peer on a connection of its own, closed after: for
// calls before the cluster starts. answered is false if the peer could not
// be reached.
func (n *ClusterNode) oneShot(method string, req, resp interface{}, d time.Duration) (answered bool, err error) {
	c := n.owner
	if c == nil {
		return false, errNotConnected
	}
	conn, err := n.dial()
	if err != nil {
		return false, err
	}
	l, err := peerwire.Dial(conn, c.hello(n.name), dialTimeout)
	if err != nil {
		return peerwire.Answered(err), err
	}
	defer l.Close()
	n.learn(&l.Peer)
	err = l.Call(method, req, resp, d)
	if errors.Is(err, peerwire.ErrTimeout) {
		return true, fmt.Errorf("no answer in %s", d)
	}
	return err == nil || peerwire.Answered(err), err
}

// servePlain accepts cluster connections on the plain listener.
func (c *Cluster) servePlain(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if c.stopped.Load() {
				return
			}
			log.ErrLogger.Error().Err(err).Str("context", "cluster.servePlain").Msg("accept")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		go peerwire.Serve(conn, c.acceptPeer(""))
	}
}

// acceptPeer decides on a dialer's hello. certName is the node a TLS
// connection's certificate names, "" on the plain listener.
func (c *Cluster) acceptPeer(certName string) peerwire.Accept {
	return func(h *peerwire.Hello) (peerwire.Hello, peerwire.Dispatcher, error) {
		answer := c.hello(h.From)
		switch {
		case c.stopped.Load():
			return answer, nil, errors.New("cluster: this node is stopping")
		case h.Protocol < minPeerProtocol:
			return answer, nil, fmt.Errorf("cluster: protocol %d is too old; this node needs %d or later", h.Protocol, minPeerProtocol)
		case h.To != c.thisNodeName:
			return answer, nil, fmt.Errorf("cluster: this is node %q, not %q", c.thisNodeName, h.To)
		case certName != "" && h.From != certName:
			return answer, nil, fmt.Errorf("cluster: a connection from %s names %q as its sender", certName, h.From)
		}
		n := c.nodes[h.From]
		if n == nil {
			log.ErrLogger.Error().Str("from", h.From).Msg("cluster: refused a connection from a node not configured")
			return answer, nil, fmt.Errorf("cluster: %q is not a peer of %q", h.From, c.thisNodeName)
		}
		n.learn(h)
		return answer, c.dispatcher(h.From, c.peerMethods()), nil
	}
}

// peerMethod is a call peers make: how to read its request, and serve it.
type peerMethod struct {
	newReq func() interface{}
	serve  func(req interface{}) (interface{}, error)
	// inline, if set, serves the request on the connection's read loop:
	// for requests that must be taken in arrival order. It replies itself.
	inline func(req interface{}, reply func(interface{}, error))
}

// method is a peerMethod served by f, net/rpc style.
func method[Req any, Resp any](f func(*Req, *Resp) error) peerMethod {
	return peerMethod{
		newReq: func() interface{} { return new(Req) },
		serve: func(req interface{}) (interface{}, error) {
			resp := new(Resp)
			err := f(req.(*Req), resp)
			return resp, err
		},
	}
}

// peerMethods are the calls this node serves. Every request type has a
// Node field naming its sender.
func (c *Cluster) peerMethods() map[string]peerMethod {
	return map[string]peerMethod{
		// Membership (cluster_membership.go).
		"Cluster.Heartbeat": method(c.Heartbeat),
		"Cluster.Vote":      method(c.Vote),
		"Cluster.Leave":     method(c.Leave),
		// Routing and delivery (cluster_routing.go).
		"Cluster.Forward":  {newReq: func() interface{} { return new(ForwardReq) }, inline: c.serveForward},
		"Cluster.ToClient": method(c.ToClient),
		"Cluster.Deliver":  method(c.Deliver),
		"Cluster.Resync":   method(c.Resync),
		// Replication, rebuild and sessions (cluster_replication.go,
		// cluster_sessions.go).
		"Cluster.Replicate":      method(c.Replicate),
		"Cluster.RebuildTopics":  method(c.RebuildTopics),
		"Cluster.RebuildHistory": method(c.RebuildHistory),
		"Cluster.FetchSession":   method(c.FetchSession),
		"Cluster.ForgetSession":  method(c.ForgetSession),
		// Served for other files.
		"Cluster.Revocations":     method(c.Revocations),
		"Cluster.StartedFrom":     method(c.StartedFrom),
		"Cluster.ReconcileTopics": method(c.ReconcileTopics),
		"Cluster.Digests":         method(c.Digests),
		"Cluster.Reconcile":       method(c.Reconcile),
	}
}

// errNoMethod is the answer for a call this node does not serve.
func errNoMethod(name string) error {
	return &peerwire.RemoteError{Code: peerwire.CodeNoMethod, Message: "cluster: no method " + name}
}

// dispatcher serves the calls of a connection bound to node peer. Each
// request must name peer as its sender.
func (c *Cluster) dispatcher(peer string, methods map[string]peerMethod) peerwire.Dispatcher {
	return func(name string, body []byte, reply func(interface{}, error)) {
		m, ok := methods[name]
		if !ok {
			reply(nil, errNoMethod(name))
			return
		}
		req := m.newReq()
		if err := peerwire.Decode(body, req); err != nil {
			reply(nil, fmt.Errorf("cluster: unreadable %s request: %v", name, err))
			return
		}
		if err := checkSender(peer, req); err != nil {
			log.ErrLogger.Warn().Err(err).Str("method", name).Msg("cluster: refused a call")
			reply(nil, err)
			return
		}
		if m.inline != nil {
			m.inline(req, reply)
			return
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.ErrLogger.Error().Str("method", name).Msgf("cluster: panic serving a call: %v\n%s", r, debug.Stack())
					reply(nil, fmt.Errorf("cluster: %s failed", name))
				}
			}()
			resp, err := m.serve(req)
			reply(resp, err)
		}()
	}
}

// checkSender refuses a request that does not name peer, the node of its
// connection, as its sender.
func checkSender(peer string, req interface{}) error {
	v := reflect.ValueOf(req)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	f := v.FieldByName("Node")
	if !f.IsValid() || f.Kind() != reflect.String {
		return &peerwire.RemoteError{Code: peerwire.CodeSender, Message: fmt.Sprintf("cluster: a call from %s names no sender", peer)}
	}
	if f.String() != peer {
		return &peerwire.RemoteError{Code: peerwire.CodeSender, Message: fmt.Sprintf("cluster: a call from %s names %q as its sender", peer, f.String())}
	}
	return nil
}
