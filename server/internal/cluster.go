/*
 * Copyright 2020 Saffat Technologies, Ltd.
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

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/message/security"
	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/net/listener"
	rh "github.com/unit-io/unitdb/server/internal/pkg/hash"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/utp"
)

const (
	// Default timeout before attempting to reconnect to a node
	defaultClusterReconnect = 200 * time.Millisecond
	// Number of points each node has on the ring: with 160, each node of 3 to
	// 5 owns within about 10% of an even share of the keys; with 20, up to
	// 30% off.
	clusterHashReplicas = 160
	// Time a starting node waits for the others to send it their clients'
	// subscriptions (resyncOnStart). A move they retry takes up to
	// rebalanceAttempts * rebalanceRetry.
	startResyncTimeout = 3 * time.Second
	// Time between attempts to move subscriptions after a rehash: the other
	// nodes rehash a few heartbeats after the leader, and reject a move
	// until then.
	rebalanceRetry = 200 * time.Millisecond
	// Attempts to move subscriptions after a rehash.
	rebalanceAttempts = 10
	// Default number of nodes storing each message: its topic's owner and the
	// next nodes on the ring.
	defaultClusterReplicas = 2
	// Messages queued for a replica before new ones are dropped.
	replicationQueueSize = 4096
	// Messages sent to a replica in one call.
	replicationBatchSize = 256
	// Time a connecting client waits for the other nodes' copies of its session.
	fetchSessionTimeout = time.Second
	// Time a reliable publish waits for a replica to store it.
	replicaAckTimeout = time.Second
	// Default time a node shutting down waits at most for the others to take
	// it out of their rings, and for its hints and queues to be sent.
	defaultDrainTimeout = 10 * time.Second
	// Time a client request that a topic's owner did not take is sent again
	// for, and the time between attempts: longer than failure detection and
	// the other nodes' rehash, for a dead owner to be replaced.
	forwardRetryFor = 3 * time.Second
	forwardRetry    = 100 * time.Millisecond
	// Default time a rebuilt message is kept for, if stored without its expiry.
	defaultRebuildTTL = 24 * time.Hour
	// Time a hint for a session log change is kept for a replica that is not back.
	sessionHintTTL = "24h"
	// Ids of replicated messages a replica remembers, to store each once.
	seenReplicas = 100000
	// Hints kept in memory, until the store takes them, when it failed to.
	maxPendingHints = 10000
	// Attempts to reach each node when rebuilding, and the time between them.
	rebuildAttempts = 20
	rebuildRetry    = 500 * time.Millisecond
	// Time a rebuilding node waits for a node's list of topics, and for a
	// topic's messages.
	rebuildTimeout = 30 * time.Second
)

var (
	// errNotConnected is returned for a call to a node that is not
	// connected: the call was not sent.
	errNotConnected = errors.New("not connected")
	// errRejected is returned for a request a node did not take: its ring
	// does not give it the request's topic. It was not processed.
	errRejected = errors.New("request rejected: the node does not hold the topic in its ring")
)

// retryable reports whether a forwarded request failed without the node
// processing it, so that sending it again cannot process it twice: the node
// was not connected, its connection had already failed (rpc.ErrShutdown, for
// a call made after that), or it rejected the request.
func retryable(err error) bool {
	return errors.Is(err, errNotConnected) || errors.Is(err, rpc.ErrShutdown) || errors.Is(err, errRejected) || notSent(err)
}

// connectionFailed reports whether a call failed by its connection, not
// with an error the node answered, such as a method it lacks: the
// connection is then closed, and every call on it fails.
func connectionFailed(err error) bool {
	var answer rpc.ServerError
	return err != nil && !errors.As(err, &answer)
}

// notSent reports whether a call failed writing to a connection this node
// had closed, as when another call on it failed: the node did not get it,
// or got part of it, which it cannot decode.
func notSent(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "write" && errors.Is(err, net.ErrClosed)
}

// handoffInterval is the time between handoffs of the hints kept for each
// node. Set by the UNITDB_HANDOFF_INTERVAL environment variable, as a
// duration, for tests; 5s otherwise.
var handoffInterval = func() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("UNITDB_HANDOFF_INTERVAL")); err == nil && d > 0 {
		return d
	}
	return 5 * time.Second
}()

// deliverDelay delays each call delivering messages to another node's
// clients, for tests of how long a fan-out to many takes. Set by the
// UNITDB_DELIVER_DELAY environment variable, as a duration.
var deliverDelay, _ = time.ParseDuration(os.Getenv("UNITDB_DELIVER_DELAY"))

// replicationDelay delays each batch of asynchronous replication, for tests
// of what waits for a replica and what does not. Set by the
// UNITDB_REPLICATION_DELAY environment variable, as a duration.
var replicationDelay, _ = time.ParseDuration(os.Getenv("UNITDB_REPLICATION_DELAY"))

type clusterNodeConfig struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
	// TLSAddr is where the node takes cluster connections over mutual TLS,
	// when cluster_config.tls is set.
	TLSAddr string `json:"tls_addr,omitempty"`
}

type clusterConfig struct {
	// List of all members of the cluster, including this member
	Nodes []clusterNodeConfig `json:"nodes"`
	// Name of this cluster node
	ThisName string `json:"self"`
	// Number of nodes storing each message, its topic's owner included.
	// 1 disables replication; 0 means the default.
	Replicas int `json:"replicas"`
	// AsyncReplication acknowledges express publishes and session changes
	// without waiting for a replica, as reliable publishes are not: lower
	// latency, but the last ones before a crash can be lost.
	AsyncReplication bool `json:"async_replication"`
	// Time a node rebuilding its store keeps a message whose expiry was not
	// recorded, as a duration; empty means the default.
	RebuildTTL string `json:"rebuild_ttl"`
	// Time a node shutting down waits at most for the others to take it out
	// of their rings, and for what it holds for them to be sent, as a
	// duration; empty means the default.
	DrainTimeout string `json:"drain_timeout"`
	// Highest ring version the cluster routes by, once every node supports
	// it; 0 means the latest.
	RingVersion int `json:"ring_version"`
	// Failover configuration
	Failover *clusterFailoverConfig
	// TLS, if set, has nodes talk over mutual TLS: see cluster_tls.go.
	TLS *clusterTLSConfig `json:"tls"`
}

// ClusterNode is a client's connection to another node.
type ClusterNode struct {
	lock sync.Mutex

	// RPC endpoint
	endpoint *rpc.Client
	// The endpoint's connection, to tell whether it has closed
	conn *watchedConn
	// True if the endpoint is believed to be connected
	connected bool
	// True if a go routine is trying to reconnect the node
	reconnecting bool
	// TCP address in the form host:port
	address string
	// TLS address, if the node takes connections over TLS
	tlsAddress string
	// Name of the node
	name string

	// A number of times this node has failed in a row
	failCount int

	// Channel for shutting down the runner; buffered, 1
	done chan bool

	// Messages to store on the node as a replica of their topic's owner, and
	// changes to session logs it holds a replica of, in order.
	repl chan replicaItem
	// Set while a batch from repl is being sent.
	sending atomic.Bool
	// Closed to stop sending replicas to the node.
	replDone chan struct{}
	// Held while handing hints to the node.
	handoffMu sync.Mutex
	// What the node can do, as far as this node knows.
	caps peerCapabilities
}

// ReplicaEntry is a stored message sent to a replica of its topic's owner.
type ReplicaEntry struct {
	// ID is unique to the message: a replica that gets it again, from a hint
	// for a batch it stored but did not answer in time, stores it once.
	ID        string
	Contract  uint32
	Topic     string // as stored: the topic with its options
	Payload   []byte
	Ttl       string
	ExpiresAt int64 // unix seconds, 0 if it does not expire
}

// replicaItem is a message or a session log change queued for a replica.
type replicaItem struct {
	entry *ReplicaEntry
	op    *store.LogOp
	// done, if set, is sent the result once the item's batch is stored, or
	// not, for a caller waiting for the replica.
	done chan<- error
}

// errReplicaUnable is sent to a waiting caller when the replica cannot take
// an item: it lacks the capability. The item is kept as a hint.
var errReplicaUnable = errors.New("cluster: replica cannot take the item")

// replicaHint is a message, or a session log change, kept for a replica that
// could not take it, and the id it is kept under. For a session log change,
// the hint holds which key changed, or which session's log was reset: the
// handoff sends its state then, so that hints need no order.
type replicaHint struct {
	ID    []byte
	Entry ReplicaEntry
	Op    *store.LogOp
}

// ReplicateReq is a batch of stored messages and session log changes for a
// replica.
type ReplicateReq struct {
	// Name of the node sending this request
	Node    string
	Entries []ReplicaEntry
	Log     []store.LogOp
	// Handoff is set for messages kept as hints for the replica.
	Handoff bool
}

// RebuildReq asks a node for what a node rebuilding its store needs from it.
type RebuildReq struct {
	// Name of the rebuilding node
	Node string
}

// RebuildTopicsResp lists the topics a node hands a rebuilding node the
// messages of.
type RebuildTopicsResp struct {
	Topics []store.TopicRef
}

// RebuildHistoryReq asks a node for a topic's messages.
type RebuildHistoryReq struct {
	// Name of the rebuilding node
	Node  string
	Topic store.TopicRef
}

// RebuildHistoryResp is a topic's messages.
type RebuildHistoryResp struct {
	Entries []store.HistoryEntry
}

// FetchSessionReq asks a node for its copy of a session.
type FetchSessionReq struct {
	// Name of the node sending this request
	Node string
	// Session key, as the connecting client gives it
	SessKey uint64
}

// ForgetSessionReq asks a node to drop its copy of a session.
type ForgetSessionReq struct {
	// Name of the node the session's client resumed on
	Node    string
	SessKey uint64
	Block   uint32 // the session id
}

// FetchSessionResp is a node's copy of a session: its row, and its log.
type FetchSessionResp struct {
	Found bool
	Row   []byte
	Block uint32 // the session id
	Log   []store.LogOp
}

// ClusterSess is a basic info on a remote session where the message was created.
type ClusterSess struct {
	// IP address of the client. For long polling this is the IP of the last poll
	RemoteAddr string
	// protocol - NONE (unset), RPC, GRPC, GRPC_WEB, WEBSOCK
	Proto lp.Proto
	// Connection ID
	ConnID uid.LID
	// Session ID
	SessID uid.LID
	// Client ID
	ClientID uid.ID
	// Insecure is set for a connection whose requests skip topic key checks.
	// A node with capService sets it only for a trusted service's connection
	// (a service client id, or one a service vouched for); an older node
	// sends its client's own CONNECT flag. So a node takes it only from
	// peers that advertise capService.
	Insecure bool
}

// ClusterReq is a Proxy to Master request message.
type ClusterReq struct {
	// Name of the node sending this request
	Node string

	// Ring hash signature of the node sending this request. The receiver
	// takes a request by what its own ring holds, not by the signature: rings
	// disagree for a few heartbeats after a rehash.
	Signature string

	SubMsg   *utp.Subscribe
	PubMsg   *utp.Publish
	UnsubMsg *utp.Unsubscribe
	RelayMsg *utp.Relay
	Topic    *security.Topic
	Type     uint8
	Message  *message.Message

	// Originating session
	Conn *ClusterSess
	// True if the original session has disconnected
	ConnGone bool
}

// ClusterResp is a Master to Proxy response message.
type ClusterResp struct {
	Type     uint8
	SubMsg   *utp.Subscribe
	PubMsg   *utp.Publish
	UnsubMsg *utp.Unsubscribe
	RespMsg  []byte
	Topic    *security.Topic
	// Message is a publish for the connection to deliver as it delivers a
	// local one, with the subscription's delivery mode and delay.
	Message *message.Message
	// Reliable is set when Message is to be logged until the client completes
	// the flow, rather than sent at once.
	Reliable bool
	// Connection ID to forward message to, if any.
	FromConnID uid.LID
}

// Handle outbound node communication: read messages from the channel, forward to remote nodes.
// FIXME(gene): this will drain the outbound queue in case of a failure: all unprocessed messages will be dropped.
// Maybe it's a good thing, maybe not.
func (n *ClusterNode) reconnect() {
	var reconnTicker *time.Ticker

	// Avoid parallel reconnection threads
	n.lock.Lock()
	if n.reconnecting {
		n.lock.Unlock()
		return
	}
	n.reconnecting = true
	n.lock.Unlock()

	var count = 0
	for {
		// Attempt to reconnect right away
		if endpoint, conn, err := n.dial(); err == nil {
			if reconnTicker != nil {
				reconnTicker.Stop()
			}
			n.lock.Lock()
			if n.connected {
				// A call redialed in the meantime; keep its connection.
				endpoint.Close()
			} else {
				n.endpoint, n.conn = endpoint, conn
				n.connected = true
			}
			n.reconnecting = false
			n.lock.Unlock()
			log.Info("cluster.reconnect", "connection established "+n.name)
			n.resync()
			return
		} else if count == 0 {
			reconnTicker = time.NewTicker(defaultClusterReconnect)
		}

		count++

		select {
		case <-reconnTicker.C:
			// Wait for timer to try to reconnect again. Do nothing if the timer is inactive.
		case <-n.done:
			// Shutting down
			log.Info("cluster.reconnect", "node shutdown started "+n.name)
			reconnTicker.Stop()
			n.lock.Lock()
			if n.endpoint != nil {
				n.endpoint.Close()
			}
			n.connected = false
			n.reconnecting = false
			n.lock.Unlock()
			log.Info("cluster", "node shut down completed "+n.name)
			return
		}
	}
}

// watchedConn records when the connection has closed. The rpc client reads
// it continuously, so a node going away is seen as soon as its socket closes.
type watchedConn struct {
	net.Conn
	closed int32
}

func (c *watchedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil {
		atomic.StoreInt32(&c.closed, 1)
	}
	return n, err
}

func (c *watchedConn) isClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

// client returns the node's RPC endpoint if it is connected. The endpoint is
// replaced by reconnect, so it is only read under the node lock.
//
// If the endpoint's connection has already closed, e.g. the node restarted
// while the connection was idle, client redials first, so the first request
// after a restart is not lost. Requests are never retried after being sent:
// a failed call may still have been processed by the node.
func (n *ClusterNode) client() (*rpc.Client, bool) {
	n.lock.Lock()
	defer n.lock.Unlock()
	if n.connected && n.conn != nil && n.conn.isClosed() {
		if endpoint, conn, err := n.dial(); err == nil {
			n.endpoint.Close()
			n.endpoint, n.conn = endpoint, conn
			n.resync()
		}
	}
	return n.endpoint, n.connected
}

// resync sends the node again every subscription this node's clients hold
// there, once it is reachable again: it may have restarted and lost them
// without failing for long enough to be removed from the ring.
// It also hands the node its hints, and exchanges the whole security state
// with it (pushRevocations), which it may have missed while away.
func (n *ClusterNode) resync() {
	if c := Globals.Cluster; c != nil {
		go c.rebalance(map[string]bool{n.name: true})
		go c.handoff(n.name)
		go c.pushRevocations(n)
	}
}

// disconnected marks the node down after a failed call on endpoint and starts
// reconnecting, unless another caller already did.
func (n *ClusterNode) disconnected(endpoint *rpc.Client) {
	n.lock.Lock()
	defer n.lock.Unlock()
	if n.connected && n.endpoint == endpoint {
		n.endpoint.Close()
		n.connected = false
		go n.reconnect()
	}
}

func (n *ClusterNode) call(proc string, reqMsg, respMsg interface{}) error {
	endpoint, connected := n.client()
	if !connected {
		return fmt.Errorf("cluster.call: node '%s': %w", n.name, errNotConnected)
	}

	if err := endpoint.Call(proc, reqMsg, respMsg); err != nil {
		// A call failed by the connection means the node went away;
		// reconnect rather than exit, or one node's failure would take down
		// every node talking to it.
		log.ErrLogger.Error().Err(err).Str("context", "cluster.call").Msg("call failed to " + n.name)
		if connectionFailed(err) {
			n.disconnected(endpoint)
		}
		return err
	}

	return nil
}

func (n *ClusterNode) callAsync(proc string, reqMsg, respMsg interface{}, done chan *rpc.Call) *rpc.Call {
	if done != nil && cap(done) == 0 {
		log.Fatal("cluster.callAsync", "RPC done channel is unbuffered", nil)
	}

	endpoint, connected := n.client()
	if !connected {
		call := &rpc.Call{
			ServiceMethod: proc,
			Args:          reqMsg,
			Reply:         respMsg,
			Error:         fmt.Errorf("cluster.callAsync: node '%s': %w", n.name, errNotConnected),
			Done:          done,
		}
		if done != nil {
			done <- call
		}
		return call
	}

	myDone := make(chan *rpc.Call, 1)
	go func() {
		call := <-myDone
		if connectionFailed(call.Error) {
			n.disconnected(endpoint)
		}

		if done != nil {
			done <- call
		}
	}()

	// The goroutine above forwards the finished call to done. Setting call.Done
	// here would race with net/rpc delivering the result.
	return endpoint.Go(proc, reqMsg, respMsg, myDone)
}

// callTimeout calls proc on the node and waits up to d for it to answer. A
// call that times out may still complete on the node.
func (n *ClusterNode) callTimeout(proc string, reqMsg, respMsg interface{}, d time.Duration) error {
	done := make(chan *rpc.Call, 1)
	n.callAsync(proc, reqMsg, respMsg, done)
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case call := <-done:
		return call.Error
	case <-timer.C:
		return errors.New("cluster.callTimeout: node '" + n.name + "' did not answer " + proc + " in " + d.String())
	}
}

// Proxy forwards message to master
func (n *ClusterNode) forward(forwMsg *ClusterReq) error {
	log.Info("cluster.forward", "forwarding request to node "+n.name)
	forwMsg.Node = Globals.Cluster.thisNodeName
	rejected := false
	err := n.call("Cluster.Master", forwMsg, &rejected)
	if err == nil && rejected {
		err = fmt.Errorf("cluster.forward: node '%s': %w", n.name, errRejected)
	}
	return err
}

// masterLocks serializes Master requests per proxied connection ID.
var masterLocks sync.Map

// connNodesMu guards _Conn.nodes, which publishes handled on their own
// goroutines write while the connection's close reads it. It is separate from
// the connection lock, which subscribe holds while routing.
var connNodesMu sync.Mutex

// Cluster is the representation of the cluster.
type Cluster struct {
	// Cluster nodes with RPC endpoints
	nodes map[string]*ClusterNode
	// Name of the local node
	thisNodeName string

	// Resolved address to listed on
	listenOn string

	// Socket for inbound connections
	inbound *net.TCPListener
	// Listener for inbound connections over TLS, and the TLS setup, if set
	tlsInbound net.Listener
	tls        *clusterTLS
	// Ring hash of every configured node, live or not: a topic's replicas in
	// it are the nodes that should hold the topic's messages. Replaced when
	// the ring version changes, so use getFullRing.
	fullRing *rh.Ring
	// Every configured node, this one included.
	allNodes []string
	// Version of the ring, guarded by ringMu; and the highest the cluster's
	// configuration allows, 0 for the latest.
	ringVersion int
	ringTarget  int
	// Ring version the cluster was last seen routing by, from the leader's
	// pings or as the leader; 0 until seen. A change of it moves stored
	// messages.
	clusterRing atomic.Int32
	// Ring hash for mapping topic names to nodes. It is replaced on rehash by
	// the failover runner while requests read it, so use getRing.
	ringMu sync.RWMutex
	ring   *rh.Ring
	// Nodes in the ring, this node included: the nodes failover considers live.
	ringNodes []string

	// Failover parameters. Could be nil if failover is not enabled
	fo *clusterFailover

	// Number of nodes storing each message, its topic's owner included.
	replicas int
	// asyncReplication: express publishes and session changes don't wait
	// for a replica.
	asyncReplication bool
	// Time a rebuilt message is kept for, if stored without its expiry.
	rebuildTTL time.Duration
	// Set while this node, started with an empty store, copies its topics'
	// messages from the other nodes.
	rebuilding atomic.Bool
	// Set once this node, shutting down, leaves the cluster.
	leaving atomic.Bool
	// Set once the cluster is shut down.
	stopped atomic.Bool
	// When this node last heard from a leader, for its readiness.
	health clusterHealth
	// Time it waits at most for that.
	drainTimeout time.Duration

	// Ids given to the messages this node replicates: this node's name, the
	// time it started, and a sequence.
	replicaIDPrefix string
	replicaSeq      atomic.Uint64
	// Ids of the messages this node recently stored as a replica.
	seen seenSet

	// Hints the store did not take, to store again.
	pendingMu sync.Mutex
	pending   []pendingHint
}

// pendingHint is a hint for replica the store did not take, and how long to
// keep it for once stored.
type pendingHint struct {
	replica string
	hint    replicaHint
	ttl     string
}

// putHint stores an encoded hint; tests replace it to make the store fail.
var putHint = store.Hint.Put

// seenSet holds the most recent ids added to it, up to seenReplicas.
type seenSet struct {
	mu    sync.Mutex
	ids   map[string]bool
	order []string
	next  int
}

// add adds id, and reports whether it was not there already.
func (s *seenSet) add(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids == nil {
		s.ids = make(map[string]bool, seenReplicas)
		s.order = make([]string, seenReplicas)
	}
	if s.ids[id] {
		return false
	}
	delete(s.ids, s.order[s.next])
	s.order[s.next] = id
	s.next = (s.next + 1) % seenReplicas
	s.ids[id] = true
	return true
}

// remove removes id, added for a message that could not be stored after all.
func (s *seenSet) remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ids, id)
}

// Master at topic's master node receives C2S messages from topic's proxy nodes.
// The message is treated like it came from a session: find or create a session locally,
// dispatch the message to it like it came from a normal ws/lp connection.
// Called by a remote node.
func (c *Cluster) Master(reqMsg *ClusterReq, rejected *bool) error {
	log.Info("cluster.Master", "master request received from node "+reqMsg.Node)
	if reqMsg.Conn == nil {
		// Every request is for a connection; a panic here would stop the node.
		return errors.New("cluster.Master: a request without its connection")
	}

	// net/rpc serves each request on its own goroutine; handle requests for one
	// proxied connection one at a time, as its reads would be for a direct one.
	mu, _ := masterLocks.LoadOrStore(reqMsg.Conn.ConnID, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	// Find the local connection associated with the given remote connection.
	conn := Globals.connCache.get(reqMsg.Conn.ConnID)
	if conn != nil && conn.clnode != nil && conn.clnode.name != reqMsg.Node {
		// The connection was proxied for another node: a node acts only on
		// the connections of its own clients.
		log.ErrLogger.Error().Str("context", "cluster.Master").Int64("connid", int64(reqMsg.Conn.ConnID)).Msg("request from " + reqMsg.Node + " for a connection of " + conn.clnode.name + ": dropped")
		return nil
	}

	if reqMsg.ConnGone {
		// Original session has disconnected. Tear down the local proxied session.
		if conn != nil && conn.clnode != nil {
			conn.stopRPC()
		}
		masterLocks.Delete(reqMsg.Conn.ConnID)
	} else if reqMsg.Conn.Insecure && !c.knowsCapabilities(reqMsg.Node) {
		// A trusted connection's request from a node whose capabilities this
		// one has not heard yet, as when the cluster has just started: the
		// trust can't be taken, nor the request handled as untrusted, which
		// fails its key check unseen. Rejected, it is sent again, until the
		// node's capabilities are known.
		*rejected = true
	} else if c.takes(reqMsg) {
		// This cluster member received a request for a topic it holds.

		if conn == nil {
			// If the session is not found, create it.
			node := Globals.Cluster.nodes[reqMsg.Node]
			if node == nil {
				log.Error("cluster.Master", "request from an unknown node "+reqMsg.Node)
				return nil
			}

			log.Info("cluster.Master", "new connection request"+fmt.Sprint(reqMsg.Conn.ConnID))
			conn = Globals.Service.newRpcConn(node, reqMsg.Conn.ConnID, reqMsg.Conn.SessID, reqMsg.Conn.ClientID)
			go conn.rpcWriteLoop()
		}
		if conn.clnode == nil {
			// The id is of one of this node's own clients, not of a
			// connection proxied for another node: never act for the peer's
			// client as this one.
			log.ErrLogger.Error().Str("context", "cluster.Master").Int64("connid", int64(reqMsg.Conn.ConnID)).Msg("request from " + reqMsg.Node + " for the id of a local connection: dropped")
			return nil
		}
		// The proxied connection skips key checks only for a trusted
		// service's connection, as the client's node found it. It is taken
		// per request, since a service may vouch for a connection after it
		// connects, and only from a peer that advertises capService: an older
		// node forwards its client's own insecure flag.
		if n := c.nodes[reqMsg.Node]; n != nil && n.knownToSupport(capService) {
			conn.insecure.Store(reqMsg.Conn.Insecure)
		} else {
			conn.insecure.Store(false)
		}
		// connID is the lookup key and clientID was set when the proxied
		// connection was created; rewriting them per request raced with the
		// connection's own goroutines.

		switch reqMsg.Type {
		case message.SUBSCRIBE:
			conn.handler(reqMsg.SubMsg)
		case message.UNSUBSCRIBE:
			conn.handler(reqMsg.UnsubMsg)
		case message.PUBLISH:
			conn.handler(reqMsg.PubMsg)
		case message.RELAY:
			conn.handler(reqMsg.RelayMsg)
		}
	} else {
		// Reject the request: this node's ring does not give it the topic.
		// The sender tries again, or another node.
		*rejected = true
	}

	return nil
}

// takes reports whether this node takes a request another node forwarded, by
// what its own ring holds: rings disagree for a few heartbeats after a rehash.
// It takes every subscribe and unsubscribe: a subscription sent to a node
// that does not own its topic moves when the sender's ring catches up, and
// one that could not be removed would stay. It takes a publish only for a
// topic it owns, where the topic's subscriptions are held, and a relay only
// for a topic it holds the messages of, and not while it rebuilds them.
func (c *Cluster) takes(req *ClusterReq) bool {
	contract := req.Conn.ClientID.Contract()
	switch req.Type {
	case message.PUBLISH:
		for _, m := range req.PubMsg.Messages {
			t := security.ParseKey(m.Topic)
			if t.TopicType != security.TopicInvalid && c.isRemoteTopic(contract, t.Topic[:t.Size]) {
				return false
			}
		}
	case message.RELAY:
		if c.rebuilding.Load() {
			return false
		}
		for _, r := range req.RelayMsg.RelayRequests {
			t := security.ParseKey(r.Topic)
			if t.TopicType == security.TopicInvalid || isWildcardTopic(t.Topic[:t.Size]) {
				continue
			}
			if !c.holdsTopic(contract, t.Topic[:t.Size]) {
				return false
			}
		}
	}
	return true
}

// holdsTopic reports whether this node is one of topic's replicas, which
// hold its messages, in the current ring.
func (c *Cluster) holdsTopic(contract uint32, topic string) bool {
	for _, n := range c.getRing().GetN(topicRingKey(contract, topic), c.replicas) {
		if n == c.thisNodeName {
			return true
		}
	}
	return false
}

// Dispatch receives messages from the master node addressed to a specific local connection.
func (c *Cluster) Proxy(resp *ClusterResp, unused *bool) error {
	log.Info("cluster.Proxy", "response from Master for connection "+fmt.Sprint(resp.FromConnID))
	if resp.Message != nil {
		time.Sleep(deliverDelay)
	}

	// This cluster member received a response from topic owner to be forwarded to a connection
	// Find appropriate connection, send the message to it

	if conn := Globals.connCache.get(resp.FromConnID); conn != nil {
		if resp.Message != nil {
			// A publish from the topic's owner: deliver it here, where the
			// client's flow control for it is handled.
			if !conn.deliver(resp.Message, resp.Reliable) {
				log.Error("cluster.Proxy", "Proxy: delivery failed")
			}
		} else if !conn.SendRawBytes(resp.RespMsg) {
			log.Error("cluster.Proxy", "Proxy: timeout")
		}
	} else {
		log.ErrLogger.Error().Str("context", "cluster.Proxy").Uint64("connid", uint64(resp.FromConnID)).Msg("master response for unknown session")
	}

	return nil
}

// Delivery is a message for a client of another node, delivered there as to
// a local subscriber.
type Delivery struct {
	ConnID   uid.LID
	Message  *message.Message
	Reliable bool
}

// DeliverReq is the messages a topic's owner delivers to a node's clients.
type DeliverReq struct {
	// Name of the node sending this request
	Node       string
	Deliveries []Delivery
}

// deliverRemote hands each node the messages for its clients, in one call per
// node, to all the nodes at once, and waits for them.
func (c *Cluster) deliverRemote(byNode map[*ClusterNode][]Delivery) {
	if c == nil || len(byNode) == 0 {
		return
	}
	var wg sync.WaitGroup
	for n, deliveries := range byNode {
		wg.Add(1)
		go func(n *ClusterNode, deliveries []Delivery) {
			defer wg.Done()
			if hasCapability(capDeliver) && n.supports(capDeliver) {
				var unused bool
				err := n.call("Cluster.Deliver", &DeliverReq{Node: c.thisNodeName, Deliveries: deliveries}, &unused)
				if err == nil {
					return
				}
				if !n.lacks(err, capDeliver) {
					log.ErrLogger.Error().Err(err).Str("context", "cluster.deliverRemote").Int("messages", len(deliveries)).Msg("unable to deliver to clients of " + n.name)
					return
				}
			}
			// A node that cannot take them all at once: one call each.
			for _, d := range deliveries {
				var unused bool
				if err := n.call("Cluster.Proxy", &ClusterResp{Message: d.Message, Reliable: d.Reliable, FromConnID: d.ConnID}, &unused); err != nil {
					log.ErrLogger.Error().Err(err).Str("context", "cluster.deliverRemote").Msg("unable to deliver to a client of " + n.name)
				}
			}
		}(n, deliveries)
	}
	wg.Wait()
}

// Deliver delivers messages from a topic's owner to this node's clients, each
// as to a local subscriber, all at once: a slow client does not hold up the
// others. Called by the topic's owner.
func (c *Cluster) Deliver(req *DeliverReq, unused *bool) error {
	if err := refuse(capDeliver); err != nil {
		return err
	}
	time.Sleep(deliverDelay)
	var wg sync.WaitGroup
	for _, d := range req.Deliveries {
		conn := Globals.connCache.get(d.ConnID)
		if conn == nil {
			log.ErrLogger.Error().Str("context", "cluster.Deliver").Uint64("connid", uint64(d.ConnID)).Msg("message for unknown session")
			continue
		}
		wg.Add(1)
		go func(conn *_Conn, d Delivery) {
			defer wg.Done()
			if !conn.deliver(d.Message, d.Reliable) {
				log.ErrLogger.Error().Str("context", "cluster.Deliver").Uint64("connid", uint64(d.ConnID)).Msg("delivery failed")
			}
		}(conn, d)
	}
	wg.Wait()
	return nil
}

// topicRingKey is the ring key of a topic: the contract and the topic, without
// its options. Every subscription to a topic and every publish on it meet at
// the node the key hashes to, the topic's owner.
func topicRingKey(contract uint32, topic string) string {
	return strconv.FormatUint(uint64(contract), 10) + "/" + topic
}

// knowsCapabilities reports whether this node has heard which capabilities
// node has.
func (c *Cluster) knowsCapabilities(node string) bool {
	n := c.nodes[node]
	if n == nil {
		return false
	}
	_, ok := n.capabilities()
	return ok
}

// isWildcardTopic reports whether topic is a pattern (* or ...), which can
// match topics owned by any node.
func isWildcardTopic(topic string) bool {
	return strings.Contains(topic, "*") || strings.HasSuffix(topic, "...")
}

// isRemoteTopic reports whether another node owns topic. A wildcard topic has
// no single owner: it is never remote, and is sent to every node instead.
func (c *Cluster) isRemoteTopic(contract uint32, topic string) bool {
	if c == nil {
		// Cluster not initialized, all topics are local
		return false
	}
	if isWildcardTopic(topic) {
		return false
	}
	return c.getRing().Get(topicRingKey(contract, topic)) != c.thisNodeName
}

// routeToTopic forwards a client's publish on topic to the topic's owner, and
// reports false if this node owns the topic, for the publish to be handled
// here. msg holds that one topic's message only. A publish the owner did not
// take, or could not be sent as the owner is not connected, goes to the owner
// in the current ring again, for up to forwardRetryFor. One that fails after
// being sent does not: the owner may have processed it.
func (c *Cluster) routeToTopic(msg lp.MessagePack, contract uint32, topic string, conn *_Conn) (bool, error) {
	deadline := time.Now().Add(forwardRetryFor)
	for {
		owner := c.getRing().Get(topicRingKey(contract, topic))
		if owner == c.thisNodeName {
			return false, nil
		}
		n := c.nodes[owner]
		if n == nil {
			return false, errors.New("cluster.routeToTopic: no node " + owner)
		}
		err := c.forwardTo(n, msg, conn)
		if err == nil || !retryable(err) || time.Now().After(deadline) {
			return true, err
		}
		time.Sleep(forwardRetry)
	}
}

// relayFromHolder forwards a relay on topic to the first node of the topic's
// replica set that takes it: the owner, then its replicas. It reports false if
// this node comes first, or none takes it, for the relay to be answered here.
func (c *Cluster) relayFromHolder(msg *utp.Relay, contract uint32, topic string, conn *_Conn) bool {
	rebuilding := c.rebuilding.Load()
	for _, holder := range c.getRing().GetN(topicRingKey(contract, topic), c.replicas) {
		n := c.nodes[holder]
		if n == nil {
			if rebuilding {
				continue // this node, whose messages are not all here yet
			}
			return false // this node
		}
		if err := c.forwardTo(n, msg, conn); err != nil {
			log.ErrLogger.Error().Err(err).Str("context", "cluster.relayFromHolder").Str("topic", topic).Msg("relay from " + holder + " failed")
			continue
		}
		return true
	}
	return false
}

// forwardTo forwards a client request to node n, which handles it as a
// request of a local session.
func (c *Cluster) forwardTo(n *ClusterNode, msg lp.MessagePack, conn *_Conn) error {
	// Save node name: it's need in order to inform relevant nodes when the session is disconnected
	connNodesMu.Lock()
	if conn.nodes == nil {
		conn.nodes = make(map[string]bool)
	}
	conn.nodes[n.name] = true
	connNodesMu.Unlock()

	req := &ClusterReq{
		Node:      c.thisNodeName,
		Signature: c.getRing().Signature(),
		Conn: &ClusterSess{
			//RemoteAddr: conn.(),
			ConnID:   conn.connID,
			SessID:   conn.sessID,
			ClientID: conn.clientID,
			Insecure: conn.insecure.Load()}}
	switch m := msg.(type) {
	case *utp.Subscribe:
		m.IsForwarded = true
		req.Type, req.SubMsg = message.SUBSCRIBE, m
	case *utp.Unsubscribe:
		m.IsForwarded = true
		req.Type, req.UnsubMsg = message.UNSUBSCRIBE, m
	case *utp.Publish:
		m.IsForwarded = true
		req.Type, req.PubMsg = message.PUBLISH, m
	case *utp.Relay:
		m.IsForwarded = true
		req.Type, req.RelayMsg = message.RELAY, m
	default:
		return errors.New("cluster.forwardTo: unexpected message type")
	}
	return n.forward(req)
}

// replicate sends a message this node stored, as its topic's owner, to the
// topic's other replicas: the next live nodes on the ring. If wait is set, it
// first stores the message on one replica, the first in ring order that takes
// it, before it returns; the others, and all of them otherwise, get it from a
// queue, without waiting. The message is kept as a hint for a replica that
// does not take it, or whose queue has no room for it, and for each node that
// should be a replica but is not live, to be handed to it when it is back.
// waitsForReplica reports whether a publish is acknowledged only once a
// replica stored it: a reliable one always, an express one unless the
// cluster replicates asynchronously.
func (c *Cluster) waitsForReplica(reliable bool) bool {
	return reliable || (c != nil && !c.asyncReplication)
}

func (c *Cluster) replicate(contract uint32, name, topic string, payload []byte, ttl string, wait bool) {
	if c == nil || c.replicas < 2 || isWildcardTopic(name) || !hasCapability(capReplicate) {
		return
	}
	key := topicRingKey(contract, name)
	e := ReplicaEntry{
		ID:        c.replicaIDPrefix + strconv.FormatUint(c.replicaSeq.Add(1), 36),
		Contract:  contract,
		Topic:     topic,
		Payload:   payload,
		Ttl:       ttl,
		ExpiresAt: store.ExpiresAt(ttl),
	}
	live := make(map[string]bool)
	for _, n := range c.getRingNodes() {
		live[n] = true
	}
	stored := false
	for _, replica := range c.getRing().GetN(key, c.replicas) {
		n := c.nodes[replica]
		if n == nil {
			continue // this node
		}
		if !n.supports(capReplicate) {
			c.hint(replica, e) // for when it can take it
			continue
		}
		if wait && !stored {
			var unused bool
			if err := n.callTimeout("Cluster.Replicate", &ReplicateReq{Node: c.thisNodeName, Entries: []ReplicaEntry{e}}, &unused, replicaAckTimeout); err != nil {
				c.hint(replica, e)
				continue
			}
			stored = true
			continue
		}
		select {
		case n.repl <- replicaItem{entry: &e}:
		default:
			c.hint(replica, e)
		}
	}
	if wait && !stored {
		log.ErrLogger.Warn().Str("context", "cluster.replicate").Str("topic", name).Msg("no replica took the message: stored on this node only")
	}
	for _, replica := range c.getFullRing().GetN(key, c.replicas) {
		if !live[replica] {
			c.hint(replica, e)
		}
	}
}

// hint keeps a message for replica, which could not take it, until it is
// handed to it or expires with the message.
func (c *Cluster) hint(replica string, e ReplicaEntry) {
	c.keepHint(replica, replicaHint{Entry: e}, e.Ttl)
}

// keepHint stores a hint for replica, or keeps it in memory to store again
// if the store does not take it.
func (c *Cluster) keepHint(replica string, h replicaHint, ttl string) {
	if err := storeHint(replica, h, ttl); err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "cluster.keepHint").Msg("unable to store hint for " + replica + ": kept in memory")
		c.pendingMu.Lock()
		if len(c.pending) >= maxPendingHints {
			c.pending = c.pending[1:]
			log.ErrLogger.Error().Str("context", "cluster.keepHint").Msg("too many hints kept in memory: the oldest dropped")
		}
		c.pending = append(c.pending, pendingHint{replica: replica, hint: h, ttl: ttl})
		c.pendingMu.Unlock()
	}
}

// storeHint stores a hint for replica, under a new id.
func storeHint(replica string, h replicaHint, ttl string) error {
	id, err := store.Hint.NewID()
	if err != nil {
		return err
	}
	h.ID = id
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(h); err != nil {
		return err
	}
	return putHint(replica, id, buf.Bytes(), ttl)
}

// storePendingHints stores the hints kept in memory, and keeps those the
// store still does not take.
func (c *Cluster) storePendingHints() {
	c.pendingMu.Lock()
	pending := c.pending
	c.pending = nil
	c.pendingMu.Unlock()
	var failed []pendingHint
	for _, p := range pending {
		if err := storeHint(p.replica, p.hint, p.ttl); err != nil {
			failed = append(failed, p)
		}
	}
	if len(failed) > 0 {
		c.pendingMu.Lock()
		c.pending = append(failed, c.pending...)
		if len(c.pending) > maxPendingHints {
			c.pending = c.pending[len(c.pending)-maxPendingHints:]
		}
		c.pendingMu.Unlock()
	}
}

// handoffLoop hands the node its hints every handoffInterval, until the
// cluster shuts down: a node that stalled without its connection failing is
// not resynced, and would otherwise keep missing them.
func (c *Cluster) handoffLoop(n *ClusterNode) {
	ticker := time.NewTicker(handoffInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.storePendingHints()
			if _, connected := n.client(); connected {
				c.handoff(n.name)
			}
		case <-n.replDone:
			return
		}
	}
}

// hintLog keeps, for replica, which key of a session's log or row changed, or
// that the session's log was reset, until it is handed to it.
func (c *Cluster) hintLog(replica string, op store.LogOp) {
	// The state is read when handed off.
	op.Raw = nil
	c.keepHint(replica, replicaHint{Op: &op}, sessionHintTTL)
}

// currentState returns the changes that give a replica this node's state of
// what a session log hint names: the key, or the whole log of a reset session.
func currentState(op store.LogOp) []store.LogOp {
	if op.Reset {
		ops := []store.LogOp{op}
		for _, key := range store.Log.Keys(op.Block) {
			if raw := store.Log.Raw(key); raw != nil {
				ops = append(ops, store.LogOp{Block: op.Block, Key: key, Raw: raw})
			}
		}
		return ops
	}
	// Nil Raw, for a key that is gone, deletes it.
	return []store.LogOp{{Block: op.Block, Key: op.Key, Raw: store.Log.Raw(op.Key)}}
}

// handoff hands the messages kept for the node as hints to it, and deletes
// them once it has taken them. It returns at once if a handoff to the node is
// already running.
func (c *Cluster) handoff(name string) {
	n := c.nodes[name]
	if n == nil || !n.handoffMu.TryLock() {
		return
	}
	defer n.handoffMu.Unlock()
	c.storePendingHints()
	for {
		// The store returns up to its query limit: hand those off, delete them,
		// and ask again until none are left.
		raw, err := store.Hint.Get(name)
		if err != nil || len(raw) == 0 {
			return
		}
		hints := make([]replicaHint, 0, len(raw))
		for _, b := range raw {
			var h replicaHint
			if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&h); err != nil {
				log.ErrLogger.Error().Err(err).Str("context", "cluster.handoff").Msg("unreadable hint for " + name)
				continue
			}
			hints = append(hints, h)
		}
		for start := 0; start < len(hints); start += replicationBatchSize {
			batch := hints[start:]
			if len(batch) > replicationBatchSize {
				batch = batch[:replicationBatchSize]
			}
			req := &ReplicateReq{Node: c.thisNodeName, Handoff: true}
			var sent []replicaHint
			for _, h := range batch {
				if h.Op != nil {
					if !n.supports(capSessions) {
						continue // kept until it can take it
					}
					req.Log = append(req.Log, currentState(*h.Op)...)
				} else {
					if !n.supports(capReplicate) {
						continue
					}
					req.Entries = append(req.Entries, h.Entry)
				}
				sent = append(sent, h)
			}
			if len(sent) == 0 {
				// None of these can go: they would come back each round.
				return
			}
			var unused bool
			if err := n.call("Cluster.Replicate", req, &unused); err != nil {
				// Kept for the next handoff.
				n.lacks(err, capReplicate)
				n.lacks(err, capSessions)
				return
			}
			for _, h := range sent {
				if err := store.Hint.Delete(name, h.ID); err != nil {
					// Not deleted, it would be handed off again and again.
					log.ErrLogger.Error().Err(err).Str("context", "cluster.handoff").Msg("unable to delete hint for " + name)
					return
				}
			}
		}
		log.ErrLogger.Info().Str("context", "cluster.handoff").Int("messages", len(hints)).Msg("handed off to " + name)
	}
}

// replicateLoop sends the messages and session log changes queued for the
// node in batches, until the cluster shuts down.
// delayBatch holds an asynchronous batch for replicationDelay, adding to it
// what is queued meanwhile. An item someone waits for ends the delay: it
// would otherwise wait behind the batch, past replicaAckTimeout.
func (n *ClusterNode) delayBatch(batch []replicaItem) []replicaItem {
	delay := time.NewTimer(replicationDelay)
	defer delay.Stop()
	for len(batch) < replicationBatchSize {
		select {
		case it := <-n.repl:
			batch = append(batch, it)
			if it.done != nil {
				return batch
			}
		case <-delay.C:
			return batch
		case <-n.replDone:
			return batch
		}
	}
	return batch
}

func (n *ClusterNode) replicateLoop(from string) {
	for {
		var batch []replicaItem
		select {
		case it := <-n.repl:
			batch = append(batch, it)
		case <-n.replDone:
			return
		}
		// Add what else is queued, up to a batch.
	fill:
		for len(batch) < replicationBatchSize {
			select {
			case it := <-n.repl:
				batch = append(batch, it)
			default:
				break fill
			}
		}
		// The delay is for asynchronous replication only: a batch that
		// someone waits for is sent at once.
		waited := false
		for _, it := range batch {
			if it.done != nil {
				waited = true
			}
		}
		if !waited && replicationDelay > 0 {
			batch = n.delayBatch(batch)
		}
		req := &ReplicateReq{Node: from}
		c := Globals.Cluster
		var waiters []chan<- error
		for _, it := range batch {
			switch {
			case it.entry != nil && !n.supports(capReplicate):
				if c != nil {
					c.hint(n.name, *it.entry) // for when it can take it
				}
				answer(it.done, errReplicaUnable)
			case it.entry != nil:
				req.Entries = append(req.Entries, *it.entry)
				if it.done != nil {
					waiters = append(waiters, it.done)
				}
			case !n.supports(capSessions):
				if c != nil {
					c.hintLog(n.name, *it.op)
				}
				answer(it.done, errReplicaUnable)
			default:
				req.Log = append(req.Log, *it.op)
				if it.done != nil {
					waiters = append(waiters, it.done)
				}
			}
		}
		if len(req.Entries) == 0 && len(req.Log) == 0 {
			continue
		}
		var unused bool
		n.sending.Store(true)
		err := n.call("Cluster.Replicate", req, &unused)
		n.sending.Store(false)
		for _, done := range waiters {
			answer(done, err)
		}
		if err != nil {
			n.lacks(err, capReplicate)
			n.lacks(err, capSessions)
			// Kept for the node until it is back. The node may have stored the
			// batch before failing, and would then get the messages twice.
			if c := Globals.Cluster; c != nil {
				for _, e := range req.Entries {
					c.hint(n.name, e)
				}
				for _, op := range req.Log {
					c.hintLog(n.name, op)
				}
			}
		}
	}
}

// rebuild copies the messages of the topics this node is a replica of from the
// other nodes, when it started with an empty store: a new node, or one that
// lost its disk. Each topic's messages come from one node, the first live one
// of the topic's replicas other than this node. A message stored without its
// expiry is kept for rebuildTTL. A node that cannot be reached is skipped.
func (c *Cluster) rebuild() {
	defer c.rebuilding.Store(false)
	for _, n := range c.nodes {
		if !n.supports(capReplicate) {
			continue // it holds no replicas
		}
		var topics RebuildTopicsResp
		var err error
		for attempt := 0; attempt < rebuildAttempts; attempt++ {
			if err = n.callTimeout("Cluster.RebuildTopics", &RebuildReq{Node: c.thisNodeName}, &topics, rebuildTimeout); err == nil {
				break
			}
			time.Sleep(rebuildRetry)
		}
		if err != nil {
			log.ErrLogger.Error().Err(err).Str("context", "cluster.rebuild").Msg("rebuild skipped " + n.name)
			continue
		}
		copied := 0
		for _, t := range topics.Topics {
			var history RebuildHistoryResp
			if err := n.callTimeout("Cluster.RebuildHistory", &RebuildHistoryReq{Node: c.thisNodeName, Topic: t}, &history, rebuildTimeout); err != nil {
				log.ErrLogger.Error().Err(err).Str("context", "cluster.rebuild").Str("topic", t.Topic).Msg("topic not rebuilt from " + n.name)
				continue
			}
			for _, e := range history.Entries {
				expiresAt := e.ExpiresAt
				if !e.Known {
					expiresAt = time.Now().Add(c.rebuildTTL).Unix()
				}
				if err := store.Message.PutReplica(t.Contract, t.Topic, e.Payload, expiresAt); err != nil {
					log.ErrLogger.Error().Err(err).Str("context", "cluster.rebuild").Str("topic", t.Topic).Msg("unable to store rebuilt message")
					continue
				}
				copied++
			}
		}
		log.ErrLogger.Info().Str("context", "cluster.rebuild").Int("topics", len(topics.Topics)).Int("messages", copied).Msg("rebuilt from " + n.name)
	}
}

// RebuildTopics returns the topics a rebuilding node is a replica of, for
// which this node is the one to hand it the messages: the first live node of
// the topic's replicas other than the rebuilding one. It drops the hints kept
// for the node, whose messages it gets with the rest. Called by the
// rebuilding node.
func (c *Cluster) RebuildTopics(req *RebuildReq, resp *RebuildTopicsResp) error {
	if err := refuse(capReplicate); err != nil {
		return err
	}
	if n := c.nodes[req.Node]; n != nil {
		n.handoffMu.Lock()
		c.dropHints(req.Node)
		n.handoffMu.Unlock()
	}
	live := make(map[string]bool)
	for _, n := range c.getRingNodes() {
		live[n] = true
	}
	for _, t := range store.Message.Topics() {
		holders := c.getFullRing().GetN(topicRingKey(t.Contract, t.Topic), c.replicas)
		replica := false
		for _, h := range holders {
			replica = replica || h == req.Node
		}
		if !replica {
			continue
		}
		for _, h := range holders {
			if h == req.Node || !live[h] {
				continue
			}
			if h == c.thisNodeName {
				resp.Topics = append(resp.Topics, t)
			}
			break
		}
	}
	return nil
}

// RebuildHistory returns the messages this node stores for a topic, for a
// rebuilding node. Called by the rebuilding node.
func (c *Cluster) RebuildHistory(req *RebuildHistoryReq, resp *RebuildHistoryResp) error {
	if err := refuse(capReplicate); err != nil {
		return err
	}
	entries, err := store.Message.History(req.Topic.Contract, req.Topic.Topic)
	resp.Entries = entries
	return err
}

// dropHints deletes the message hints kept for node, and keeps the session log
// ones: a rebuild copies messages only. The caller holds the node's handoff
// lock.
func (c *Cluster) dropHints(name string) {
	for {
		raw, err := store.Hint.Get(name)
		if err != nil || len(raw) == 0 {
			return
		}
		deleted := 0
		for _, b := range raw {
			var h replicaHint
			if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&h); err != nil || h.Op != nil {
				continue
			}
			if err := store.Hint.Delete(name, h.ID); err != nil {
				log.ErrLogger.Error().Err(err).Str("context", "cluster.dropHints").Msg("unable to delete hint for " + name)
				return
			}
			deleted++
		}
		if deleted == 0 {
			return // only hints to keep, or that cannot be read, are left
		}
	}
}

// sessionRingKey is the ring key of a session: its replicas are the nodes the
// key hashes to.
func sessionRingKey(sessID uint32) string {
	return "session/" + strconv.FormatUint(uint64(sessID), 10)
}

// replicateLog sends a change this node made to a session's log or row to
// the session's replicas. A change that stores something, such as a message
// logged for delivery, is first stored on one replica, the first in ring
// order that takes it, before it returns, unless the cluster replicates
// asynchronously; the other replicas, and deletions, which at worst
// redeliver a message if lost, get it from a queue, without waiting. The
// change is kept as a hint for a replica that does not take it, or whose
// queue has no room for it, and for each node that should be a replica but
// is not live.
func (c *Cluster) replicateLog(op store.LogOp) {
	if c.replicas < 2 || !hasCapability(capSessions) {
		return
	}
	wait := !c.asyncReplication && !op.Reset && op.Raw != nil
	key := sessionRingKey(op.Block)
	live := make(map[string]bool)
	for _, n := range c.getRingNodes() {
		live[n] = true
	}
	// A waited change goes through the queues too, so that each replica gets
	// a session's changes in order: sent directly, it could overtake an
	// older queued deletion of the same key, which would then remove it.
	var done chan error
	waiting := 0
	if wait {
		done = make(chan error, c.replicas)
	}
	for _, replica := range c.getRing().GetN(key, c.replicas) {
		n := c.nodes[replica]
		if n == nil {
			continue // this node
		}
		if !n.supports(capSessions) {
			c.hintLog(replica, op) // for when it can take it
			continue
		}
		it := replicaItem{op: &op}
		if wait {
			it.done = done
		}
		select {
		case n.repl <- it:
			if wait {
				waiting++
			}
		default:
			c.hintLog(replica, op)
		}
	}
	if waiting > 0 && !c.awaitReplica(done, waiting) {
		log.ErrLogger.Warn().Str("context", "cluster.replicateLog").Uint32("session", op.Block).Msg("no replica took the session change in time: it stays queued")
	}
	for _, replica := range c.getFullRing().GetN(key, c.replicas) {
		if !live[replica] {
			c.hintLog(replica, op)
		}
	}
}

// awaitReplica waits until one of n replicas answers done with success, up
// to replicaAckTimeout, and reports whether one did.
func (c *Cluster) awaitReplica(done <-chan error, n int) bool {
	timeout := time.NewTimer(replicaAckTimeout)
	defer timeout.Stop()
	for ; n > 0; n-- {
		select {
		case err := <-done:
			if err == nil {
				return true
			}
		case <-timeout.C:
			return false
		}
	}
	return false
}

// answer sends err to a waiting caller, if any. done is buffered for every
// replica, so it never blocks.
func answer(done chan<- error, err error) {
	if done != nil {
		done <- err
	}
}

// FetchSession returns this node's copy of a session, if it has one: as a
// replica, or as a node the session's client was connected to. Called by a
// remote node the client connects to.
func (c *Cluster) FetchSession(req *FetchSessionReq, resp *FetchSessionResp) error {
	if err := refuse(capSessions); err != nil {
		return err
	}
	row := store.Log.Raw(req.SessKey)
	if len(row) < 4 {
		return nil
	}
	resp.Found, resp.Row = true, row
	resp.Block = binary.LittleEndian.Uint32(row[:4])
	for _, key := range store.Log.Keys(resp.Block) {
		if raw := store.Log.Raw(key); raw != nil {
			resp.Log = append(resp.Log, store.LogOp{Block: resp.Block, Key: key, Raw: raw})
		}
	}
	return nil
}

// fetchSession gets the other nodes' copies of the session with key sessKey,
// before a client resumes it here, and stores them here: the session's row,
// unless this node has one, and every log entry any copy holds. A copy may
// miss the latest changes, or still hold entries the client completed
// elsewhere: taking every entry redelivers those rather than lose any. The
// nodes that are not the session's replicas then drop their copies, which
// would go stale as the client goes on here. It waits up to
// fetchSessionTimeout.
func (c *Cluster) fetchSession(sessKey uint64) {
	if c == nil || c.replicas < 2 || !hasCapability(capSessions) {
		return
	}
	done := make(chan *rpc.Call, len(c.nodes))
	callers := make(map[*rpc.Call]string, len(c.nodes))
	for _, n := range c.nodes {
		if n.supports(capSessions) {
			callers[n.callAsync("Cluster.FetchSession", &FetchSessionReq{Node: c.thisNodeName, SessKey: sessKey}, &FetchSessionResp{}, done)] = n.name
		}
	}
	var block uint32
	var row []byte
	if local := store.Log.Raw(sessKey); len(local) >= 4 {
		block, row = binary.LittleEndian.Uint32(local[:4]), local
	}
	haveRow := row != nil
	entries := make(map[uint64][]byte)
	var holders []string
	timeout := time.NewTimer(fetchSessionTimeout)
	defer timeout.Stop()
	for i := 0; i < len(callers); i++ {
		select {
		case call := <-done:
			resp := call.Reply.(*FetchSessionResp)
			if call.Error != nil {
				c.nodes[callers[call]].lacks(call.Error, capSessions)
				continue
			}
			if !resp.Found {
				continue
			}
			if row == nil {
				block, row = resp.Block, resp.Row
			} else if resp.Block != block {
				continue // another session under the key: keep the first
			}
			holders = append(holders, callers[call])
			for _, op := range resp.Log {
				entries[op.Key] = op.Raw
			}
		case <-timeout.C:
			i = len(callers)
		}
	}
	for key, raw := range entries {
		store.Log.Apply(store.LogOp{Block: block, Key: key, Raw: raw})
	}
	if row != nil && !haveRow {
		store.Log.Apply(store.LogOp{Block: block, Key: sessKey, Raw: row})
	}
	for _, h := range holders {
		if !c.isSessionReplica(h, block) && c.nodes[h].supports(capSessions) {
			var unused bool
			c.nodes[h].callAsync("Cluster.ForgetSession", &ForgetSessionReq{Node: c.thisNodeName, SessKey: sessKey, Block: block}, &unused, nil)
		}
	}
}

// isSessionReplica reports whether node holds a replica of the session with
// id block: in the current ring, or in the ring of every configured node.
func (c *Cluster) isSessionReplica(node string, block uint32) bool {
	key := sessionRingKey(block)
	for _, ring := range []*rh.Ring{c.getRing(), c.getFullRing()} {
		for _, n := range ring.GetN(key, c.replicas) {
			if n == node {
				return true
			}
		}
	}
	return false
}

// ForgetSession drops this node's copy of a session whose client resumed on
// another node, unless this node is one of the session's replicas: the copy
// would go stale, and be merged back into the session when its client next
// moves. Called by the node the client resumed on.
func (c *Cluster) ForgetSession(req *ForgetSessionReq, unused *bool) error {
	if err := refuse(capSessions); err != nil {
		return err
	}
	if c.isSessionReplica(c.thisNodeName, req.Block) {
		return nil
	}
	store.Log.Apply(store.LogOp{Block: req.Block, Reset: true})
	store.Log.Apply(store.LogOp{Block: req.Block, Key: req.SessKey})
	return nil
}

// Replicate stores messages for which this node is a replica of their topic's
// owner. Called by a remote node. Each message is stored under an id of this
// node's store: an id carries the sequence of the store that made it, which
// another store may use for another message.
func (c *Cluster) Replicate(req *ReplicateReq, unused *bool) error {
	if len(req.Entries) > 0 {
		if err := refuse(capReplicate); err != nil {
			return err
		}
	}
	if len(req.Log) > 0 {
		if err := refuse(capSessions); err != nil {
			return err
		}
	}
	if req.Handoff && c.rebuilding.Load() {
		// The node's rebuild copies these messages with the rest. It does
		// not copy session logs: their changes are applied below.
		req.Entries = nil
	}
	for _, e := range req.Entries {
		if e.ID != "" && !c.seen.add(e.ID) {
			continue // stored already
		}
		if err := store.Message.PutReplica(e.Contract, e.Topic, e.Payload, e.ExpiresAt); err != nil {
			log.ErrLogger.Error().Err(err).Str("context", "cluster.Replicate").Str("topic", e.Topic).Msg("unable to store replica from " + req.Node)
			if e.ID != "" {
				c.seen.remove(e.ID) // to store when it comes again
			}
			continue
		}
		// Recorded after the message: a crash between the two stores it
		// twice at worst, rather than not at all.
		if e.ID != "" {
			if err := store.Seen.Put(e.ID, e.ExpiresAt); err != nil {
				log.ErrLogger.Error().Err(err).Str("context", "cluster.Replicate").Msg("unable to record a replicated message's id")
			}
		}
	}
	for _, op := range req.Log {
		store.Log.Apply(op)
	}
	return nil
}

// Session terminated at origin. Inform remote Master nodes that the session is gone.
func (c *Cluster) connGone(conn *_Conn) error {
	if c == nil {
		return nil
	}

	// Inform every node the connection was routed to, not just the first.
	connNodesMu.Lock()
	var names []string
	for name := range conn.nodes {
		names = append(names, name)
	}
	connNodesMu.Unlock()

	var err error
	for _, name := range names {
		if n := c.nodes[name]; n != nil {
			if e := n.forward(
				&ClusterReq{
					Node:     c.thisNodeName,
					ConnGone: true,
					Conn: &ClusterSess{
						//RemoteAddr: sess.remoteAddr,
						ConnID: conn.connID}}); e != nil && err == nil {
				err = e
			}
		}
	}
	return err
}

// Returns worker id
func ClusterInit(configString json.RawMessage, self *string) int {
	if Globals.Cluster != nil {
		log.Fatal("cluster.ClusterInit", "Cluster already initialized.", nil)
	}

	// This is a standalone server, not initializing
	if len(configString) == 0 {
		log.Info("cluster.ClusterInit", "Running as a standalone server.")
		return 1
	}

	var config clusterConfig
	if err := json.Unmarshal(configString, &config); err != nil {
		log.Fatal("cluster.ClusterInit", "error parsing cluster config", err)
	}

	thisName := *self
	if thisName == "" {
		thisName = config.ThisName
	}

	// Name of the current node is not specified - disable clustering
	if thisName == "" {
		log.Info("cluster.ClusterInit", "Running as a standalone server.")
		return 1
	}

	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register(utp.Publish{})
	gob.Register(utp.Subscribe{})
	gob.Register(utp.Unsubscribe{})
	gob.Register(utp.Relay{})

	replicas := config.Replicas
	if replicas <= 0 {
		replicas = defaultClusterReplicas
	}
	rebuildTTL := defaultRebuildTTL
	if config.RebuildTTL != "" {
		d, err := time.ParseDuration(config.RebuildTTL)
		if err != nil {
			log.Fatal("cluster.ClusterInit", "invalid rebuild_ttl", err)
		}
		rebuildTTL = d
	}
	drainTimeout := defaultDrainTimeout
	if config.DrainTimeout != "" {
		d, err := time.ParseDuration(config.DrainTimeout)
		if err != nil {
			log.Fatal("cluster.ClusterInit", "invalid drain_timeout", err)
		}
		drainTimeout = d
	}
	Globals.Cluster = &Cluster{
		ringTarget:       config.RingVersion,
		ringVersion:      initialRingVersion(config.RingVersion),
		drainTimeout:     drainTimeout,
		thisNodeName:     thisName,
		nodes:            make(map[string]*ClusterNode),
		replicas:         replicas,
		asyncReplication: config.AsyncReplication,
		rebuildTTL:       rebuildTTL,
		replicaIDPrefix:  thisName + "/" + strconv.FormatInt(time.Now().UnixNano(), 36) + "/"}

	var nodeNames []string
	var tlsListenOn string
	for _, host := range config.Nodes {
		nodeNames = append(nodeNames, host.Name)

		if host.Name == thisName {
			Globals.Cluster.listenOn = host.Addr
			tlsListenOn = host.TLSAddr
			// Don't create a cluster member for this local instance
			continue
		}

		n := ClusterNode{
			address:    host.Addr,
			tlsAddress: host.TLSAddr,
			name:       host.Name,
			done:       make(chan bool, 1),
			repl:       make(chan replicaItem, replicationQueueSize),
			replDone:   make(chan struct{})}

		Globals.Cluster.nodes[host.Name] = &n
	}

	if config.TLS != nil {
		t, err := loadClusterTLS(config.TLS, thisName, tlsListenOn)
		if err != nil {
			log.Fatal("cluster.ClusterInit", "invalid cluster_config.tls", err)
		}
		Globals.Cluster.tls = t
		for _, n := range Globals.Cluster.nodes {
			if t.require && n.tlsAddress == "" {
				log.ErrLogger.Warn().Str("context", "cluster.ClusterInit").Msg("node '" + n.name + "' has no tls_addr, and TLS is required: it cannot be reached")
			}
		}
	}

	if len(Globals.Cluster.nodes) == 0 {
		// Cluster needs at least two nodes.
		log.Info("cluster.ClusterInit", "Invalid cluster size: 1")
	}

	Globals.Cluster.allNodes = append([]string(nil), nodeNames...)
	Globals.Cluster.fullRing = newRing(Globals.Cluster.ringVersion, nodeNames)
	logRingVersion(Globals.Cluster.ringVersion)
	if !Globals.Cluster.failoverInit(config.Failover) {
		Globals.Cluster.rehash(nil)
	}

	sort.Strings(nodeNames)
	workerId := sort.SearchStrings(nodeNames, thisName) + 1

	return workerId
}

// This is a session handler at a master node: forward messages from the master to the session origin.
func (c *_Conn) rpcWriteLoop() {
	// There is no readLoop for RPC, delete the session here
	defer func() {
		c.closeRPC()
		Globals.connCache.delete(c.connID)
		c.unsubAll()
	}()

	var unused bool

	// forward sends a message to the originating connection on the remote node.
	forward := func(outMsg lp.MessagePack) bool {
		if _, connected := c.clnode.client(); !connected {
			return false
		}
		buf, err := lp.Encode(outMsg)
		if err != nil {
			log.Error("conn.writeRpc", err.Error())
			return false
		}
		// The error is returned if the remote node is down. Which means the remote
		// session is also disconnected.
		if err := c.clnode.call("Cluster.Proxy", &ClusterResp{RespMsg: buf.Bytes(), FromConnID: c.connID}, &unused); err != nil {
			log.Error("conn.writeRPC", err.Error())
			return false
		}
		return true
	}

	for {
		select {
		case outMsg, ok := <-c.send:
			if !ok || !forward(outMsg) {
				return
			}
		case pub, ok := <-c.pub:
			// Messages published to this proxied subscriber, as the socket
			// write loop sends them for a direct one.
			if !ok || !forward(pub) {
				return
			}
		case stop := <-c.stop:
			// Shutdown is requested, don't care if the message is delivered
			if stop != nil {
				c.clnode.call("Cluster.Proxy", &ClusterResp{RespMsg: stop.([]byte), FromConnID: c.connID}, &unused)
			}
			return
		}
	}
}

// stopRPC asks a proxied session's write loop to stop. It does not block if
// a stop is already pending.
func (c *_Conn) stopRPC() {
	select {
	case c.stop <- nil:
	default:
	}
}

// Proxied session is being closed at the Master node
func (c *_Conn) closeRPC() {
	log.Info("cluster.closeRPC", "session closed at master")
}

// Start accepting connections.
// checkPeers stops the process if a configured node that answers runs a
// version from before replication, as unitdb v0.3.0: it cannot run in one cluster
// with this one, and the upgrade from it stops every node first
// (docs/rolling-deploys.md). Every later version takes an empty Replicate.
// A node that does not answer is not checked: a node of that version fails
// the pings of a leader of this one, and leaves its ring.
//
// It dials the plain address, which such a node listens on; with TLS
// required there is none, and a v0.3.0 node, which has no TLS, cannot be
// reached anyway.
func (c *Cluster) checkPeers() {
	if c.tls != nil && c.tls.require {
		return
	}
	for _, n := range c.nodes {
		conn, err := net.DialTimeout("tcp", n.address, time.Second)
		if err != nil {
			continue
		}
		endpoint := rpc.NewClient(conn)
		var unused bool
		call := endpoint.Go("Cluster.Replicate", &ReplicateReq{Node: c.thisNodeName}, &unused, make(chan *rpc.Call, 1))
		select {
		case <-call.Done:
			err = call.Error
		case <-time.After(2 * time.Second):
			err = nil
		}
		endpoint.Close()
		if missingMethod(err, capReplicate) {
			log.Fatal("cluster.checkPeers", "node '"+n.name+"' at "+n.address+" runs a version from before replication (unitdb v0.3.0 or older), which cannot run in one cluster with this one: stop every node before upgrading, see docs/rolling-deploys.md", err)
		}
	}
}

func (c *Cluster) Start() {
	c.checkPeers()

	// The plain listener, unless TLS is required; and the TLS one, beside it.
	var l *listener.Listener
	if c.tls == nil || !c.tls.require {
		var err error
		if l, err = listener.New(c.listenOn); err != nil {
			panic(err)
		}
		l.SetReadTimeout(120 * time.Second)
	}
	if c.tls != nil {
		tl, err := tls.Listen("tcp", c.tls.listenOn, c.tls.serverConfig())
		if err != nil {
			panic(err)
		}
		c.tlsInbound = tl
		go c.serveTLS(tl)
	}

	for _, n := range c.nodes {
		go n.reconnect()
		go n.replicateLoop(c.thisNodeName)
		go c.handoffLoop(n)
	}
	store.OnLogChange = c.replicateLog
	// The ids of replicas stored before a restart, oldest first, so that
	// the newest stay in the set.
	if ids, err := store.Seen.Recent(seenReplicas); err == nil {
		for i := len(ids) - 1; i >= 0; i-- {
			c.seen.add(ids[i])
		}
	} else {
		log.ErrLogger.Error().Err(err).Str("context", "cluster.Start").Msg("unable to load replicated messages' ids")
	}
	if c.replicas >= 2 && store.WasEmpty() && hasCapability(capReplicate) {
		c.rebuilding.Store(true)
		go c.rebuild()
	}

	if c.fo != nil {
		go c.run()
	}

	if err := rpc.Register(c); err != nil {
		log.Fatal("cluster.Start", "error registering rpc server", err)
	}

	if l != nil {
		go rpc.Accept(l)
	}
	//go l.Serve()

	// Before the service takes clients: get back the subscriptions of the
	// other nodes' clients this node holds, in case it restarted.
	c.resyncOnStart()

	listening := c.listenOn
	if c.tls != nil {
		listening = c.listenOn + ", TLS " + c.tls.listenOn
		if c.tls.require {
			listening = "TLS " + c.tls.listenOn + " only"
		}
	}
	log.ConnLogger.Info().Str("context", "cluster.Start").Msgf("Cluster of %d nodes initialized, node '%s' listening on [%s]", len(Globals.Cluster.nodes)+1,
		Globals.Cluster.thisNodeName, listening)
}

// drain has this node, shutting down, leave the cluster, before it closes its
// clients' connections: the others take it out of their rings, so that their
// requests go to the topics' new owners and its clients' subscriptions they
// held move, and it sends them the hints and replicas it has queued. It waits
// up to drainTimeout.
func (c *Cluster) drain() {
	if c == nil {
		return
	}
	deadline := time.Now().Add(c.drainTimeout)
	c.leaving.Store(true)
	if c.fo != nil {
		answer := make(chan bool, 1)
		c.fo.leave <- answer
		var wasLeader bool
		select {
		case wasLeader = <-answer:
		case <-time.After(time.Until(deadline)):
		}
		// The followers rehash on the second ping without this node.
		if wasLeader {
			time.Sleep(3 * c.fo.heartBeat)
		} else {
			for c.fo.pingsWithoutSelf.Load() < 3 && time.Now().Before(deadline) {
				time.Sleep(c.fo.heartBeat / 2)
			}
		}
	}
	for _, n := range c.nodes {
		c.handoff(n.name)
	}
	for time.Now().Before(deadline) {
		busy := false
		for _, n := range c.nodes {
			busy = busy || len(n.repl) > 0 || n.sending.Load()
		}
		if !busy {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	log.Info("cluster.drain", "left the cluster")
}

func (c *Cluster) shutdown() {
	// Globals.Cluster stays set: goroutines that read it run until the
	// process exits, and clearing it raced with them.
	if c == nil || !c.stopped.CompareAndSwap(false, true) {
		return
	}
	c.inbound.Close()
	if c.tlsInbound != nil {
		c.tlsInbound.Close()
	}

	if c.fo != nil {
		c.fo.done <- true
	}

	for _, n := range c.nodes {
		n.done <- true
		close(n.replDone)
	}

	log.Info("cluster.shutdown", "Cluster shut down")
}

// Recalculate the ring hash using provided list of nodes or only nodes in a non-failed state.
// Returns the list of nodes used for ring hash. A node leaving the cluster
// leaves itself out.
func (c *Cluster) rehash(nodes []string) []string {
	if c.leaving.Load() && nodes != nil {
		nodes = withoutNode(nodes, c.thisNodeName)
	}
	var ringKeys []string

	if nodes == nil {
		for _, node := range c.nodes {
			ringKeys = append(ringKeys, node.name)
		}
		ringKeys = append(ringKeys, c.thisNodeName)
	} else {
		ringKeys = append(ringKeys, nodes...)
	}
	ring := newRing(c.getRingVersion(), ringKeys)

	c.ringMu.Lock()
	c.ring = ring
	c.ringNodes = ringKeys
	c.ringMu.Unlock()

	return ringKeys
}

// rehashAndRebalance recalculates the ring hash over nodes, then moves this
// node's clients' subscriptions to where the new ring holds them. Nodes new to
// the ring are sent every subscription they hold, as they may have restarted.
func (c *Cluster) rehashAndRebalance(nodes []string) {
	c.ringMu.RLock()
	before := make(map[string]bool, len(c.ringNodes))
	for _, n := range c.ringNodes {
		before[n] = true
	}
	c.ringMu.RUnlock()

	added := make(map[string]bool)
	for _, n := range c.rehash(nodes) {
		if !before[n] {
			added[n] = true
		}
	}
	go c.rebalance(added)
	for n := range added {
		go c.handoff(n)
		go c.askResync(n)
	}
}

// ResyncReq asks a node to send the subscriptions of its clients it holds on
// the requesting node again.
type ResyncReq struct {
	// Name of the node sending this request
	Node string
	// Wait answers only once the subscriptions were sent, for a node that
	// starts: it takes clients once it holds their subscriptions again.
	// Nodes that do not know the field answer at once.
	Wait bool
}

// askResync asks a node back in the ring to send its clients' subscriptions
// here again. While it was out, this node stopped the connections it held for
// its clients, and their subscriptions with them; a node that stalled without
// its connections failing does not know, and is not resynced otherwise.
func (c *Cluster) askResync(name string) {
	n := c.nodes[name]
	if n == nil || !n.supports(capResync) {
		return
	}
	var unused bool
	if err := n.callTimeout("Cluster.Resync", &ResyncReq{Node: c.thisNodeName}, &unused, rebuildTimeout); err != nil && !n.lacks(err, capResync) {
		log.ErrLogger.Error().Err(err).Str("context", "cluster.askResync").Msg("unable to ask " + name + " to resync")
	}
}

// Resync sends the subscriptions of this node's clients held on the
// requesting node to it again. Called by a node that put this one back in
// its ring.
func (c *Cluster) Resync(req *ResyncReq, unused *bool) error {
	if err := refuse(capResync); err != nil {
		return err
	}
	if req.Wait {
		c.rebalance(map[string]bool{req.Node: true})
		return nil
	}
	go c.rebalance(map[string]bool{req.Node: true})
	return nil
}

// resyncOnStart asks every other node that answers to send the subscriptions
// of its clients this node holds, and waits for them, up to
// startResyncTimeout. A node that restarts before the others fail it over
// stays in their rings, so nothing else tells them it lost those
// subscriptions: without this, a publish on one of its topics just after it
// takes clients again would miss those subscribers. A node that is down, or
// does not know Resync, is skipped.
func (c *Cluster) resyncOnStart() {
	var wg sync.WaitGroup
	for _, n := range c.nodes {
		wg.Add(1)
		go func(n *ClusterNode) {
			defer wg.Done()
			endpoint, _, err := n.dial()
			if err != nil {
				return // not up: it has no clients here
			}
			defer endpoint.Close()
			var unused bool
			call := endpoint.Go("Cluster.Resync", &ResyncReq{Node: c.thisNodeName, Wait: true}, &unused, make(chan *rpc.Call, 1))
			select {
			case <-call.Done:
				if call.Error != nil && !missingMethod(call.Error, capResync) {
					log.ErrLogger.Warn().Err(call.Error).Str("context", "cluster.resyncOnStart").Msg("unable to resync from " + n.name)
				}
			case <-time.After(startResyncTimeout):
				log.ErrLogger.Warn().Str("context", "cluster.resyncOnStart").Msg("resync from " + n.name + " timed out")
			}
		}(n)
	}
	wg.Wait()
}

// rebalance moves every subscription of this node's clients to the nodes the
// current ring holds it on, sending it again to the nodes in resend, and
// retries the connections it could not move. It stops the connections this
// node holds for clients of nodes no longer in the ring: those clients are
// gone, and would otherwise keep their subscriptions here.
func (c *Cluster) rebalance(resend map[string]bool) {
	if Globals.connCache == nil {
		return
	}
	live := make(map[string]bool)
	for _, n := range c.getRingNodes() {
		live[n] = true
	}
	var pending []*_Conn
	for _, conn := range Globals.connCache.all() {
		if conn.clnode != nil {
			if !live[conn.clnode.name] {
				conn.stopRPC()
			}
			continue
		}
		pending = append(pending, conn)
	}
	for attempt := 1; len(pending) > 0; attempt++ {
		var failed []*_Conn
		for _, conn := range pending {
			if !conn.rehome(resend, attempt == rebalanceAttempts) {
				failed = append(failed, conn)
			}
		}
		if len(failed) == 0 || attempt == rebalanceAttempts {
			return
		}
		pending = failed
		time.Sleep(rebalanceRetry)
	}
}

// getRingNodes returns the nodes in the current ring.
func (c *Cluster) getRingNodes() []string {
	c.ringMu.RLock()
	defer c.ringMu.RUnlock()
	return c.ringNodes
}

// holders returns where a subscription to topic is held: here (local), and on
// the other nodes listed. A topic is held by its owner alone, and a wildcard
// here and by every other node in the ring.
func (c *Cluster) holders(contract uint32, topic string) (local bool, nodes []string) {
	if c == nil {
		return true, nil
	}
	c.ringMu.RLock()
	ring, ringNodes := c.ring, c.ringNodes
	c.ringMu.RUnlock()
	if isWildcardTopic(topic) {
		for _, n := range ringNodes {
			if n != c.thisNodeName {
				nodes = append(nodes, n)
			}
		}
		return true, nodes
	}
	if owner := ring.Get(topicRingKey(contract, topic)); owner != c.thisNodeName {
		return false, []string{owner}
	}
	return true, nil
}

// subscribeAt subscribes the client of conn on node name.
func (c *Cluster) subscribeAt(name string, sub *utp.Subscription, conn *_Conn) error {
	n := c.nodes[name]
	if n == nil {
		return errors.New("cluster.subscribeAt: no node " + name)
	}
	return c.forwardTo(n, &utp.Subscribe{Subscriptions: []*utp.Subscription{sub}}, conn)
}

// unsubscribeAt unsubscribes the client of conn on node name.
func (c *Cluster) unsubscribeAt(name string, sub *utp.Subscription, conn *_Conn) error {
	n := c.nodes[name]
	if n == nil {
		return errors.New("cluster.unsubscribeAt: no node " + name)
	}
	return c.forwardTo(n, &utp.Unsubscribe{Subscriptions: []*utp.Subscription{sub}}, conn)
}

// getRing returns the current ring hash.
func (c *Cluster) getRing() *rh.Ring {
	c.ringMu.RLock()
	defer c.ringMu.RUnlock()
	return c.ring
}
