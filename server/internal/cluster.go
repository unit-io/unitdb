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
	"encoding/gob"
	"encoding/json"
	"errors"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/message/security"
	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/net/listener"
	rh "github.com/unit-io/unitdb/server/internal/pkg/hash"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
)

const (
	// Default timeout before attempting to reconnect to a node
	defaultClusterReconnect = 200 * time.Millisecond
	// Number of replicas in ringhash
	clusterHashReplicas = 20
)

type _ClusterNodeConfig struct {
	name string `json:"name"`
	addr string `json:"addr"`
}

type _ClusterConfig struct {
	// List of all members of the cluster, including this member
	nodes []_ClusterNodeConfig `json:"nodes"`
	// Name of this cluster node
	thisName string `json:"self"`
	// Failover configuration
	failover *_ClusterFailoverConfig
}

// _ClusterNode is a client's connection to another node.
type _ClusterNode struct {
	lock sync.Mutex

	// RPC endpoint
	endpoint *rpc.Client
	// True if the endpoint is believed to be connected
	connected bool
	// True if a go routine is trying to reconnect the node
	reconnecting bool
	// TCP address in the form host:port
	address string
	// Name of the node
	name string

	// A number of times this node has failed in a row
	failCount int

	// Channel for shutting down the runner; buffered, 1
	done chan bool
}

// _ClusterSess is a basic info on a remote session where the message was created.
type _ClusterSess struct {
	// IP address of the client. For long polling this is the IP of the last poll
	remoteAddr string

	// Connection ID
	connID uid.LID

	// Client ID
	clientID uid.ID
}

// _ClusterReq is a Proxy to Master request message.
type _ClusterReq struct {
	// Name of the node sending this request
	node string

	// Ring hash signature of the node sending this request
	// Signature must match the signature of the receiver, otherwise the
	// Cluster is desynchronized.
	signature string

	msgSub   *lp.Subscribe
	msgPub   *lp.Publish
	msgUnsub *lp.Unsubscribe
	topic    *security.Topic
	reqType  uint8
	message  *message.Message

	// Originating session
	conn *_ClusterSess
	// True if the original session has disconnected
	connGone bool
}

// _ClusterResp is a Master to Proxy response message.
type _ClusterResp struct {
	respType uint8
	msgSub   *lp.Subscribe
	msgPub   *lp.Publish
	msgUnsub *lp.Unsubscribe
	msg      []byte
	topic    *security.Topic
	message  *message.Message
	// Connection ID to forward message to, if any.
	fromConnID uid.LID
}

// Handle outbound node communication: read messages from the channel, forward to remote nodes.
// FIXME(gene): this will drain the outbound queue in case of a failure: all unprocessed messages will be dropped.
// Maybe it's a good thing, maybe not.
func (n *_ClusterNode) reconnect() {
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
	var err error
	for {
		// Attempt to reconnect right away
		if n.endpoint, err = rpc.Dial("tcp", n.address); err == nil {
			if reconnTicker != nil {
				reconnTicker.Stop()
			}
			n.lock.Lock()
			n.connected = true
			n.reconnecting = false
			n.lock.Unlock()
			log.Info("cluster.reconnect", "connection established "+n.name)
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
			if n.endpoint != nil {
				n.endpoint.Close()
			}
			n.lock.Lock()
			n.connected = false
			n.reconnecting = false
			n.lock.Unlock()
			log.Info("cluster", "node shut down completed "+n.name)
			return
		}
	}
}

func (n *_ClusterNode) call(proc string, msg, resp interface{}) error {
	if !n.connected {
		return errors.New("cluster.call: node '" + n.name + "' not connected")
	}

	if err := n.endpoint.Call(proc, msg, resp); err != nil {
		log.Fatal("cluster.call", "call failed to "+n.name, err)

		n.lock.Lock()
		if n.connected {
			n.endpoint.Close()
			n.connected = false
			go n.reconnect()
		}
		n.lock.Unlock()
		return err
	}

	return nil
}

func (n *_ClusterNode) callAsync(proc string, msg, resp interface{}, done chan *rpc.Call) *rpc.Call {
	if done != nil && cap(done) == 0 {
		log.Fatal("cluster.callAsync", "RPC done channel is unbuffered", nil)
	}

	if !n.connected {
		call := &rpc.Call{
			ServiceMethod: proc,
			Args:          msg,
			Reply:         resp,
			Error:         errors.New("cluster.callAsync: node '" + n.name + "' not connected"),
			Done:          done,
		}
		if done != nil {
			done <- call
		}
		return call
	}

	myDone := make(chan *rpc.Call, 1)
	go func() {
		select {
		case call := <-myDone:
			if call.Error != nil {
				n.lock.Lock()
				if n.connected {
					n.endpoint.Close()
					n.connected = false
					go n.reconnect()
				}
				n.lock.Unlock()
			}

			if done != nil {
				done <- call
			}
		}
	}()

	call := n.endpoint.Go(proc, msg, resp, myDone)
	call.Done = done

	return call
}

// Proxy forwards message to master
func (n *_ClusterNode) forward(msg *_ClusterReq) error {
	log.Info("cluster.forward", "forwarding request to node "+n.name)
	msg.node = Globals.Cluster.thisNodeName
	rejected := false
	err := n.call("Cluster.Master", msg, &rejected)
	if err == nil && rejected {
		err = errors.New("cluster.forward: master node out of sync")
	}
	return err
}

// _Cluster is the representation of the cluster.
type _Cluster struct {
	// Cluster nodes with RPC endpoints
	nodes map[string]*_ClusterNode
	// Name of the local node
	thisNodeName string

	// Resolved address to listed on
	listenOn string

	// Socket for inbound connections
	inbound *net.TCPListener
	// Ring hash for mapping topic names to nodes
	ring *rh.Ring

	// Failover parameters. Could be nil if failover is not enabled
	fo *_ClusterFailover
}

// master at topic's master node receives C2S messages from topic's proxy nodes.
// The message is treated like it came from a session: find or create a session locally,
// dispatch the message to it like it came from a normal ws/lp connection.
// Called by a remote node.
func (c *_Cluster) master(msg *_ClusterReq, rejected *bool) error {
	log.Info("cluster.Master", "master request received from node "+msg.node)

	// Find the local connection associated with the given remote connection.
	conn := Globals.connCache.get(msg.conn.connID)

	if msg.connGone {
		// Original session has disconnected. Tear down the local proxied session.
		if conn != nil {
			conn.stop <- nil
		}
	} else if msg.signature == c.ring.Signature() {
		// This cluster member received a request for a topic it owns.

		if conn == nil {
			// If the session is not found, create it.
			node := Globals.Cluster.nodes[msg.node]
			if node == nil {
				log.Error("cluster.Master", "request from an unknown node "+msg.node)
				return nil
			}

			log.Info("cluster.Master", "new connection request"+string(msg.conn.connID))
			conn = Globals.Service.newRpcConn(node, msg.conn.connID, msg.conn.clientID)
			go conn.rpcWriteLoop()
		}
		// Update session params which may have changed since the last call.
		conn.connid = msg.conn.connID

		switch msg.reqType {
		case message.SUBSCRIBE:
			conn.handle(msg.msgSub)
		case message.UNSUBSCRIBE:
			conn.handle(msg.msgUnsub)
		case message.PUBLISH:
			conn.handle(msg.msgPub)
		}
	} else {
		// Reject the request: wrong signature, cluster is out of sync.
		*rejected = true
	}

	return nil
}

// Dispatch receives messages from the master node addressed to a specific local connection.
func (_Cluster) proxy(resp *_ClusterResp, unused *bool) error {
	log.Info("cluster.Proxy", "response from Master for connection "+string(resp.fromConnID))

	// This cluster member received a response from topic owner to be forwarded to a connection
	// Find appropriate connection, send the message to it

	if conn := Globals.connCache.get(resp.fromConnID); conn != nil {
		if !conn.SendRawBytes(resp.msg) {
			log.Error("cluster.Proxy", "Proxy: timeout")
		}
	} else {
		log.ErrLogger.Error().Str("context", "cluster.Proxy").Uint64("connid", uint64(resp.fromConnID)).Msg("master response for unknown session")
	}

	return nil
}

// Given contract name, find appropriate cluster node to route message to
func (c *_Cluster) nodeForContract(contract string) *_ClusterNode {
	key := c.ring.Get(contract)
	if key == c.thisNodeName {
		log.Error("cluster", "request to route to self")
		// Do not route to self
		return nil
	}

	node := Globals.Cluster.nodes[key]
	if node == nil {
		log.Error("cluster", "no node for contract "+contract+key)
	}
	return node
}

func (c *_Cluster) isRemoteContract(contract string) bool {
	if c == nil {
		// Cluster not initialized, all contracts are local
		return false
	}
	return c.ring.Get(contract) != c.thisNodeName
}

// Forward client message to the Master (cluster node which owns the topic)
func (c *_Cluster) routeToContract(msg lp.Packet, topic *security.Topic, msgType uint8, m *message.Message, conn *_Conn) error {
	// Find the cluster node which owns the topic, then forward to it.
	n := c.nodeForContract(string(conn.clientid.Contract()))
	if n == nil {
		return errors.New("cluster.routeToContract: attempt to route to non-existent node")
	}

	// Save node name: it's need in order to inform relevant nodes when the session is disconnected
	if conn.nodes == nil {
		conn.nodes = make(map[string]bool)
	}
	conn.nodes[n.name] = true

	// var msgSub,msgPub,msgUnsub lp.Packet
	var msgSub *lp.Subscribe
	var msgPub *lp.Publish
	var msgUnsub *lp.Unsubscribe
	switch msgType {
	case message.SUBSCRIBE:
		msgSub = msg.(*lp.Subscribe)
		msgSub.IsForwarded = true
	case message.UNSUBSCRIBE:
		msgUnsub = msg.(*lp.Unsubscribe)
		msgUnsub.IsForwarded = true
	case message.PUBLISH:
		msgPub = msg.(*lp.Publish)
		msgPub.IsForwarded = true
	}
	return n.forward(
		&_ClusterReq{
			node:      c.thisNodeName,
			signature: c.ring.Signature(),
			msgSub:    msgSub,
			msgUnsub:  msgUnsub,
			msgPub:    msgPub,
			topic:     topic,
			reqType:   msgType,
			message:   m,
			conn: &_ClusterSess{
				//RemoteAddr: conn.(),
				connID:   conn.connid,
				clientID: conn.clientid}})
}

// Session terminated at origin. Inform remote Master nodes that the session is gone.
func (c *_Cluster) connGone(conn *_Conn) error {
	if c == nil {
		return nil
	}

	// Save node name: it's need in order to inform relevant nodes when the connection is gone
	for name := range conn.nodes {
		n := c.nodes[name]
		if n != nil {
			return n.forward(
				&_ClusterReq{
					node:     c.thisNodeName,
					connGone: true,
					conn: &_ClusterSess{
						//RemoteAddr: sess.remoteAddr,
						connID: conn.connid}})
		}
	}
	return nil
}

// ClusterInit returns worker id
func ClusterInit(configString json.RawMessage, self *string) int {
	if Globals.Cluster != nil {
		log.Fatal("cluster.ClusterInit", "Cluster already initialized.", nil)
	}

	// This is a standalone server, not initializing
	if len(configString) == 0 {
		log.Info("cluster.ClusterInit", "Running as a standalone server.")
		return 1
	}

	var config _ClusterConfig
	if err := json.Unmarshal(configString, &config); err != nil {
		log.Fatal("cluster.ClusterInit", "error parsing cluster config", err)
	}

	thisName := *self
	if thisName == "" {
		thisName = config.thisName
	}

	// Name of the current node is not specified - disable clustering
	if thisName == "" {
		log.Info("cluster.ClusterInit", "Running as a standalone server.")
		return 1
	}

	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register(lp.Publish{})
	gob.Register(lp.Subscribe{})
	gob.Register(lp.Unsubscribe{})

	Globals.Cluster = &_Cluster{
		thisNodeName: thisName,
		nodes:        make(map[string]*_ClusterNode)}

	var nodeNames []string
	for _, host := range config.nodes {
		nodeNames = append(nodeNames, host.name)

		if host.name == thisName {
			Globals.Cluster.listenOn = host.addr
			// Don't create a cluster member for this local instance
			continue
		}

		n := _ClusterNode{
			address: host.addr,
			name:    host.name,
			done:    make(chan bool, 1)}

		Globals.Cluster.nodes[host.name] = &n
	}

	if len(Globals.Cluster.nodes) == 0 {
		// Cluster needs at least two nodes.
		log.Info("cluster.ClusterInit", "Invalid cluster size: 1")
	}

	if !Globals.Cluster.failoverInit(config.failover) {
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
		Globals.connCache.delete(c.connid)
		c.unsubAll()
	}()

	var unused bool

	for {
		select {
		case msg, ok := <-c.send:
			if !ok || c.clnode.endpoint == nil {
				// channel closed
				return
			}
			m, err := lp.Encode(c.proto, msg)
			if err != nil {
				log.Error("conn.writeRpc", err.Error())
				return
			}
			// The error is returned if the remote node is down. Which means the remote
			// session is also disconnected.
			if err := c.clnode.call("Cluster.Proxy", &_ClusterResp{msg: m.Bytes(), fromConnID: c.connid}, &unused); err != nil {
				log.Error("conn.writeRPC", err.Error())
				return
			}
		case msg := <-c.stop:
			// Shutdown is requested, don't care if the message is delivered
			if msg != nil {
				c.clnode.call("Cluster.Proxy", &_ClusterResp{msg: msg.([]byte), fromConnID: c.connid}, &unused)
			}
			return
		}
	}
}

// Proxied session is being closed at the Master node
func (c *_Conn) closeRPC() {
	log.Info("cluster.closeRPC", "session closed at master")
}

// Start accepting connections.
func (c *_Cluster) Start() {
	l, err := listener.New(c.listenOn)
	if err != nil {
		panic(err)
	}

	l.SetReadTimeout(120 * time.Second)

	for _, n := range c.nodes {
		go n.reconnect()
	}

	if c.fo != nil {
		go c.run()
	}

	err = rpc.Register(c)
	if err != nil {
		log.Fatal("cluster.Start", "error registering rpc server", err)
	}

	go rpc.Accept(l)
	//go l.Serve()

	log.ConnLogger.Info().Str("context", "cluster.Start").Msgf("Cluster of %d nodes initialized, node '%s' listening on [%s]", len(Globals.Cluster.nodes)+1,
		Globals.Cluster.thisNodeName, c.listenOn)
}

func (c *_Cluster) shutdown() {
	if Globals.Cluster == nil {
		return
	}
	Globals.Cluster = nil
	c.inbound.Close()

	if c.fo != nil {
		c.fo.done <- true
	}

	for _, n := range c.nodes {
		n.done <- true
	}

	log.Info("cluster.shutdown", "Cluster shut down")
}

// Recalculate the ring hash using provided list of nodes or only nodes in a non-failed state.
// Returns the list of nodes used for ring hash.
func (c *_Cluster) rehash(nodes []string) []string {
	ring := rh.NewRing(clusterHashReplicas, nil)

	var ringKeys []string

	if nodes == nil {
		for _, node := range c.nodes {
			ringKeys = append(ringKeys, node.name)
		}
		ringKeys = append(ringKeys, c.thisNodeName)
	} else {
		for _, name := range nodes {
			ringKeys = append(ringKeys, name)
		}
	}
	ring.Add(ringKeys...)

	c.ring = ring

	return ringKeys
}
