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
	"fmt"
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
	"github.com/unit-io/unitdb/server/utp"
)

const (
	// Default timeout before attempting to reconnect to a node
	defaultClusterReconnect = 200 * time.Millisecond
	// Number of replicas in ringhash
	clusterHashReplicas = 20
)

type clusterNodeConfig struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

type clusterConfig struct {
	// List of all members of the cluster, including this member
	Nodes []clusterNodeConfig `json:"nodes"`
	// Name of this cluster node
	ThisName string `json:"self"`
	// Failover configuration
	Failover *clusterFailoverConfig
}

// ClusterNode is a client's connection to another node.
type ClusterNode struct {
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
}

// ClusterReq is a Proxy to Master request message.
type ClusterReq struct {
	// Name of the node sending this request
	Node string

	// Ring hash signature of the node sending this request
	// Signature must match the signature of the receiver, otherwise the
	// Cluster is desynchronized.
	Signature string

	SubMsg   *utp.Subscribe
	PubMsg   *utp.Publish
	UnsubMsg *utp.Unsubscribe
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
	Message  *message.Message
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

func (n *ClusterNode) call(proc string, reqMsg, respMsg interface{}) error {
	if !n.connected {
		return errors.New("cluster.call: node '" + n.name + "' not connected")
	}

	if err := n.endpoint.Call(proc, reqMsg, respMsg); err != nil {
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

func (n *ClusterNode) callAsync(proc string, reqMsg, respMsg interface{}, done chan *rpc.Call) *rpc.Call {
	if done != nil && cap(done) == 0 {
		log.Fatal("cluster.callAsync", "RPC done channel is unbuffered", nil)
	}

	if !n.connected {
		call := &rpc.Call{
			ServiceMethod: proc,
			Args:          reqMsg,
			Reply:         respMsg,
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
		call := <-myDone
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
	}()

	call := n.endpoint.Go(proc, reqMsg, respMsg, myDone)
	call.Done = done

	return call
}

// Proxy forwards message to master
func (n *ClusterNode) forward(forwMsg *ClusterReq) error {
	log.Info("cluster.forward", "forwarding request to node "+n.name)
	forwMsg.Node = Globals.Cluster.thisNodeName
	rejected := false
	err := n.call("Cluster.Master", forwMsg, &rejected)
	if err == nil && rejected {
		err = errors.New("cluster.forward: master node out of sync")
	}
	return err
}

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
	// Ring hash for mapping topic names to nodes
	ring *rh.Ring

	// Failover parameters. Could be nil if failover is not enabled
	fo *clusterFailover
}

// Master at topic's master node receives C2S messages from topic's proxy nodes.
// The message is treated like it came from a session: find or create a session locally,
// dispatch the message to it like it came from a normal ws/lp connection.
// Called by a remote node.
func (c *Cluster) Master(reqMsg *ClusterReq, rejected *bool) error {
	log.Info("cluster.Master", "master request received from node "+reqMsg.Node)

	// Find the local connection associated with the given remote connection.
	conn := Globals.connCache.get(reqMsg.Conn.ConnID)

	if reqMsg.ConnGone {
		// Original session has disconnected. Tear down the local proxied session.
		if conn != nil {
			conn.stop <- nil
		}
	} else if reqMsg.Signature == c.ring.Signature() {
		// This cluster member received a request for a topic it owns.

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
		// Update session params which may have changed since the last call.
		conn.connID = reqMsg.Conn.ConnID
		conn.clientID = reqMsg.Conn.ClientID

		switch reqMsg.Type {
		case message.SUBSCRIBE:
			conn.handler(reqMsg.SubMsg)
		case message.UNSUBSCRIBE:
			conn.handler(reqMsg.UnsubMsg)
		case message.PUBLISH:
			conn.handler(reqMsg.PubMsg)
		}
	} else {
		// Reject the request: wrong signature, cluster is out of sync.
		*rejected = true
	}

	return nil
}

// Dispatch receives messages from the master node addressed to a specific local connection.
func (Cluster) Proxy(resp *ClusterResp, unused *bool) error {
	log.Info("cluster.Proxy", "response from Master for connection "+fmt.Sprint(resp.FromConnID))

	// This cluster member received a response from topic owner to be forwarded to a connection
	// Find appropriate connection, send the message to it

	if conn := Globals.connCache.get(resp.FromConnID); conn != nil {
		if !conn.SendRawBytes(resp.RespMsg) {
			log.Error("cluster.Proxy", "Proxy: timeout")
		}
	} else {
		log.ErrLogger.Error().Str("context", "cluster.Proxy").Uint64("connid", uint64(resp.FromConnID)).Msg("master response for unknown session")
	}

	return nil
}

// Given contract name, find appropriate cluster node to route message to
func (c *Cluster) nodeForContract(contract string) *ClusterNode {
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

func (c *Cluster) isRemoteContract(contract string) bool {
	if c == nil {
		// Cluster not initialized, all contracts are local
		return false
	}
	return c.ring.Get(contract) != c.thisNodeName
}

// Forward client message to the Master (cluster node which owns the topic)
func (c *Cluster) routeToContract(msg lp.MessagePack, topic *security.Topic, msgType uint8, m *message.Message, conn *_Conn) error {
	// Find the cluster node which owns the topic, then forward to it.
	n := c.nodeForContract(fmt.Sprint(conn.clientID.Contract()))
	if n == nil {
		return errors.New("cluster.routeToContract: attempt to route to non-existent node")
	}

	// Save node name: it's need in order to inform relevant nodes when the session is disconnected
	if conn.nodes == nil {
		conn.nodes = make(map[string]bool)
	}
	conn.nodes[n.name] = true

	// var msgSub,msgPub,msgUnsub lp.Packet
	var subMsg *utp.Subscribe
	var pubMsg *utp.Publish
	var unsubMsg *utp.Unsubscribe
	switch msgType {
	case message.SUBSCRIBE:
		subMsg = msg.(*utp.Subscribe)
		subMsg.IsForwarded = true
	case message.UNSUBSCRIBE:
		unsubMsg = msg.(*utp.Unsubscribe)
		unsubMsg.IsForwarded = true
	case message.PUBLISH:
		pubMsg = msg.(*utp.Publish)
		pubMsg.IsForwarded = true
	}
	return n.forward(
		&ClusterReq{
			Node:      c.thisNodeName,
			Signature: c.ring.Signature(),
			SubMsg:    subMsg,
			UnsubMsg:  unsubMsg,
			PubMsg:    pubMsg,
			Topic:     topic,
			Type:      msgType,
			Message:   m,
			Conn: &ClusterSess{
				//RemoteAddr: conn.(),
				ConnID:   conn.connID,
				SessID:   conn.sessID,
				ClientID: conn.clientID}})
}

// Session terminated at origin. Inform remote Master nodes that the session is gone.
func (c *Cluster) connGone(conn *_Conn) error {
	if c == nil {
		return nil
	}

	// Save node name: it's need in order to inform relevant nodes when the connection is gone
	for name := range conn.nodes {
		n := c.nodes[name]
		if n != nil {
			return n.forward(
				&ClusterReq{
					Node:     c.thisNodeName,
					ConnGone: true,
					Conn: &ClusterSess{
						//RemoteAddr: sess.remoteAddr,
						ConnID: conn.connID}})
		}
	}
	return nil
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

	Globals.Cluster = &Cluster{
		thisNodeName: thisName,
		nodes:        make(map[string]*ClusterNode)}

	var nodeNames []string
	for _, host := range config.Nodes {
		nodeNames = append(nodeNames, host.Name)

		if host.Name == thisName {
			Globals.Cluster.listenOn = host.Addr
			// Don't create a cluster member for this local instance
			continue
		}

		n := ClusterNode{
			address: host.Addr,
			name:    host.Name,
			done:    make(chan bool, 1)}

		Globals.Cluster.nodes[host.Name] = &n
	}

	if len(Globals.Cluster.nodes) == 0 {
		// Cluster needs at least two nodes.
		log.Info("cluster.ClusterInit", "Invalid cluster size: 1")
	}

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

	for {
		select {
		case outMsg, ok := <-c.send:
			if !ok || c.clnode.endpoint == nil {
				// channel closed
				return
			}
			buf, err := lp.Encode(outMsg)
			if err != nil {
				log.Error("conn.writeRpc", err.Error())
				return
			}
			// The error is returned if the remote node is down. Which means the remote
			// session is also disconnected.
			if err := c.clnode.call("Cluster.Proxy", &ClusterResp{RespMsg: buf.Bytes(), FromConnID: c.connID}, &unused); err != nil {
				log.Error("conn.writeRPC", err.Error())
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

// Proxied session is being closed at the Master node
func (c *_Conn) closeRPC() {
	log.Info("cluster.closeRPC", "session closed at master")
}

// Start accepting connections.
func (c *Cluster) Start() {
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

func (c *Cluster) shutdown() {
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
func (c *Cluster) rehash(nodes []string) []string {
	ring := rh.NewRing(clusterHashReplicas, nil)

	var ringKeys []string

	if nodes == nil {
		for _, node := range c.nodes {
			ringKeys = append(ringKeys, node.name)
		}
		ringKeys = append(ringKeys, c.thisNodeName)
	} else {
		ringKeys = append(ringKeys, nodes...)
	}
	ring.Add(ringKeys...)

	c.ring = ring

	return ringKeys
}
