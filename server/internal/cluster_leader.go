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
	"log"
	"math/rand"
	"net/rpc"
	"time"
)

// Cluster methods related to leader node election. Based on ideas from Raft protocol.
// The leader node issues heartbeats to follower nodes. If the follower node fails enough
// times, the leader node annouces it dead and initiates rehashing: it regenerates ring hash with
// only live nodes and communicates the new list of nodes to followers. They in turn do their
// rehashing using the new list. When the dead node is revived, rehashing happens again.

// Failover config
type _ClusterFailover struct {
	// Current leader
	leader string
	// Current election term
	term int
	// Hearbeat interval
	heartBeat time.Duration
	// Vote timeout: the number of missed heartbeats before a new election is initiated.
	voteTimeout int

	// The list of nodes the leader considers active
	activeNodes []string
	// The number of heartbeats a node can fail before being declared dead
	nodeFailCountLimit int

	// Channel for processing leader pings
	leaderPing chan *_ClusterPing
	// Channel for processing election votes
	electionVote chan *_ClusterVote
	// Channel for stopping the failover runner
	done chan bool
}

type _ClusterFailoverConfig struct {
	// Failover is enabled
	enabled bool `json:"enabled"`
	// Time in milliseconds between heartbeats
	heartbeat int `json:"heartbeat"`
	// Number of failed heartbeats before a leader election is initiated.
	voteAfter int `json:"vote_after"`
	// Number of failures before a node is considered dead
	nodeFailAfter int `json:"node_fail_after"`
}

// _ClusterPing is content of a leader node ping to a follower node.
type _ClusterPing struct {
	// Name of the leader node
	leader string
	// Election term
	term int
	// Ring hash signature that represents the cluster
	signature string
	// Names of nodes currently active in the cluster
	nodes []string
}

// _ClusterVoteRequest is a request from a leader candidate to a node to vote for the candidate.
type _ClusterVoteRequest struct {
	// Candidate node which issued this request
	node string
	// Election term
	term int
}

// _ClusterVoteResponse is a vote from a node.
type _ClusterVoteResponse struct {
	// Actual vote
	result bool
	// Node's term after the vote
	term int
}

// _ClusterVote is a vote request and a response in leader election.
type _ClusterVote struct {
	req  *_ClusterVoteRequest
	resp chan _ClusterVoteResponse
}

func (c *_Cluster) failoverInit(config *_ClusterFailoverConfig) bool {
	if config == nil || !config.enabled {
		return false
	}
	if len(c.nodes) < 2 {
		log.Printf("cluster: failover disabled; need at least 3 nodes, got %d", len(c.nodes)+1)
		return false
	}

	// Generate ring hash on the assumption that all nodes are alive and well.
	// This minimizes rehashing during normal operations.
	var activeNodes []string
	for _, node := range c.nodes {
		activeNodes = append(activeNodes, node.name)
	}
	activeNodes = append(activeNodes, c.thisNodeName)
	c.rehash(activeNodes)

	// Random heartbeat ticker: 0.75 * config.HeartBeat + random(0, 0.5 * config.HeartBeat)
	rand.Seed(time.Now().UnixNano())
	hb := time.Duration(config.heartbeat) * time.Millisecond
	hb = (hb >> 1) + (hb >> 2) + time.Duration(rand.Intn(int(hb>>1)))

	c.fo = &_ClusterFailover{
		activeNodes:        activeNodes,
		heartBeat:          hb,
		voteTimeout:        config.voteAfter,
		nodeFailCountLimit: config.nodeFailAfter,
		leaderPing:         make(chan *_ClusterPing, config.voteAfter),
		electionVote:       make(chan *_ClusterVote, len(c.nodes)),
		done:               make(chan bool, 1)}

	log.Println("cluster: failover mode enabled")

	return true
}

// ping is called by the leader node to assert leadership and check status
// of the followers.
func (c *_Cluster) ping(ping *_ClusterPing, unused *bool) error {
	select {
	case c.fo.leaderPing <- ping:
	default:
	}
	return nil
}

// vote processes request for a vote from a candidate.
func (c *_Cluster) vote(vreq *_ClusterVoteRequest, response *_ClusterVoteResponse) error {
	respChan := make(chan _ClusterVoteResponse, 1)

	c.fo.electionVote <- &_ClusterVote{
		req:  vreq,
		resp: respChan}

	*response = <-respChan

	return nil
}

func (c *_Cluster) sendPings() {
	rehash := false

	for _, node := range c.nodes {
		unused := false
		err := node.call("Cluster.Ping", &_ClusterPing{
			leader:    c.thisNodeName,
			term:      c.fo.term,
			signature: c.ring.Signature(),
			nodes:     c.fo.activeNodes}, &unused)

		if err != nil {
			node.failCount++
			if node.failCount == c.fo.nodeFailCountLimit {
				// Node failed too many times
				rehash = true
			}
		} else {
			if node.failCount >= c.fo.nodeFailCountLimit {
				// Node has recovered
				rehash = true
			}
			node.failCount = 0
		}
	}

	if rehash {
		var activeNodes []string
		for _, node := range c.nodes {
			if node.failCount < c.fo.nodeFailCountLimit {
				activeNodes = append(activeNodes, node.name)
			}
		}
		activeNodes = append(activeNodes, c.thisNodeName)

		c.fo.activeNodes = activeNodes
		c.rehash(activeNodes)

		log.Println("cluster: initiating failover rehash for nodes", activeNodes)
		//globals.hub.rehash <- true
	}
}

func (c *_Cluster) electLeader() {
	// Increment the term (voting for myself in this term) and clear the leader
	c.fo.term++
	c.fo.leader = ""

	log.Println("cluster: leading new election for term", c.fo.term)

	nodeCount := len(c.nodes)
	// Number of votes needed to elect the leader
	expectVotes := (nodeCount+1)>>1 + 1
	done := make(chan *rpc.Call, nodeCount)

	// Send async requests for votes to other nodes
	for _, node := range c.nodes {
		response := _ClusterVoteResponse{}
		node.callAsync("Cluster.Vote", &_ClusterVoteRequest{
			node: c.thisNodeName,
			term: c.fo.term}, &response, done)
	}

	// Number of votes received (1 vote for self)
	voteCount := 1
	timeout := time.NewTimer(c.fo.heartBeat>>1 + c.fo.heartBeat)
	// Wait for one of the following
	// 1. More than half of the nodes voting in favor
	// 2. All nodes responded.
	// 3. Timeout.
	for i := 0; i < nodeCount && voteCount < expectVotes; {
		select {
		case call := <-done:
			if call.Error == nil {
				if call.Reply.(*_ClusterVoteResponse).result {
					// Vote in my favor
					voteCount++
				} else if c.fo.term < call.Reply.(*_ClusterVoteResponse).term {
					// Vote against me. Abandon vote: this node's term is behind the cluster
					i = nodeCount
					voteCount = 0
				}
			}

			i++
		case <-timeout.C:
			// break the loop
			i = nodeCount
		}
	}

	if voteCount >= expectVotes {
		// Current node elected as the leader
		c.fo.leader = c.thisNodeName
		log.Println("Elected myself as a new leader")
	}
}

// Go routine that processes calls related to leader election and maintenance.
func (c *_Cluster) run() {

	ticker := time.NewTicker(c.fo.heartBeat)

	missed := 0
	// Don't rehash immediately on the first ping. If this node just came onlyne, leader will
	// account it on the next ping. Otherwise it will be rehashing twice.
	rehashSkipped := false

	for {
		select {
		case <-ticker.C:
			if c.fo.leader == c.thisNodeName {
				// I'm the leader, send pings
				c.sendPings()
			} else {
				missed++
				if missed >= c.fo.voteTimeout {
					// Elect the leader
					missed = 0
					c.electLeader()
				}
			}
		case ping := <-c.fo.leaderPing:
			// Ping from a leader.

			if ping.term < c.fo.term {
				// This is a ping from a stale leader. Ignore.
				log.Println("cluster: ping from a stale leader", ping.term, c.fo.term, ping.leader, c.fo.leader)
				continue
			}

			if ping.term > c.fo.term {
				c.fo.term = ping.term
				c.fo.leader = ping.leader
				log.Printf("cluster: leader '%s' elected", c.fo.leader)
			} else if ping.leader != c.fo.leader {
				if c.fo.leader != "" {
					// Wrong leader. It's a bug, should never happen!
					log.Printf("cluster: wrong leader '%s' while expecting '%s'; term %d",
						ping.leader, c.fo.leader, ping.term)
				} else {
					log.Printf("cluster: leader set to '%s'", ping.leader)
				}
				c.fo.leader = ping.leader
			}

			missed = 0
			if ping.signature != c.ring.Signature() {
				if rehashSkipped {
					log.Println("cluster: rehashing at a request of",
						ping.leader, ping.nodes, ping.signature, c.ring.Signature())
					c.rehash(ping.nodes)
					rehashSkipped = false

					//globals.hub.rehash <- true
				} else {
					rehashSkipped = true
				}
			}

		case vreq := <-c.fo.electionVote:
			if c.fo.term < vreq.req.term {
				// This is a new election. This node has not voted yet. Vote for the requestor and
				// clear the current leader.
				log.Printf("Voting YES for %s, my term %d, vote term %d", vreq.req.node, c.fo.term, vreq.req.term)
				c.fo.term = vreq.req.term
				c.fo.leader = ""
				vreq.resp <- _ClusterVoteResponse{result: true, term: c.fo.term}
			} else {
				// This node has voted already or stale election, reject.
				log.Printf("Voting NO for %s, my term %d, vote term %d", vreq.req.node, c.fo.term, vreq.req.term)
				vreq.resp <- _ClusterVoteResponse{result: false, term: c.fo.term}
			}
		case <-c.fo.done:
			return
		}
	}
}
