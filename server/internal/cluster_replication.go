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

// Replication of stored messages and session logs to their replicas, and
// hinted handoff of what a replica could not take: an independent
// implementation of docs/design/cluster-spec.md, sections 1.10 to 1.12.
//
// Each peer has one queue, for messages and session changes alike, so that
// it gets one session's changes in order; its sender sends whatever is
// queued in one call. What a peer can't take now is kept as a hint, in the
// store, and handed to it later.

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"sync"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
)

// ReplicaEntry is a stored message sent to a replica. Its fields are those
// of the hints stored before (spec 4.2): keep their names and types.
type ReplicaEntry struct {
	// ID is unique in the cluster, across restarts; replicas store each
	// message once by it. Empty for messages that need no such care.
	ID       string
	Contract uint32
	// Topic is the topic as stored, with options.
	Topic   string
	Payload []byte
	Ttl     string
	// ExpiresAt is unix seconds, 0 for never.
	ExpiresAt int64
}

// ReplicateReq carries messages and session changes to a replica. Handoff
// marks hints handed off.
type ReplicateReq struct {
	Node    string
	Entries []ReplicaEntry
	Log     []store.LogOp
	Handoff bool
}

// replicaItem is a message or a session change queued for a peer; done, if
// set, gets the outcome of its call.
type replicaItem struct {
	entry *ReplicaEntry
	op    *store.LogOp
	done  chan error
}

// hintRecord is a hint as stored: field names and types are the stored
// format (spec 4.2). Op is set for a session hint, with Raw nil.
type hintRecord struct {
	ID    []byte
	Entry ReplicaEntry
	Op    *store.LogOp
}

// pendingHint is a hint the store refused, kept in memory.
type pendingHint struct {
	node string
	rec  hintRecord
	ttl  string
}

// putHint stores a hint: a variable so that a test can make it fail.
var putHint = store.Hint.Put

// waitsForReplica reports whether a publish waits for a replica to store
// it before it is acknowledged.
func (c *Cluster) waitsForReplica(reliable bool) bool {
	if c == nil {
		return reliable
	}
	return reliable || !c.asyncReplication
}

// newReplicaID returns a replication id unique in the cluster and across
// this node's restarts, and unlike any of the format before.
func (c *Cluster) newReplicaID() string {
	return "r3:" + c.thisNodeName + ":" + strconv.FormatInt(c.incarnation, 36) + ":" + strconv.FormatUint(c.replSeq.Add(1), 36)
}

// replicate sends a message this node stored as its topic's owner to the
// topic's replicas. name is the topic without options, topic as stored.
// With wait, it returns once a replica stored it, or none did in time.
func (c *Cluster) replicate(contract uint32, name, topic string, payload []byte, ttl string, wait bool) {
	if c == nil || c.replicas < 2 || isWildcardTopic(name) || !hasCapability(capReplicate) {
		return
	}
	e := ReplicaEntry{ID: c.newReplicaID(), Contract: contract, Topic: topic, Payload: payload, Ttl: ttl, ExpiresAt: store.ExpiresAt(ttl)}
	key := topicRingKey(contract, name)
	live := c.getRingNodes()
	stored := !wait
	for _, h := range c.getRing().GetN(key, c.replicas) {
		n := c.nodes[h]
		if n == nil {
			continue // this node
		}
		if !n.supports(capReplicate) {
			c.hint(h, e)
			continue
		}
		if stored {
			if !n.enqueue(replicaItem{entry: &e}) {
				c.hint(h, e)
			}
			continue
		}
		done := make(chan error, 1)
		if !n.enqueue(replicaItem{entry: &e, done: done}) {
			c.hint(h, e)
			continue
		}
		select {
		case err := <-done:
			// A failed batch hinted it.
			stored = err == nil
		case <-time.After(replicaAckTimeout):
			c.hint(h, e)
		}
	}
	if !stored {
		log.ErrLogger.Warn().Str("context", "cluster.replicate").Uint32("contract", contract).Str("topic", name).Msg("no replica took the message in time: it is on this node only until a hint is handed off")
	}
	for _, h := range c.getFullRing().GetN(key, c.replicas) {
		if h != c.thisNodeName && !containsNode(live, h) {
			c.hint(h, e)
		}
	}
}

// onLogChange sends a change this node made to a session's log or row to
// the session's replicas. A change that stores something waits for one,
// unless replication is asynchronous.
func (c *Cluster) onLogChange(op store.LogOp) {
	if c.replicas < 2 || !hasCapability(capSessions) {
		return
	}
	if op.Raw != nil {
		op.Raw = append([]byte(nil), op.Raw...)
	}
	key := sessionRingKey(op.Block)
	live := c.getRingNodes()
	wait := op.Raw != nil && !op.Reset && !c.asyncReplication
	holders := c.getRing().GetN(key, c.replicas)
	done := make(chan error, len(holders))
	queued := 0
	for _, h := range holders {
		n := c.nodes[h]
		if n == nil {
			continue
		}
		if !n.supports(capSessions) {
			c.sessionHint(h, op)
			continue
		}
		it := replicaItem{op: &op}
		if wait {
			it.done = done
		}
		if !n.enqueue(it) {
			c.sessionHint(h, op)
			continue
		}
		queued++
	}
	for _, h := range c.getFullRing().GetN(key, c.replicas) {
		if h != c.thisNodeName && !containsNode(live, h) {
			c.sessionHint(h, op)
		}
	}
	if !wait || queued == 0 {
		return
	}
	timeout := time.After(replicaAckTimeout)
	for i := 0; i < queued; i++ {
		select {
		case err := <-done:
			if err == nil {
				return
			}
		case <-timeout:
			log.ErrLogger.Warn().Str("context", "cluster.onLogChange").Uint32("session", op.Block).Msg("no replica took the session change in time; it stays queued")
			return
		}
	}
}

// enqueue queues it for the peer, unless its queue is full.
func (n *ClusterNode) enqueue(it replicaItem) bool {
	select {
	case n.repl <- it:
		return true
	default:
		return false
	}
}

// replicateLoop sends the peer what is queued for it, a batch at a time,
// until replDone closes. With UNITDB_REPLICATION_DELAY (a test's), a batch
// nobody waits for is held that long, gathering more, unless an item
// someone waits for comes.
func (n *ClusterNode) replicateLoop(self string) {
	for {
		var first replicaItem
		select {
		case first = <-n.repl:
		case <-n.replDone:
			return
		}
		if c := n.owner; c != nil {
			c.replInFlight.Add(1)
		}
		batch := []replicaItem{first}
		waited := first.done != nil
	gather:
		for len(batch) < replicationBatchSize {
			select {
			case it := <-n.repl:
				batch = append(batch, it)
				waited = waited || it.done != nil
			default:
				break gather
			}
		}
		if d := replicationDelay; d > 0 && !waited {
			timer := time.NewTimer(d)
		hold:
			for len(batch) < replicationBatchSize {
				select {
				case it := <-n.repl:
					batch = append(batch, it)
					if it.done != nil {
						break hold
					}
				case <-timer.C:
					break hold
				case <-n.replDone:
					break hold
				}
			}
			timer.Stop()
		}
		n.sendBatch(self, batch)
		if c := n.owner; c != nil {
			c.replInFlight.Add(-1)
		}
	}
}

// sendBatch sends a batch to the peer. If it fails, everything in it is
// kept as hints: the peer may have stored part of it, and stores each
// message once.
func (n *ClusterNode) sendBatch(self string, batch []replicaItem) {
	req := &ReplicateReq{Node: self}
	for _, it := range batch {
		if it.entry != nil {
			req.Entries = append(req.Entries, *it.entry)
		}
		if it.op != nil {
			req.Log = append(req.Log, *it.op)
		}
	}
	var unused bool
	err := n.callTimeout("Cluster.Replicate", req, &unused, replicateCallTimeout)
	if err != nil {
		if !n.lacks(err, capReplicate) {
			n.lacks(err, capSessions)
		}
		log.ErrLogger.Debug().Err(err).Str("peer", n.name).Int("items", len(batch)).Msg("cluster: replication batch failed: kept as hints")
		if c := n.owner; c != nil {
			for _, it := range batch {
				if it.entry != nil {
					c.hint(n.name, *it.entry)
				}
				if it.op != nil {
					c.sessionHint(n.name, *it.op)
				}
			}
		}
	}
	for _, it := range batch {
		if it.done != nil {
			it.done <- err
		}
	}
}

// Replicate stores the messages and applies the session changes a peer
// sends. A message whose id was stored already is skipped. Hints handed to
// a node catching up have their messages dropped: it copies them anyway.
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
	if !(req.Handoff && c.catchingUp()) {
		for _, e := range req.Entries {
			if e.ID != "" && !c.seen.add(e.ID) {
				continue
			}
			if err := store.Message.PutReplica(e.Contract, e.Topic, e.Payload, e.ExpiresAt); err != nil {
				if e.ID != "" {
					c.seen.remove(e.ID)
				}
				return err
			}
			if e.ID != "" {
				if err := store.Seen.Put(e.ID, e.ExpiresAt); err != nil {
					log.ErrLogger.Warn().Err(err).Str("context", "cluster.Replicate").Msg("unable to record a replicated message's id")
				}
			}
		}
	}
	for _, op := range req.Log {
		store.Log.Apply(op)
	}
	return nil
}

// seenIDs are the replication ids this node stored last, oldest dropped.
type seenIDs struct {
	mu    sync.Mutex
	slot  map[string]int
	ring  []string
	next  int
	limit int
}

// add records id, and reports false if it was recorded already.
func (s *seenIDs) add(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.slot == nil {
		s.slot = make(map[string]int)
		if s.limit == 0 {
			s.limit = seenIDsKept
		}
	}
	if _, ok := s.slot[id]; ok {
		return false
	}
	if len(s.ring) < s.limit {
		s.ring = append(s.ring, id)
		s.slot[id] = len(s.ring) - 1
		return true
	}
	old := s.ring[s.next]
	if i, ok := s.slot[old]; ok && i == s.next {
		delete(s.slot, old)
	}
	s.ring[s.next] = id
	s.slot[id] = s.next
	s.next = (s.next + 1) % s.limit
	return true
}

// remove forgets id, so that it is stored when it comes again.
func (s *seenIDs) remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.slot, id)
}

// loadSeen loads the ids this node stored lately, oldest first.
func (c *Cluster) loadSeen() {
	ids, err := store.Seen.Recent(seenIDsKept)
	if err != nil {
		log.ErrLogger.Warn().Err(err).Str("context", "cluster.loadSeen").Msg("unable to read the replicated messages' ids")
	}
	for i := len(ids) - 1; i >= 0; i-- {
		c.seen.add(ids[i])
	}
}

// hint keeps message e for node, which could not take it now.
func (c *Cluster) hint(node string, e ReplicaEntry) {
	c.keepHint(node, hintRecord{Entry: e}, e.Ttl)
}

// sessionHint keeps, for node, which key of a session changed; the change
// itself is read when the hint is handed off.
func (c *Cluster) sessionHint(node string, op store.LogOp) {
	op.Raw = nil
	c.keepHint(node, hintRecord{Op: &op}, sessionHintTTL)
}

// keepHint stores a hint, or keeps it in memory if the store refuses it.
func (c *Cluster) keepHint(node string, rec hintRecord, ttl string) {
	ph := pendingHint{node: node, rec: rec, ttl: ttl}
	if err := c.storeHint(&ph); err != nil {
		log.ErrLogger.Warn().Err(err).Str("context", "cluster.hint").Str("node", node).Msg("unable to store a hint: kept in memory")
		c.pendingMu.Lock()
		c.pending = append(c.pending, ph)
		if len(c.pending) > maxPendingHints {
			log.ErrLogger.Error().Str("context", "cluster.hint").Str("node", c.pending[0].node).Msg("too many hints in memory: the oldest is dropped")
			c.pending = c.pending[1:]
		}
		c.pendingMu.Unlock()
	}
}

// storeHint stores a hint under a new id, unless it has one.
func (c *Cluster) storeHint(ph *pendingHint) error {
	if ph.rec.ID == nil {
		id, err := store.Hint.NewID()
		if err != nil {
			return err
		}
		ph.rec.ID = id
	}
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(&ph.rec); err != nil {
		return err
	}
	return putHint(ph.node, ph.rec.ID, b.Bytes(), ph.ttl)
}

// storePendingHints stores the hints kept in memory, keeping those the
// store still refuses.
func (c *Cluster) storePendingHints() {
	c.pendingMu.Lock()
	pending := c.pending
	c.pending = nil
	c.pendingMu.Unlock()
	var kept []pendingHint
	for i := range pending {
		if c.storeHint(&pending[i]) != nil {
			kept = append(kept, pending[i])
		}
	}
	if len(kept) > 0 {
		c.pendingMu.Lock()
		c.pending = append(kept, c.pending...)
		c.pendingMu.Unlock()
	}
}

// readHints reads up to the store's query limit of node's hints, skipping
// unreadable ones.
func readHints(node string) []hintRecord {
	raw, err := store.Hint.Get(node)
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "cluster.readHints").Str("node", node).Msg("unable to read hints")
	}
	recs := make([]hintRecord, 0, len(raw))
	for _, b := range raw {
		var rec hintRecord
		if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&rec); err != nil || rec.ID == nil {
			log.ErrLogger.Error().Err(err).Str("context", "cluster.readHints").Str("node", node).Msg("an unreadable hint: skipped")
			continue
		}
		recs = append(recs, rec)
	}
	return recs
}

// handoff hands node n the hints kept for it that it can take, in batches;
// one handoff at a time per peer.
func (c *Cluster) handoff(n *ClusterNode) {
	if c.stopped.Load() || !n.handoffMu.TryLock() {
		return
	}
	defer n.handoffMu.Unlock()
	c.storePendingHints()
	handed := 0
	defer func() {
		if handed > 0 {
			log.ErrLogger.Info().Str("context", "cluster.handoff").Int("hints", handed).Msg("handed off to " + n.name)
		}
	}()
	for {
		var send []hintRecord
		for _, rec := range readHints(n.name) {
			if rec.Op == nil && n.supports(capReplicate) || rec.Op != nil && n.supports(capSessions) {
				send = append(send, rec)
			}
		}
		if len(send) == 0 {
			return
		}
		for start := 0; start < len(send); start += replicationBatchSize {
			end := start + replicationBatchSize
			if end > len(send) {
				end = len(send)
			}
			batch := send[start:end]
			req := &ReplicateReq{Node: c.thisNodeName, Handoff: true}
			for _, rec := range batch {
				if rec.Op == nil {
					req.Entries = append(req.Entries, rec.Entry)
				} else {
					req.Log = append(req.Log, currentState(rec.Op)...)
				}
			}
			var unused bool
			if err := n.callTimeout("Cluster.Replicate", req, &unused, replicateCallTimeout); err != nil {
				if !n.lacks(err, capReplicate) {
					n.lacks(err, capSessions)
				}
				log.ErrLogger.Debug().Err(err).Str("peer", n.name).Msg("cluster: hints not handed off")
				return
			}
			for _, rec := range batch {
				if err := store.Hint.Delete(n.name, rec.ID); err != nil {
					log.ErrLogger.Error().Err(err).Str("context", "cluster.handoff").Str("node", n.name).Msg("unable to delete a hint handed off")
					return
				}
				handed++
			}
		}
	}
}

// currentState is what a session hint stands for, as the session is now:
// the key's bytes (nil deletes it), or a reset and every key of the session.
func currentState(op *store.LogOp) []store.LogOp {
	if !op.Reset {
		return []store.LogOp{{Block: op.Block, Key: op.Key, Raw: store.Log.Raw(op.Key)}}
	}
	ops := []store.LogOp{{Block: op.Block, Reset: true}}
	for _, k := range store.Log.Keys(op.Block) {
		ops = append(ops, store.LogOp{Block: op.Block, Key: k, Raw: store.Log.Raw(k)})
	}
	return ops
}

// handoffLoop hands their hints to the connected peers every
// handoffInterval, and stores the hints kept in memory.
func (c *Cluster) handoffLoop() {
	t := time.NewTicker(handoffInterval)
	defer t.Stop()
	for {
		select {
		case <-c.quit:
			return
		case <-t.C:
		}
		c.storePendingHints()
		for _, n := range c.nodes {
			if n.currentLink() != nil {
				go c.handoff(n)
			}
		}
	}
}

// dropHints drops the message hints kept for node, in the store and in
// memory, keeping its session hints. The caller holds the node's
// handoffMu, so no handoff to it runs meanwhile.
func (c *Cluster) dropHints(node string) {
	for {
		dropped := 0
		for _, rec := range readHints(node) {
			if rec.Op != nil {
				continue
			}
			if err := store.Hint.Delete(node, rec.ID); err != nil {
				log.ErrLogger.Error().Err(err).Str("context", "cluster.dropHints").Str("node", node).Msg("unable to drop a hint")
				return
			}
			dropped++
		}
		if dropped == 0 {
			break
		}
	}
	c.pendingMu.Lock()
	kept := c.pending[:0]
	for _, ph := range c.pending {
		if ph.node != node || ph.rec.Op != nil {
			kept = append(kept, ph)
		}
	}
	c.pending = kept
	c.pendingMu.Unlock()
}

// dropMessageHints drops node's message hints, excluding a handoff to it.
func (c *Cluster) dropMessageHints(node string) {
	n := c.nodes[node]
	if n == nil {
		return
	}
	n.handoffMu.Lock()
	defer n.handoffMu.Unlock()
	c.dropHints(node)
}
