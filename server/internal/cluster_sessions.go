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

// Rebuilding a node that starts with an empty store, and fetching a
// session's copies from the other nodes when a client resumes it: an
// independent implementation of docs/design/cluster-spec.md, sections 1.13
// and 1.14.

import (
	"encoding/binary"
	"sort"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
)

// RebuildReq asks a node for the topics the sender, rebuilding, holds.
type RebuildReq struct {
	Node string
}

// RebuildTopicsResp lists them.
type RebuildTopicsResp struct {
	Topics []store.TopicRef
}

// RebuildHistoryReq asks a node for a topic's messages.
type RebuildHistoryReq struct {
	Node     string
	Contract uint32
	Topic    string
}

// RebuildHistoryResp holds them.
type RebuildHistoryResp struct {
	Entries []store.HistoryEntry
}

// rebuild copies, from every peer that can, the messages of the topics
// this node holds; meanwhile it is not ready and answers no relays.
func (c *Cluster) rebuild() {
	defer c.rebuilding.Store(false)
	names := make([]string, 0, len(c.nodes))
	for name := range c.nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		n := c.nodes[name]
		if c.stopped.Load() {
			return
		}
		if !n.supports(capReplicate) {
			continue
		}
		var resp RebuildTopicsResp
		var err error
		for attempt := 0; attempt < rebuildAttempts; attempt++ {
			resp = RebuildTopicsResp{}
			if err = n.callTimeout("Cluster.RebuildTopics", &RebuildReq{Node: c.thisNodeName}, &resp, rebuildTimeout); err == nil || n.lacks(err, capReplicate) {
				break
			}
			time.Sleep(rebuildRetry)
		}
		if err != nil {
			log.ErrLogger.Error().Err(err).Str("context", "cluster.rebuild").Msg("no topics from " + name)
			continue
		}
		messages := 0
		for _, t := range resp.Topics {
			var h RebuildHistoryResp
			if err := n.callTimeout("Cluster.RebuildHistory", &RebuildHistoryReq{Node: c.thisNodeName, Contract: t.Contract, Topic: t.Topic}, &h, rebuildTimeout); err != nil {
				log.ErrLogger.Error().Err(err).Str("context", "cluster.rebuild").Str("topic", t.Topic).Msg("no messages from " + name)
				continue
			}
			for _, e := range h.Entries {
				expiresAt := e.ExpiresAt
				if !e.Known {
					expiresAt = time.Now().Add(c.rebuildTTL).Unix()
				}
				if err := store.Message.PutReplica(t.Contract, t.Topic, e.Payload, expiresAt); err != nil {
					log.ErrLogger.Error().Err(err).Str("context", "cluster.rebuild").Str("topic", t.Topic).Msg("unable to store a message")
					continue
				}
				messages++
			}
		}
		log.ErrLogger.Info().Str("context", "cluster.rebuild").Int("topics", len(resp.Topics)).Int("messages", messages).Msg("rebuilt from " + name)
	}
}

// RebuildTopics returns the topics the sender holds that this node is to
// send it: those of which it is the first other live holder. The message
// hints kept for the sender are dropped: it copies the messages now.
func (c *Cluster) RebuildTopics(req *RebuildReq, resp *RebuildTopicsResp) error {
	if err := refuse(capReplicate); err != nil {
		return err
	}
	c.dropMessageHints(req.Node)
	live := c.getRingNodes()
	full := c.getFullRing()
	for _, t := range store.Message.Topics() {
		holders := full.GetN(topicRingKey(t.Contract, t.Topic), c.replicas)
		if !containsNode(holders, req.Node) {
			continue
		}
		for _, h := range holders {
			if h == req.Node || !containsNode(live, h) {
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

// RebuildHistory returns a topic's messages.
func (c *Cluster) RebuildHistory(req *RebuildHistoryReq, resp *RebuildHistoryResp) error {
	if err := refuse(capReplicate); err != nil {
		return err
	}
	entries, err := store.Message.History(req.Contract, req.Topic)
	resp.Entries = entries
	return err
}

// FetchSessionReq asks a node for its copy of a session.
type FetchSessionReq struct {
	Node string
	Key  uint64 // the session row's key
}

// FetchSessionResp is a node's copy of a session: its row and log.
type FetchSessionResp struct {
	Found  bool
	Row    []byte
	SessID uint32
	Log    []store.LogOp
}

// ForgetSessionReq tells a node to drop its copy of a session.
type ForgetSessionReq struct {
	Node   string
	Key    uint64
	SessID uint32
}

// fetchSession brings a session a client resumes here up to date from the
// copies the other nodes hold: its replicas, and nodes it was resumed on
// before. Those that are not replicas then drop theirs.
func (c *Cluster) fetchSession(sessKey uint64) {
	if c == nil || c.replicas < 2 || !hasCapability(capSessions) {
		return
	}
	type answer struct {
		n    *ClusterNode
		resp *FetchSessionResp
		err  error
	}
	answers := make(chan answer, len(c.nodes))
	asked := 0
	for _, n := range c.nodes {
		if !n.supports(capSessions) {
			continue
		}
		asked++
		n, resp := n, &FetchSessionResp{}
		n.goCall("Cluster.FetchSession", &FetchSessionReq{Node: c.thisNodeName, Key: sessKey}, resp, fetchSessionWait, func(err error) {
			answers <- answer{n, resp, err}
		})
	}
	var got []answer
	deadline := time.After(fetchSessionWait)
collect:
	for i := 0; i < asked; i++ {
		select {
		case a := <-answers:
			if a.err != nil {
				a.n.lacks(a.err, capSessions)
				continue
			}
			if a.resp.Found {
				got = append(got, a)
			}
		case <-deadline:
			break collect
		}
	}
	if len(got) == 0 {
		return
	}
	local, _ := store.Session.Get(sessKey)
	haveLocal := len(local) >= 4
	row := local
	if !haveLocal {
		row = got[0].resp.Row
	}
	sessID := binary.LittleEndian.Uint32(row[:4])
	for _, a := range got {
		if a.resp.SessID != sessID {
			continue
		}
		for _, op := range a.resp.Log {
			store.Log.Apply(op)
		}
	}
	if !haveLocal {
		store.Log.Apply(store.LogOp{Block: sessID, Key: sessKey, Raw: row})
	}
	for _, a := range got {
		if !c.isSessionReplica(a.n.name, sessID) {
			n := a.n
			var unused bool
			n.goCall("Cluster.ForgetSession", &ForgetSessionReq{Node: c.thisNodeName, Key: sessKey, SessID: a.resp.SessID}, &unused, rebuildTimeout, func(err error) {
				if err != nil {
					log.ErrLogger.Debug().Err(err).Str("peer", n.name).Msg("cluster: stale session copy not dropped")
				}
			})
		}
	}
}

// isSessionReplica reports whether node holds session sessID, by the
// current ring or the full one.
func (c *Cluster) isSessionReplica(node string, sessID uint32) bool {
	key := sessionRingKey(sessID)
	return containsNode(c.getRing().GetN(key, c.replicas), node) || containsNode(c.getFullRing().GetN(key, c.replicas), node)
}

// FetchSession returns this node's copy of a session, if it has one.
func (c *Cluster) FetchSession(req *FetchSessionReq, resp *FetchSessionResp) error {
	if err := refuse(capSessions); err != nil {
		return err
	}
	row, err := store.Session.Get(req.Key)
	if err != nil || len(row) < 4 {
		return nil
	}
	resp.Found = true
	resp.Row = row
	resp.SessID = binary.LittleEndian.Uint32(row[:4])
	for _, k := range store.Log.Keys(resp.SessID) {
		if raw := store.Log.Raw(k); raw != nil {
			resp.Log = append(resp.Log, store.LogOp{Block: resp.SessID, Key: k, Raw: raw})
		}
	}
	return nil
}

// ForgetSession drops this node's copy of a session, unless it is one of
// the session's replicas.
func (c *Cluster) ForgetSession(req *ForgetSessionReq, unused *bool) error {
	if err := refuse(capSessions); err != nil {
		return err
	}
	if c.isSessionReplica(c.thisNodeName, req.SessID) {
		return nil
	}
	store.Log.Apply(store.LogOp{Block: req.SessID, Reset: true})
	store.Log.Apply(store.LogOp{Block: req.SessID, Key: req.Key})
	return nil
}
