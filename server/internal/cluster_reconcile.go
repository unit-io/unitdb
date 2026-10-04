package internal

import (
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
)

// Reconciliation after a restore (docs/backup-restore.md): a
// node started with -restored, at a checkpoint, settles each topic it holds
// with the topic's other holders on the union of their messages, before it
// takes clients. A message one took in the seconds between two nodes'
// checkpoints of a run, or since this node's checkpoint while the others ran
// on, survives if either has it.
//
// Stored messages carry no identity, so they are compared by content: each
// side lists the topic's messages as digests,
// SHA-256(contract, topic, expiry, payload), with how many times each
// occurs (two identical publishes stay two), and each side gets what it
// holds fewer of. Expired messages are skipped. What is sent is a message
// and how many the sender holds of it; the receiver, under the topic's
// lock, stores only as many as it lacks of that count. So two nodes both
// restored, reconciling the same topic with each other at once, settle on
// the larger count, not the sum. Nothing is deleted: clients can't delete
// messages, and the security state merges by itself, as every node holds it
// all.
//
// A topic's holders are those of the full ring, as for a rebuild; a holder
// that isn't live is skipped: when it starts, with -restored, it reconciles
// with this node. A peer is asked for the topics it holds that this node
// should too, for a topic this node's checkpoint predates.

// capReconcile: the RPCs of a reconciliation (ReconcileTopics, Digests,
// Reconcile).
const capReconcile = "reconcile"

// reconcileBatch is the most distinct messages sent in one Reconcile.
const reconcileBatch = 1000

// restoreRequested is set by -restored (CheckStart): the cluster reconciles
// when it starts.
var restoreRequested bool

// restoredPath is the db_path a -restored node started at: its
// checkpoint.json becomes restored-from.json once reconciled.
var restoredPath string

// reconcileStats counts what reconciliations did on this node.
var reconcileStats struct {
	topics   atomic.Int64
	sent     atomic.Int64
	received atomic.Int64
	failed   atomic.Int64
	millis   atomic.Int64
}

// MessageDigest identifies a stored message by its content.
type MessageDigest [sha256.Size]byte

// DigestCount is how many of a topic's messages have one digest.
type DigestCount struct {
	Digest MessageDigest
	Count  int
}

// ReconcileTopicsReq asks a node for the topics it holds that the node
// asking should hold too.
type ReconcileTopicsReq struct {
	Node string
}

// ReconcileTopicsResp lists them.
type ReconcileTopicsResp struct {
	Topics []store.TopicRef
}

// DigestsReq asks a node for a topic's messages, as digests.
type DigestsReq struct {
	Node  string
	Topic store.TopicRef
}

// DigestsResp lists them, with their counts.
type DigestsResp struct {
	Digests []DigestCount
}

// CountedEntry is a message and how many the sender holds of it.
type CountedEntry struct {
	Entry store.HistoryEntry
	Count int
}

// ReconcileReq sends a node the messages of a topic it holds fewer of
// (Put), and asks for those it holds more of (Want).
type ReconcileReq struct {
	Node  string
	Topic store.TopicRef
	Put   []CountedEntry
	Want  []MessageDigest
}

// ReconcileResp holds the messages asked for.
type ReconcileResp struct {
	Entries []CountedEntry
}

// topicLocks serializes, per topic, a reconciliation's counting and storing
// on this node.
var topicLocks sync.Map // store.TopicRef -> *sync.Mutex

func topicLock(t store.TopicRef) *sync.Mutex {
	mu, _ := topicLocks.LoadOrStore(t, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

// messageDigest returns the digest of a message of topic t.
func messageDigest(t store.TopicRef, e store.HistoryEntry) MessageDigest {
	h := sha256.New()
	var b [8]byte
	binary.LittleEndian.PutUint32(b[:4], t.Contract)
	h.Write(b[:4])
	binary.LittleEndian.PutUint32(b[:4], uint32(len(t.Topic)))
	h.Write(b[:4])
	h.Write([]byte(t.Topic))
	binary.LittleEndian.PutUint64(b[:], uint64(e.ExpiresAt))
	h.Write(b[:])
	if e.Known {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	h.Write(e.Payload)
	var d MessageDigest
	h.Sum(d[:0])
	return d
}

// byDigest groups a topic's messages by digest.
func byDigest(t store.TopicRef, entries []store.HistoryEntry) map[MessageDigest][]store.HistoryEntry {
	m := make(map[MessageDigest][]store.HistoryEntry, len(entries))
	for _, e := range entries {
		d := messageDigest(t, e)
		m[d] = append(m[d], e)
	}
	return m
}

// nodeHoldsTopic reports whether node is one of t's holders on the full ring.
func (c *Cluster) nodeHoldsTopic(node string, t store.TopicRef) bool {
	for _, h := range c.getFullRing().GetN(topicRingKey(t.Contract, t.Topic), c.replicas) {
		if h == node {
			return true
		}
	}
	return false
}

// ReconcileTopics returns the topics this node holds that req.Node holds
// too, and drops the message hints kept for it, as RebuildTopics does.
// Called by a node reconciling.
func (c *Cluster) ReconcileTopics(req *ReconcileTopicsReq, resp *ReconcileTopicsResp) error {
	if err := refuse(capReconcile); err != nil {
		return err
	}
	// The hints of messages kept for the node are as stale as its
	// checkpoint: it gets the messages from the reconciliation, and a hint
	// handed to it after would store one again.
	if n := c.nodes[req.Node]; n != nil {
		n.handoffMu.Lock()
		c.dropHints(req.Node)
		n.handoffMu.Unlock()
	}
	for _, t := range store.Message.Topics() {
		if c.nodeHoldsTopic(req.Node, t) {
			resp.Topics = append(resp.Topics, t)
		}
	}
	return nil
}

// Digests returns a topic's messages on this node, as digests with their
// counts. Called by a node reconciling.
func (c *Cluster) Digests(req *DigestsReq, resp *DigestsResp) error {
	if err := refuse(capReconcile); err != nil {
		return err
	}
	entries, err := store.Message.History(req.Topic.Contract, req.Topic.Topic)
	if err != nil {
		return err
	}
	for d, es := range byDigest(req.Topic, entries) {
		resp.Digests = append(resp.Digests, DigestCount{Digest: d, Count: len(es)})
	}
	return nil
}

// Reconcile stores the messages a reconciling node sends, and returns those
// it asks for. Called by a node reconciling.
func (c *Cluster) Reconcile(req *ReconcileReq, resp *ReconcileResp) error {
	if err := refuse(capReconcile); err != nil {
		return err
	}
	stored, err := c.storeReconciled(req.Topic, req.Put)
	reconcileStats.received.Add(int64(stored))
	if err != nil {
		return err
	}
	if len(req.Want) == 0 {
		return nil
	}
	entries, err := store.Message.History(req.Topic.Contract, req.Topic.Topic)
	if err != nil {
		return err
	}
	have := byDigest(req.Topic, entries)
	for _, d := range req.Want {
		if es := have[d]; len(es) > 0 {
			resp.Entries = append(resp.Entries, CountedEntry{Entry: es[0], Count: len(es)})
		}
	}
	return nil
}

// storeReconciled stores, of each message of topic t a peer holds Count of,
// as many as this node lacks, as a rebuild does: a message stored without
// its expiry is kept for rebuildTTL. It counts under the topic's lock, so
// what another reconciliation stored meanwhile counts.
func (c *Cluster) storeReconciled(t store.TopicRef, entries []CountedEntry) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	mu := topicLock(t)
	mu.Lock()
	defer mu.Unlock()
	local, err := store.Message.History(t.Contract, t.Topic)
	if err != nil {
		return 0, err
	}
	have := byDigest(t, local)
	stored := 0
	for _, ce := range entries {
		e := ce.Entry
		missing := ce.Count - len(have[messageDigest(t, e)])
		expiresAt := e.ExpiresAt
		if !e.Known {
			expiresAt = time.Now().Add(c.rebuildTTL).Unix()
		}
		for i := 0; i < missing; i++ {
			if err := store.Message.PutReplica(t.Contract, t.Topic, e.Payload, expiresAt); err != nil {
				return stored, err
			}
			stored++
		}
	}
	return stored, nil
}

// reconcileAll reconciles every topic this node holds, or a live peer
// holds that it should too, with the topic's live holders, then lets the
// node take clients. Run once, when a -restored node's cluster starts.
func (c *Cluster) reconcileAll() {
	defer c.reconciling.Store(false)
	start := time.Now()
	c.waitInRing(rebuildTimeout)

	live := make(map[string]bool)
	for _, n := range c.getRingNodes() {
		live[n] = true
	}
	topics := make(map[store.TopicRef]bool)
	for _, t := range store.Message.Topics() {
		topics[t] = true
	}
	for name, n := range c.nodes {
		if !live[name] || !n.supports(capReconcile) {
			continue
		}
		var resp ReconcileTopicsResp
		if err := n.callTimeout("Cluster.ReconcileTopics", &ReconcileTopicsReq{Node: c.thisNodeName}, &resp, rebuildTimeout); err != nil {
			log.ErrLogger.Error().Err(err).Str("context", "cluster.reconcileAll").Msg("no topics from " + name)
			reconcileStats.failed.Add(1)
			continue
		}
		for _, t := range resp.Topics {
			topics[t] = true
		}
	}

	failed := 0
	var peers []string
	for name := range live {
		if name != c.thisNodeName {
			peers = append(peers, name)
		}
	}
	sort.Strings(peers)
	for t := range topics {
		for _, h := range c.getFullRing().GetN(topicRingKey(t.Contract, t.Topic), c.replicas) {
			n := c.nodes[h]
			if n == nil || !live[h] || !n.supports(capReconcile) {
				continue // this node, or one that reconciles when it starts
			}
			if err := c.reconcileTopic(n, t); err != nil {
				failed++
				reconcileStats.failed.Add(1)
				log.ErrLogger.Error().Err(err).Str("context", "cluster.reconcileAll").Str("topic", t.Topic).Msg("topic not reconciled with " + h)
			}
		}
		reconcileStats.topics.Add(1)
	}
	took := time.Since(start)
	reconcileStats.millis.Store(took.Milliseconds())
	log.ErrLogger.Info().Str("context", "cluster.reconcileAll").Strs("live", peers).Int("topics", len(topics)).Int("failed", failed).
		Int64("sent", reconcileStats.sent.Load()).Int64("received", reconcileStats.received.Load()).Dur("took", took).Msg("reconciled after a restore")
	if failed == 0 {
		markRestored(restoredPath)
	}
}

// reconcileTopic settles topic t with node n on the union of their
// messages.
func (c *Cluster) reconcileTopic(n *ClusterNode, t store.TopicRef) error {
	var theirs DigestsResp
	if err := n.callTimeout("Cluster.Digests", &DigestsReq{Node: c.thisNodeName, Topic: t}, &theirs, rebuildTimeout); err != nil {
		return err
	}
	entries, err := store.Message.History(t.Contract, t.Topic)
	if err != nil {
		return err
	}
	mine := byDigest(t, entries)
	theirCount := make(map[MessageDigest]int, len(theirs.Digests))
	var want []MessageDigest
	for _, dc := range theirs.Digests {
		theirCount[dc.Digest] = dc.Count
		if dc.Count > len(mine[dc.Digest]) {
			want = append(want, dc.Digest)
		}
	}
	var put []CountedEntry
	for d, es := range mine {
		if len(es) > theirCount[d] {
			put = append(put, CountedEntry{Entry: es[0], Count: len(es)})
		}
	}
	for first := true; first || len(put) > 0; first = false {
		batch := put
		if len(batch) > reconcileBatch {
			batch = batch[:reconcileBatch]
		}
		put = put[len(batch):]
		req := &ReconcileReq{Node: c.thisNodeName, Topic: t, Put: batch}
		if first {
			req.Want = want
		}
		if len(req.Put) == 0 && len(req.Want) == 0 {
			return nil
		}
		var resp ReconcileResp
		if err := n.callTimeout("Cluster.Reconcile", req, &resp, rebuildTimeout); err != nil {
			return err
		}
		for _, ce := range batch {
			reconcileStats.sent.Add(int64(ce.Count - theirCount[messageDigest(t, ce.Entry)]))
		}
		stored, err := c.storeReconciled(t, resp.Entries)
		reconcileStats.received.Add(int64(stored))
		if err != nil {
			return err
		}
	}
	return nil
}

// waitInRing waits until this node is in the ring and has heard from a
// leader, for up to d: the live nodes it reconciles with are those of the
// ring.
func (c *Cluster) waitInRing(d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if containsNode(c.getRingNodes(), c.thisNodeName) && (c.fo == nil || c.health.lastLeader.Load() != 0) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.ErrLogger.Warn().Str("context", "cluster.waitInRing").Msg("not in the ring yet: reconciling with the nodes it has")
}

// markRestored renames the checkpoint.json of a node's db_path once it is
// reconciled: its next start is an ordinary one, not a restore.
func markRestored(dbPath string) {
	if dbPath == "" {
		return
	}
	from := filepath.Join(dbPath, store.CheckpointInfoFile)
	if err := os.Rename(from, filepath.Join(dbPath, restoredFromFile)); err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "cluster.markRestored").Msg("unable to mark the store restored")
	}
}

// restoredFromFile is what a restored node's checkpoint.json becomes.
const restoredFromFile = "restored-from.json"

// writeReconcileMetrics writes what reconciliations did on this node.
func (c *Cluster) writeReconcileMetrics(m *metricsWriter) {
	b := 0.0
	if c.reconciling.Load() {
		b = 1
	}
	m.one("unitdb_cluster_reconciling", "gauge", "Whether this node, restored, is settling its topics with the others.", b)
	m.one("unitdb_reconcile_topics_total", "counter", "Topics this node reconciled after a restore.", float64(reconcileStats.topics.Load()))
	m.one("unitdb_reconcile_messages_sent_total", "counter", "Messages this node offered a peer that held fewer, in its reconciliations.", float64(reconcileStats.sent.Load()))
	m.one("unitdb_reconcile_messages_received_total", "counter", "Messages this node stored from a peer in reconciliations.", float64(reconcileStats.received.Load()))
	m.one("unitdb_reconcile_failures_total", "counter", "Topics or peers a reconciliation failed on.", float64(reconcileStats.failed.Load()))
	m.one("unitdb_reconcile_duration_seconds", "gauge", "How long the last reconciliation took.", float64(reconcileStats.millis.Load())/1000)
}
