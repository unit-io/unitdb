package store

import (
	"encoding/binary"
	"strconv"
	"time"
)

// rawAdapter returns the adapter under the sealing one, which stores and
// reads records as they are.
func rawAdapter() interface {
	Get(contract uint32, topic string, last string) ([][]byte, error)
	GetMessage(key uint64) ([]byte, error)
	PutMessage(key uint64, payload []byte) error
	Put(contract uint32, topic string, payload []byte, ttl string) error
} {
	if a, ok := adp.(*sealingAdapter); ok {
		return a.Adapter
	}
	return adp
}

// GetRawForTest returns the records stored under contract and topic as they
// are on disk.
func GetRawForTest(contract uint32, topic string) ([][]byte, error) {
	return rawAdapter().Get(contract, topic, "")
}

// GetMessageRawForTest returns the record stored under key as it is on disk.
func GetMessageRawForTest(key uint64) ([]byte, error) {
	return rawAdapter().GetMessage(key)
}

// PutMessageRawForTest stores b under key as it is.
func PutMessageRawForTest(key uint64, b []byte) error {
	return rawAdapter().PutMessage(key, b)
}

// PutRawForTest stores b under contract and topic as it is.
func PutRawForTest(contract uint32, topic string, b []byte) error {
	return rawAdapter().Put(contract, topic, b, "")
}

// ForgetTopicsForTest empties the topic index held in memory, as a restart
// does, so that the next Open reads it from the store.
func ForgetTopicsForTest() {
	topics.Lock()
	defer topics.Unlock()
	topics.seen = make(map[TopicRef]bool)
}

// SysTopicForTest is the topic the store keeps a record of kind ("sub",
// "replica", "hint", "index", "seen", "security") for topic under.
func SysTopicForTest(kind, topic string) string { return sysTopic(kind, topic) }

// HintTopicForTest is the topic of the hints for node.
func HintTopicForTest(node string) string { return hintTopic(node) }

// SysContract is the contract the node's own records are kept under.
const SysContract = sysContract

// The store as v0.6.0 wrote it: its own records under fixed ids, through the
// sealing adapter, as a v0.6.0 node with encrypt_at_rest set as now would.

// PutLegacyReplicaForTest stores a replica of a message as v0.6.0 did, and
// indexes its topic in the old index.
func PutLegacyReplicaForTest(contract uint32, topic string, payload []byte, expiresAt int64) error {
	ttl := ""
	if expiresAt != 0 {
		ttl = strconv.FormatInt(expiresAt-time.Now().Unix(), 10)
	}
	if err := adp.Put(contract^legacyReplicaStoreId, topic, wrap(payload, expiresAt), ttl); err != nil {
		return err
	}
	return PutLegacyIndexForTest(contract, topic)
}

// PutLegacyIndexForTest adds topic to the old topic index.
func PutLegacyIndexForTest(contract uint32, topic string) error {
	b := make([]byte, 4+len(topic))
	binary.LittleEndian.PutUint32(b[:4], contract)
	copy(b[4:], topic)
	return adp.Put(legacyIndexStoreId, topicIndexTopic, b, "")
}

// PutLegacyHintForTest stores a hint for node as v0.6.0 did.
func PutLegacyHintForTest(node string, id, payload []byte) error {
	return adp.PutWithID(legacyHintStoreId, id, legacyHintTopic(node), payload, "")
}

// PutLegacySeenForTest records a replicated message's id as v0.6.0 did.
func PutLegacySeenForTest(id string) error {
	return adp.Put(legacySeenStoreId, legacySeenTopic, []byte(id), "3600")
}

// PutLegacySecurityForTest stores a record of the security state under id as
// v0.6.0 did.
func PutLegacySecurityForTest(id, payload []byte) error {
	return adp.PutWithID(legacySecurityStoreId, id, legacySecurityTopic, payload, "")
}

// PutLegacySubscriptionForTest stores a subscription as v0.6.0 did.
func PutLegacySubscriptionForTest(contract uint32, id []byte, topic string, payload []byte) error {
	return adp.PutWithID(contract^legacyConnStoreId, id, topic, payload, "")
}

// LegacyLeftForTest returns how many records are left in the old namespaces
// of the store's own records: the index, the replicas of contract's topics,
// the hints for nodes, the seen ids and the security state.
func LegacyLeftForTest(contract uint32, topics, nodes []string) map[string]int {
	left := make(map[string]int)
	count := func(kind string, c uint32, topic string) {
		ids, _, _ := adp.GetWithIDs(c, topic+moveBatch)
		left[kind] += len(ids)
	}
	count("index", legacyIndexStoreId, topicIndexTopic)
	for _, t := range topics {
		count("replica", contract^legacyReplicaStoreId, t)
	}
	for _, n := range nodes {
		count("hint", legacyHintStoreId, legacyHintTopic(n))
	}
	count("seen", legacySeenStoreId, legacySeenTopic)
	count("security", legacySecurityStoreId, legacySecurityTopic)
	return left
}

// GetForTest returns the records stored under contract and topic, opened.
func GetForTest(contract uint32, topic string) ([][]byte, error) {
	return adp.Get(contract, topic+moveBatch, "")
}

// CrashMoveAfterForTest has a move of old records stop, as a crash would,
// once records are copied and before the old ones are deleted, after n
// copies of a batch; n < 0 stops none.
func CrashMoveAfterForTest(n int) {
	if n < 0 {
		crashAfterCopy = nil
		return
	}
	calls := 0
	crashAfterCopy = func() bool {
		calls++
		return calls > n
	}
}

// LegacyReplicaContractForTest is the contract v0.6.0 kept the replicas of
// contract's messages under.
func LegacyReplicaContractForTest(contract uint32) uint32 { return contract ^ legacyReplicaStoreId }

// PutLegacyMessageForTest stores a message contract owns, and indexes its
// topic in the old index, as v0.6.0 did.
func PutLegacyMessageForTest(contract uint32, topic string, payload []byte) error {
	if err := adp.Put(contract, topic, wrap(payload, 0), ""); err != nil {
		return err
	}
	return PutLegacyIndexForTest(contract, topic)
}
