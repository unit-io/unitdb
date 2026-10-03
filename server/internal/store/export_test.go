package store

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

// ReplicaContractForTest is the contract a replica of contract's messages
// is stored under.
func ReplicaContractForTest(contract uint32) uint32 { return contract ^ replicaStoreId }
