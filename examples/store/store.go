package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"

	adapter "github.com/unit-io/unitdb/examples/db"
	net "github.com/unit-io/unitdb/plugins/pubsub"
)

const (
	// Maximum number of records to return
	maxResults         = 1024
	connStoreId uint32 = 4105991048 // hash("connectionstore")
)

var adp adapter.Adapter

type configType struct {
	// Configurations for individual adapters.
	Adapters map[string]json.RawMessage `json:"adapters"`
}

func openAdapter(jsonconf string, reset bool) error {
	var config configType
	if err := json.Unmarshal([]byte(jsonconf), &config); err != nil {
		return errors.New("store: failed to parse config: " + err.Error() + "(" + jsonconf + ")")
	}

	if adp == nil {
		return errors.New("store: database adapter is missing")
	}

	if adp.IsOpen() {
		return errors.New("store: connection is already opened")
	}

	var adapterConfig string
	if config.Adapters != nil {
		adapterConfig = string(config.Adapters[adp.GetName()])
	}

	return adp.Open(adapterConfig, reset)
}

// Open initializes the persistence system. Adapter holds a connection pool for a database instance.
// 	 name - name of the adapter rquested in the config file
//   jsonconf - configuration string
func Open(jsonconf string, reset bool) error {
	if err := openAdapter(jsonconf, reset); err != nil {
		return err
	}

	return nil
}

// Close terminates connection to persistent storage.
func Close() error {
	if adp.IsOpen() {
		return adp.Close()
	}

	return nil
}

// IsOpen checks if persistent storage connection has been initialized.
func IsOpen() bool {
	if adp != nil {
		return adp.IsOpen()
	}

	return false
}

// GetAdapterName returns the name of the current adater.
func GetAdapterName() string {
	if adp != nil {
		return adp.GetName()
	}

	return ""
}

// InitDb open the db connection. If jsconf is nil it will assume that the connection is already open.
// If it's non-nil, it will use the config string to open the DB connection first.
func InitDb(jsonconf string, reset bool) error {
	if !IsOpen() {
		if err := openAdapter(jsonconf, reset); err != nil {
			return err
		}
	}
	panic("store: Init DB error")
}

// RegisterAdapter makes a persistence adapter available.
// If Register is called twice or if the adapter is nil, it panics.
func RegisterAdapter(name string, a adapter.Adapter) {
	if a == nil {
		panic("store: Register adapter is nil")
	}

	if adp != nil {
		panic("store: adapter '" + adp.GetName() + "' is already registered")
	}

	adp = a
}

// SubscriptionStore is a Subscription struct to hold methods for persistence mapping for the subscription.
// Note, do not use same contract as messagestore
type SubscriptionStore struct{}

// Message is the ancor for storing/retrieving Message objects
var Subscription SubscriptionStore

func (s *SubscriptionStore) Put(contract uint32, messageId, topic, payload []byte) error {
	return adp.PutWithID(contract^connStoreId, messageId, topic, payload)
}

func (s *SubscriptionStore) Get(contract uint32, topic []byte) (matches [][]byte, err error) {
	resp, err := adp.Get(contract^connStoreId, topic)
	for _, payload := range resp {
		if payload == nil {
			continue
		}
		matches = append(matches, payload)
	}

	return matches, err
}

func (s *SubscriptionStore) NewID() ([]byte, error) {
	return adp.NewID()
}

func (s *SubscriptionStore) Delete(contract uint32, messageId, topic []byte) error {
	return adp.Delete(contract^connStoreId, messageId, topic)
}

// Entry represents a message entry which has to be forwarded or stored.
type Entry struct {
	MessageID uint16 `json:"message_id,omitempty"` // The ID of the message
	Topic     []byte `json:"topic,omitempty"`      // The topic of the message
	Payload   []byte `json:"data,omitempty"`       // The payload of the message
	Qos       uint8  `json:"qos,omitempty"`        // The qos of the message
	TTL       int64  `json:"ttl,omitempty"`        // The time-to-live of the message
}

// Size returns the byte size of the message.
func (m *Entry) Size() int64 {
	return int64(len(m.Payload))
}

// MessageStore is a Message struct to hold methods for persistence mapping for the Message object.
type MessageStore struct{}

// Message is the anchor for storing/retrieving Message objects
var Message MessageStore

func (m *MessageStore) Put(contract uint32, topic, payload []byte) error {
	return adp.Put(contract, topic, payload)
}

func (m *MessageStore) Get(contract uint32, topic []byte) (matches []Entry, err error) {
	resp, err := adp.Get(contract, topic)
	for _, payload := range resp {
		e := Entry{
			Topic:   topic,
			Payload: payload,
			Qos:     0, // TODO implement logic to set and get Qos from store.
		}
		matches = append(matches, e)
	}

	return matches, err
}

// MessageLog is a Message struct to hold methods for persistence mapping for the Message object.
type MessageLog struct{}

// Log is the anchor for storing/retrieving Message objects
var Log MessageLog

// PersistOutbound handles which outgoing messages are stored
func (l *MessageLog) PersistOutbound(proto net.ProtoAdapter, key uint64, msg net.Packet) {
	switch msg.(type) {
	case *net.Puback:
		// Sending puback. delete matching publish
		// from ibound
		adp.DeleteMessage(key)
	case *net.Publish, *net.Subscribe, *net.Unsubscribe:
		// Sending publish. store in obound
		// until puback received
		m, err := net.Encode(proto, msg)
		if err != nil {
			log.Printf("context: store.PersistOutbound %s", err)
			return
		}
		adp.PutMessage(key, m.Bytes())
	}
}

// PersistInbound handles which incoming messages are stored
func (l *MessageLog) PersistInbound(proto net.ProtoAdapter, key uint64, msg net.Packet) {
	switch msg.(type) {
	case *net.Puback, *net.Suback, *net.Unsuback:
		// Received a puback. delete matching publish
		// from obound
		adp.DeleteMessage(key)
	case *net.Connack:
	case *net.Publish:
		// Received a publish. store it in ibound
		// until puback sent
		m, err := net.Encode(proto, msg)
		if err != nil {
			log.Printf("context: store.PersistOutbound %s", err)
			return
		}
		adp.PutMessage(key, m.Bytes())
	}
}

// Get performs a query and attempts to fetch message for the given blockId and key
func (l *MessageLog) Get(proto net.ProtoAdapter, key uint64) net.Packet {
	if raw, err := adp.GetMessage(key); raw != nil && err == nil {
		r := bytes.NewReader(raw)
		if msg, err := net.ReadPacket(proto, r); err == nil {
			return msg
		}

	}
	return nil
}

// Keys performs a query and attempts to fetch all keys for given blockId and key prefix.
func (l *MessageLog) Keys() []uint64 {
	return adp.Keys()
}

// Delete is used to delete message.
func (l *MessageLog) Delete(key uint64) {
	adp.DeleteMessage(key)
}

// Reset removes all keys from store
func (l *MessageLog) Reset() {
	keys := adp.Keys()
	for _, key := range keys {
		adp.DeleteMessage(key)
	}
}
