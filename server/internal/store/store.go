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

package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	adapter "github.com/unit-io/unitdb/server/internal/db"
	"github.com/unit-io/unitdb/server/internal/message"
	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/pkg/hash"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/utp"
)

// The store's own records are kept under "$sys" topics (namespaces.go):
// subscriptions and replicas in their contract's namespace, and hints, the
// topic index and the ids of replicated messages under contract 0. Messages
// stored as a replica of their topic's owner are kept apart from the ones
// stored as the owner, so that a relay can ask for each message from one node
// only.
const (
	// Maximum number of records to return
	maxResults = 1024

	seenTopic = "seen"
	// Longest a replicated message's id is kept: it only guards against the
	// message coming again, from a hint, which is handed off within hours.
	maxSeenTTL = 24 * time.Hour

	topicIndexTopic = "topics"
	// Most messages a topic's history returns, the store's own maximum.
	maxHistory = 100000
)

var adp adapter.Adapter

type configType struct {
	// Configurations for individual adapters.
	Adapters map[string]json.RawMessage `json:"adapters"`
}

func openAdapter(path, jsonconf string, reset bool) error {
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

	return adp.Open(path, adapterConfig, reset)
}

// Open initializes the persistence system. Adapter holds a connection pool for a database instance.
// 	 name - name of the adapter rquested in the config file
//   jsonconf - configuration string
func Open(path, jsonconf string, reset bool) error {
	if err := openAdapter(path, jsonconf, reset); err != nil {
		return err
	}
	wasEmpty = adp.Count() == 0
	loadTopics()
	// Before the node serves: the records a v0.6.0 store kept elsewhere.
	if err := migrate(); err != nil {
		return err
	}

	return nil
}

// wasEmpty is set if the message store held no message when it was opened.
var wasEmpty bool

// WasEmpty reports whether the message store held no message when it was
// opened: a new node, or one whose disk was lost.
func WasEmpty() bool {
	return wasEmpty
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
func InitDb(path, jsonconf string, reset bool) error {
	if !IsOpen() {
		if err := openAdapter(path, jsonconf, reset); err != nil {
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

// Put stores a subscription to topic, a wildcard one or not, under
// messageId.
func (s *SubscriptionStore) Put(contract uint32, messageId []byte, topic string, payload []byte) error {
	return adp.PutWithID(contract, messageId, sysTopic(sysSubscriptions, topic), payload, "")
}

// Get gets the subscriptions that match topic.
func (s *SubscriptionStore) Get(contract uint32, topic string) (matches [][]byte, err error) {
	resp, err := adp.Get(contract, sysTopic(sysSubscriptions, topic), "")
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

func (s *SubscriptionStore) Delete(contract uint32, messageId []byte, topic string) error {
	return adp.Delete(contract, messageId, sysTopic(sysSubscriptions, topic))
}

// A stored message starts with a header holding its expiry, as the store gives
// a message back with neither its id nor its expiry. Messages stored before
// the header have none.
var envelopeMagic = [4]byte{0xE5, 0x7A, 0x1C, 0x03}

const envelopeSize = 12 // magic, and expiry as unix seconds (0: none)

func wrap(payload []byte, expiresAt int64) []byte {
	raw := make([]byte, envelopeSize+len(payload))
	copy(raw, envelopeMagic[:])
	binary.LittleEndian.PutUint64(raw[4:12], uint64(expiresAt))
	copy(raw[envelopeSize:], payload)
	return raw
}

// unwrap returns a stored message's payload, and its expiry if it has a
// header (known).
func unwrap(raw []byte) (payload []byte, expiresAt int64, known bool) {
	if len(raw) < envelopeSize || !bytes.Equal(raw[:4], envelopeMagic[:]) {
		return raw, 0, false
	}
	return raw[envelopeSize:], int64(binary.LittleEndian.Uint64(raw[4:12])), true
}

func expired(expiresAt int64, now time.Time) bool {
	return expiresAt != 0 && expiresAt <= now.Unix()
}

// ExpiresAt returns the unix time a message stored now with ttl expires at,
// or 0 if it does not: ttl is a number of seconds or a duration, as the
// store takes it, and one the store ignores does not expire.
func ExpiresAt(ttl string) int64 {
	if ttl == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(ttl, 10, 64); err == nil {
		return time.Now().Add(time.Duration(secs) * time.Second).Unix()
	}
	if d, err := time.ParseDuration(ttl); err == nil {
		return time.Now().Add(d).Unix()
	}
	return 0
}

// TopicRef is a topic this node stores messages for.
type TopicRef struct {
	Contract uint32
	Topic    string // without options
}

// topics are the topics this node stores messages for, indexed in the store
// as they are first stored, since the store cannot list them.
var topics = struct {
	sync.Mutex
	seen map[TopicRef]bool
}{seen: make(map[TopicRef]bool)}

func loadTopics() {
	raw, err := adp.Get(sysContract, sysTopic(sysIndex, topicIndexTopic), strconv.Itoa(maxHistory))
	if err != nil {
		log.ErrLogger.Err(err).Str("context", "store.loadTopics")
		return
	}
	topics.Lock()
	defer topics.Unlock()
	for _, b := range raw {
		if len(b) > 4 {
			topics.seen[TopicRef{Contract: binary.LittleEndian.Uint32(b[:4]), Topic: string(b[4:])}] = true
		}
	}
}

// indexTopic records that this node stores messages for topic.
func indexTopic(contract uint32, topic string) {
	if err := addToIndex(contract, topic); err != nil {
		log.ErrLogger.Err(err).Str("context", "store.indexTopic").Str("topic", topic)
	}
}

// addToIndex records that this node stores messages for topic, unless it is
// recorded already.
func addToIndex(contract uint32, topic string) error {
	if i := strings.IndexByte(topic, '?'); i >= 0 {
		topic = topic[:i]
	}
	ref := TopicRef{Contract: contract, Topic: topic}
	topics.Lock()
	if topics.seen[ref] {
		topics.Unlock()
		return nil
	}
	topics.seen[ref] = true
	topics.Unlock()
	b := make([]byte, 4+len(topic))
	binary.LittleEndian.PutUint32(b[:4], contract)
	copy(b[4:], topic)
	return adp.Put(sysContract, sysTopic(sysIndex, topicIndexTopic), b, "")
}

// HistoryEntry is a stored message, and its expiry if known.
type HistoryEntry struct {
	Payload   []byte
	ExpiresAt int64 // unix seconds, 0 if it does not expire
	Known     bool  // the expiry is known: the message has a header
}

// MessageStore is a Message struct to hold methods for persistence mapping for the Message object.
type MessageStore struct{}

// Message is the anchor for storing/retrieving Message objects
var Message MessageStore

func (m *MessageStore) Put(contract uint32, topic string, payload []byte, ttl string) error {
	if err := adp.Put(contract, topic, wrap(payload, ExpiresAt(ttl)), ttl); err != nil {
		return err
	}
	indexTopic(contract, topic)
	return nil
}

// PutReplica stores a message as a replica of its topic's owner, until
// expiresAt (unix seconds, 0 for never). A message already expired is not
// stored.
func (m *MessageStore) PutReplica(contract uint32, topic string, payload []byte, expiresAt int64) error {
	now := time.Now()
	if expired(expiresAt, now) {
		return nil
	}
	ttl := ""
	if expiresAt != 0 {
		ttl = strconv.FormatInt(expiresAt-now.Unix(), 10)
	}
	if err := adp.Put(contract, sysTopic(sysReplicas, topic), wrap(payload, expiresAt), ttl); err != nil {
		return err
	}
	indexTopic(contract, topic)
	return nil
}

// Topics returns the topics this node stores messages for, as their owner or
// a replica.
func (m *MessageStore) Topics() []TopicRef {
	topics.Lock()
	defer topics.Unlock()
	refs := make([]TopicRef, 0, len(topics.seen))
	for ref := range topics.seen {
		refs = append(refs, ref)
	}
	return refs
}

// History returns the messages this node stores for topic, as its owner and
// as a replica, up to the store's maximum, with their expiry where known.
func (m *MessageStore) History(contract uint32, topic string) ([]HistoryEntry, error) {
	now := time.Now()
	var entries []HistoryEntry
	for _, at := range storedAt(contract, topic) {
		raw, err := adp.Get(at.contract, at.topic, strconv.Itoa(maxHistory))
		if err != nil {
			return entries, err
		}
		for _, b := range raw {
			payload, expiresAt, known := unwrap(b)
			if known && expired(expiresAt, now) {
				continue
			}
			entries = append(entries, HistoryEntry{Payload: payload, ExpiresAt: expiresAt, Known: known})
		}
	}
	return entries, nil
}

// GetAll gets the messages stored for the topic, as its owner and as a
// replica of its owner.
func (m *MessageStore) GetAll(contract uint32, topic string, last string) ([]*message.Message, error) {
	var all []*message.Message
	for _, at := range storedAt(contract, topic) {
		matches, err := m.get(at.contract, at.topic, topic, last)
		if err != nil {
			return nil, err
		}
		all = append(all, matches...)
	}
	return all, nil
}

// Get gets the messages stored for topic as its owner.
func (m *MessageStore) Get(contract uint32, topic string, last string) (matches []*message.Message, err error) {
	return m.get(contract, topic, topic, last)
}

// get gets the messages stored on contract under storeTopic, as messages on
// topic.
func (m *MessageStore) get(contract uint32, storeTopic, topic string, last string) (matches []*message.Message, err error) {
	resp, err := adp.Get(contract, storeTopic, last)
	now := time.Now()
	for _, raw := range resp {
		payload, expiresAt, known := unwrap(raw)
		if known && expired(expiresAt, now) {
			// The store removes expired messages later.
			continue
		}
		msg := message.Message{
			Topic:   string(topic),
			Payload: payload,
		}
		matches = append(matches, &msg)
	}

	return matches, err
}

// HintStore holds messages kept for a replica that could not take them when
// they were stored, to hand them to it once it can.
type HintStore struct{}

// Hint is the anchor for storing/retrieving hints.
var Hint HintStore

// hintTopic is the topic of the hints for node: a topic part may not hold
// every character a node name can.
func hintTopic(node string) string {
	return sysTopic(sysHints, legacyHintTopic(node))
}

// legacyHintTopic is the topic v0.6.0 kept the hints for node under.
func legacyHintTopic(node string) string {
	return "hints.n" + strconv.FormatUint(uint64(hash.New([]byte(node))), 10)
}

// NewID returns an id to store a hint under.
func (h *HintStore) NewID() ([]byte, error) {
	return adp.NewID()
}

// Put stores a hint for node under id, until ttl if set.
func (h *HintStore) Put(node string, id, payload []byte, ttl string) error {
	return adp.PutWithID(sysContract, id, hintTopic(node), payload, ttl)
}

// Get gets hints for node, up to the store's query limit.
func (h *HintStore) Get(node string) ([][]byte, error) {
	return adp.Get(sysContract, hintTopic(node), "")
}

// Delete deletes the hint for node stored under id.
func (h *HintStore) Delete(node string, id []byte) error {
	return adp.Delete(sysContract, id, hintTopic(node))
}

// SeenStore holds the ids of the replicated messages this node stored as a
// replica, so that it stores each once across its own restarts.
type SeenStore struct{}

// Seen is the anchor for storing/retrieving replicated messages' ids.
var Seen SeenStore

// Put records id, of a message stored until expiresAt (unix seconds, 0 for
// never), for as long as the message, up to maxSeenTTL.
func (s *SeenStore) Put(id string, expiresAt int64) error {
	ttl := maxSeenTTL
	if expiresAt != 0 {
		if left := time.Until(time.Unix(expiresAt, 0)); left < ttl {
			ttl = left
		}
	}
	if ttl <= 0 {
		return nil
	}
	return adp.Put(sysContract, sysTopic(sysSeen, seenTopic), []byte(id), strconv.FormatInt(int64(ttl/time.Second)+1, 10))
}

// Recent returns up to n ids recorded, newest first.
func (s *SeenStore) Recent(n int) ([]string, error) {
	raw, err := adp.Get(sysContract, sysTopic(sysSeen, seenTopic), strconv.Itoa(n))
	ids := make([]string, 0, len(raw))
	for _, b := range raw {
		ids = append(ids, string(b))
	}
	return ids, err
}

// SessionStore is a Session struct to hold methods for persistence mapping for the Session object.
type SessionStore struct{}

// Session is the anchor for storing/retrieving Session objects
var Session SessionStore

func (s *SessionStore) Put(key uint64, payload []byte) error {
	if err := adp.PutMessage(key, payload); err != nil {
		return err
	}
	if len(payload) >= 4 {
		logChanged(LogOp{Block: binary.LittleEndian.Uint32(payload[:4]), Key: key, Raw: payload})
	}
	return nil
}

func (s *SessionStore) Get(key uint64) (raw []byte, err error) {
	return adp.GetMessage(key)
}

// LogOp is a change this node made to a session's log or row.
type LogOp struct {
	Block uint32 // the session id
	Key   uint64
	Raw   []byte // the stored bytes; nil deletes the key
	Reset bool   // deletes every key of the session's log
}

// OnLogChange, if set, is called with each change this node makes to a
// session's log or row, for the cluster to replicate it. It is set before the
// server takes connections.
var OnLogChange func(LogOp)

func logChanged(op LogOp) {
	if OnLogChange != nil {
		OnLogChange(op)
	}
}

// MessageLog is a Message struct to hold methods for persistence mapping for the Message object.
type MessageLog struct{}

// Log is the anchor for storing/retrieving Message objects
var Log MessageLog

// PersistOutbound handles which outgoing messages are stored
func (l *MessageLog) PersistOutbound(blockID uint32, outMsg lp.MessagePack) {
	switch outMsg.(type) {
	case *utp.Publish:
		// Received a publish. store it in ibound
		// until ACKNOWLEDGE or RECEIPT is received.
		key := uint64(outMsg.Info().MessageID)<<32 + uint64(blockID)
		m, err := lp.Encode(outMsg)
		if err != nil {
			log.ErrLogger.Err(err).Str("context", "store.PersistInbound")
			return
		}
		adp.PutMessage(key, m.Bytes())
		logChanged(LogOp{Block: blockID, Key: key, Raw: m.Bytes()})
	}
	if outMsg.Type() == utp.FLOWCONTROL {
		msg := *outMsg.(*utp.ControlMessage)
		switch msg.FlowControl {
		case utp.COMPLETE:
			// Sending ACKNOWLEDGE, delete matching PUBLISH for EXPRESS delivery mode
			// or sending COMPLETE, delete matching RECEIVE for RELIABLE delivery mode from ibound
			key := uint64(outMsg.Info().MessageID)<<32 + uint64(blockID)
			adp.DeleteMessage(key)
			logChanged(LogOp{Block: blockID, Key: key})
		}
	}
}

// PersistInbound handles which incoming messages are stored
func (l *MessageLog) PersistInbound(blockID uint32, inMsg lp.MessagePack) {
	if inMsg.Type() == utp.FLOWCONTROL {
		msg := *inMsg.(*utp.ControlMessage)
		switch msg.FlowControl {
		case utp.RECEIPT:
			// Sending RECEIPT. store in ibound
			// until COMPLETE is sent.
			key := uint64(inMsg.Info().MessageID)<<32 + uint64(blockID)
			m, err := lp.Encode(inMsg)
			if err != nil {
				log.ErrLogger.Err(err).Str("context", "store.PersistOutbound")
				return
			}
			adp.PutMessage(key, m.Bytes())
			logChanged(LogOp{Block: blockID, Key: key, Raw: m.Bytes()})
		}
	}
}

// Get performs a query and attempts to fetch message for the given key
func (l *MessageLog) Get(key uint64) lp.MessagePack {
	if raw, err := adp.GetMessage(key); raw != nil && err == nil {
		r := bytes.NewReader(raw)
		if msg, err := lp.Read(r); err == nil {
			return msg
		}
	}
	return nil
}

// Keys performs a query and attempts to fetch all keys with the prefix.
func (l *MessageLog) Keys(prefix uint32) []uint64 {
	matches := make([]uint64, 0)
	keys := adp.Keys()
	for _, key := range keys {
		if evalPrefix(prefix, key) {
			matches = append(matches, key)
		}
	}
	return matches
}

// Delete is used to delete message.
func (l *MessageLog) Delete(key uint64) {
	adp.DeleteMessage(key)
	logChanged(LogOp{Block: uint32(key), Key: key})
}

// Reset removes all keys with the prefix from store
func (l *MessageLog) Reset(prefix uint32) {
	reset(prefix)
	logChanged(LogOp{Block: prefix, Reset: true})
}

func reset(prefix uint32) {
	keys := adp.Keys()
	for _, key := range keys {
		if evalPrefix(prefix, key) {
			adp.DeleteMessage(key)
		}
	}
}

// Raw returns the stored bytes of a session's log entry or row, or nil.
func (l *MessageLog) Raw(key uint64) []byte {
	raw, err := adp.GetMessage(key)
	if err != nil {
		return nil
	}
	return raw
}

// Apply applies a change another node made to a session's log or row, which
// this node holds a replica of. It is not replicated again.
func (l *MessageLog) Apply(op LogOp) {
	switch {
	case op.Reset:
		reset(op.Block)
	case op.Raw == nil:
		adp.DeleteMessage(op.Key)
	default:
		adp.PutMessage(op.Key, op.Raw)
	}
}

func evalPrefix(prefix uint32, key uint64) bool {
	return uint64(prefix) == key&0xFFFFFFFF
}
