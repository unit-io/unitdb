package message

import (
	"sync"
)

const (
	CONNECT = uint8(iota + 1)
	PUBLISH
	SUBSCRIBE
	UNSUBSCRIBE
	PINGREQ
	PINGRESP
	DISCONNECT

	fixed = 16

	Contract = uint32(3376684800)
)

// ------------------------------------------------------------------------------------

// SubscriberType represents a type of subscriber
type SubscriberType uint8

type TopicAnyCount uint8

// Subscriber types
const (
	SubscriberDirect = SubscriberType(iota)
	SubscriberRemote
)

// Subscriber is a value associated with a subscription.
type Subscriber interface {
	ID() string
	Type() SubscriberType
	SendMessage(*Message) bool
}

// // ------------------------------------------------------------------------------------

// Message represents a message which has to be forwarded or stored.
type Message struct {
	MessageID uint16 `json:"message_id,omitempty"` // The ID of the message
	Topic     []byte `json:"topic,omitempty"`      // The topic of the message
	Payload   []byte `json:"data,omitempty"`       // The payload of the message
	Qos       uint8  `json:"qos,omitempty"`        // The qos of the message
	TTL       int64  `json:"ttl,omitempty"`        // The time-to-live of the message
}

// Size returns the byte size of the message.
func (m *Message) Size() int64 {
	return int64(len(m.Payload))
}

// // ------------------------------------------------------------------------------------

// Stats represents a subscription map.
type Stats struct {
	sync.Mutex
	stats map[string]*Stat
}

type Stat struct {
	ID      []byte
	Topic   []byte
	Counter int
}

// NewStats creates a new container.
func NewStats() *Stats {
	return &Stats{
		stats: make(map[string]*Stat),
	}
}

// Increment adds the subscription to the stats.
func (s *Stats) Increment(topic []byte, key string, id []byte) (first bool) {
	s.Lock()
	defer s.Unlock()

	stat, exists := s.stats[key]
	if !exists {
		stat = &Stat{
			ID:    id,
			Topic: topic,
		}
	}
	stat.Counter++
	s.stats[key] = stat
	return stat.Counter == 1
}

// Decrement remove a subscription from the stats.
func (s *Stats) Decrement(topic []byte, key string) (last bool, id []byte) {
	s.Lock()
	defer s.Unlock()

	if stat, exists := s.stats[key]; exists {
		stat.Counter--
		// Remove if there's no subscribers left
		if stat.Counter <= 0 {
			delete(s.stats, key)
			return true, stat.ID
		}
	}

	return false, nil
}

// Get gets subscription from the stats.
func (s *Stats) Exist(key string) (ok bool) {
	s.Lock()
	defer s.Unlock()

	if _, exists := s.stats[key]; exists {
		return true
	}
	return false
}

// All gets the all subscriptions from the stats.
func (s *Stats) All() []Stat {
	s.Lock()
	defer s.Unlock()

	stats := make([]Stat, 0, len(s.stats))
	for _, stat := range s.stats {
		stats = append(stats, *stat)
	}

	return stats
}
