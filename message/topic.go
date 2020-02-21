package message

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"time"
	"unsafe"

	"github.com/unit-io/tracedb/hash"
)

var zeroTime = time.Unix(0, 0)

// Varous constant on Topic
const (
	TopicInvalid = uint8(iota)
	TopicStatic
	TopicWildcard
	TopicAnySeparator         = '*'
	TopicChildrenAllSeparator = "..."
	TopicSeparator            = '.'   // The separator character.
	MaxMessageSize            = 65536 // Maximum message size allowed from/to the peer.
	TopicMaxDepth             = 100   // Maximum depth for topic using a separator
)

// TopicOption represents a key/value pair option.
type TopicOption struct {
	Key   string
	Value string
}

// Topic represents a parsed topic.
type Topic struct {
	Topic        []byte // Gets or sets the topic string.
	TopicOptions []byte
	Parts        []Part
	Depth        uint8
	Options      []TopicOption // Gets or sets the options.
	TopicType    uint8
}

// Part represents a parsed topic parts broken based on topic separator.
type Part struct {
	Query     uint32
	Wildchars uint8
}

// nextPart is a helper function that reads the next part from a buffer and returns the data and a bool to indicate
// success.
func nextPart(buf *bytes.Buffer) (Part, bool) {
	if buf.Len() < 5 {
		// missing length
		return Part{}, false
	}
	part := Part{}
	part.Wildchars = uint8(buf.Next(1)[0])
	part.Query = binary.LittleEndian.Uint32(buf.Next(4))
	return part, true
}

// AddContract adds contract to the parts of a topic.
func (t *Topic) AddContract(contract uint32) {
	part := Part{
		Wildchars: 0,
		Query:     contract,
	}
	if t.Parts[0].Query == Wildcard {
		t.Parts[0].Query = contract
	} else {
		parts := []Part{part}
		t.Parts = append(parts, t.Parts...)
	}
}

// GetHash combines the parts into a single hash.
func (t *Topic) GetHash(contract uint64) uint64 {
	if len(t.Parts) == 1 {
		return contract
	}
	h := t.Parts[0].Query
	for _, i := range t.Parts[1:] {
		h ^= i.Query
	}
	return uint64(h)<<32 + contract
}

// Marshal seriliazes topic to binay
func (t *Topic) Marshal() []byte {
	// preallocate buffer of appropriate size
	var size int
	//Depth size
	size++
	for range t.Parts {
		size += 5
	}
	buf := make([]byte, size)

	var n int
	buf[n] = byte(t.Depth)
	n++
	for _, part := range t.Parts {
		buf[n] = byte(part.Wildchars)
		n++
		binary.LittleEndian.PutUint32(buf[n:], part.Query)
		n += 4
	}
	return buf
}

// Unmarshal deseriliazes topic from binary data
func (t *Topic) Unmarshal(data []byte) error {
	buf := bytes.NewBuffer(data)

	var parts []Part
	depth := uint8(buf.Next(1)[0])
	for i := 0; i <= int(depth); i++ {
		if buf.Len() == 0 {
			break
		}
		wildchars := uint8(buf.Next(1)[0])
		query := binary.LittleEndian.Uint32(buf.Next(4))
		parts = append(parts, Part{
			Query:     query,
			Wildchars: wildchars,
		})
	}
	t.Depth = depth
	t.Parts = parts
	return nil
}

// SplitFunc various split function to split topic using delimeter
type splitFunc struct{}

func (splitFunc) splitTopic(c rune) bool {
	return c == TopicSeparator
}

func (splitFunc) options(c rune) bool {
	return c == '?'
}

func (splitFunc) splitOptions(c rune) bool {
	return c == '&'
}
func (splitFunc) splitOpsKeyValue(c rune) bool {
	return c == '='
}

// Target returns the topic (first element of the query, second element of Parts)
func (t *Topic) Target() uint32 {
	return t.Parts[0].Query
}

// TTL returns a Time-To-Live option.
func (t *Topic) TTL() (int64, bool) {
	ttl, sec, ok := t.getOption("ttl")
	if sec > 0 {
		return int64(time.Duration(sec) * time.Second), ok
	}
	var duration time.Duration
	duration, _ = time.ParseDuration(ttl)
	return int64(duration), ok
}

// Last returns the 'last' option, which is a number of messages to retrieve.
func (t *Topic) Last() (time.Time, uint32, bool) {
	dur, last, ok := t.getOption("last")
	if ok {
		if last > 0 {
			return zeroTime, last, ok
		}
		base := time.Now()
		var duration time.Duration
		duration, _ = time.ParseDuration(dur)
		start := base.Add(-duration)
		return start, 0, ok
	}

	return zeroTime, 0, ok
}

// Converts the time to Unix Time with validation.
func toUnix(t int64) time.Time {
	if t == 0 {
		return zeroTime
	}

	return time.Unix(t, 0)
}

// getOptUint retrieves a Uint option
func (t *Topic) getOption(name string) (string, uint32, bool) {
	for i := 0; i < len(t.Options); i++ {
		if t.Options[i].Key == name {
			val, err := strconv.ParseInt(t.Options[i].Value, 10, 64)
			if err == nil {
				return "", uint32(val), true
			}
			return t.Options[i].Value, 0, true
		}
	}
	return "", 0, false
}

// parseOptions parse the options from the topic
func (t *Topic) parseOptions(text []byte) (ok bool) {
	//Parse Options
	var fn splitFunc
	ops := bytes.FieldsFunc(text, fn.splitOptions)
	if ops != nil || len(ops) >= 1 {
		for _, o := range ops {
			op := bytes.FieldsFunc(o, fn.splitOpsKeyValue)
			if op == nil || len(op) < 2 {
				continue
			}
			t.Options = append(t.Options, TopicOption{
				Key:   unsafeToString(op[0]),
				Value: unsafeToString(op[1]),
			})
		}
	}
	return true
}

// GetHashCode combines the topic parts into a single hash.
func (t *Topic) GetHashCode() uint32 {
	h := t.Parts[0].Query
	for _, i := range t.Parts[1:] {
		h ^= i.Query
	}
	return h
}

// ParseKey attempts to parse the key
func (t *Topic) ParseKey(text []byte) {
	var fn splitFunc

	parts := bytes.FieldsFunc(text, fn.options)
	l := len(parts)
	if parts == nil || l < 1 {
		t.TopicType = TopicInvalid
		return
	}
	if l > 1 {
		t.TopicOptions = parts[1]
	}
	t.Topic = parts[0]
}

// Parse attempts to parse the static vs wildcard topic
func (t *Topic) Parse(contract uint32, wildcard bool) {
	if wildcard {
		parseWildcardTopic(contract, t)
		return
	}
	parseStaticTopic(contract, t)

	return
}

// parseStaticTopic attempts to parse the topic from the underlying slice.
func parseStaticTopic(contract uint32, topic *Topic) (ok bool) {
	// start := time.Now()
	// defer logger.Debug().Str("context", "topic.parseStaticTopic").Dur("duration", time.Since(start)).Msg("")

	var part Part
	var fn splitFunc
	topic.Parts = make([]Part, 0, 6)
	ok = topic.parseOptions(topic.TopicOptions)

	if !ok {
		topic.TopicType = TopicInvalid
		return false
	}

	parts := bytes.FieldsFunc(topic.Topic, fn.splitTopic)
	part = Part{}
	for _, p := range parts {
		part.Query = hash.WithSalt(p, contract)
		topic.Parts = append(topic.Parts, part)
	}

	topic.Depth = uint8(len(topic.Parts))
	topic.TopicType = TopicStatic
	return true
}

// parseWildcardTopic attempts to parse the topic from the underlying slice.
func parseWildcardTopic(contract uint32, topic *Topic) (ok bool) {
	// start := time.Now()
	// defer logger.Debug().Str("context", "topic.parseWildcardTopic").Dur("duration", time.Since(start)).Msg("")

	var part Part
	var fn splitFunc
	topic.Parts = make([]Part, 0, 6)
	ok = topic.parseOptions(topic.TopicOptions)

	if !ok {
		topic.TopicType = TopicInvalid
		return false
	}

	depth := uint8(0)
	q := []byte(TopicChildrenAllSeparator)
	if bytes.HasSuffix(topic.Topic, q) {
		topic.Topic = bytes.TrimRight(topic.Topic, string(TopicChildrenAllSeparator))
		topic.TopicType = TopicWildcard
		topic.Depth = TopicMaxDepth

		if len(topic.Topic) == 0 {
			part.Query = Wildcard
			topic.Parts = append(topic.Parts, part)
			return false
		}
	}

	parts := bytes.FieldsFunc(topic.Topic, fn.splitTopic)
	q = []byte{TopicAnySeparator}
	part = Part{}
	wildchars := uint8(0)
	wildcharcount := 0
	for idx, p := range parts {
		depth++
		if bytes.HasSuffix(p, q) {
			topic.TopicType = TopicWildcard
			if idx == 0 {
				part.Query = hash.WithSalt(p, contract)
				topic.Parts = append(topic.Parts, part)
			}
			wildchars++
			wildcharcount++
			continue
		}
		part.Query = hash.WithSalt(p, contract)
		topic.Parts = append(topic.Parts, part)
		if wildchars > 0 {
			if idx-wildcharcount-1 >= 0 {
				topic.Parts[idx-wildcharcount-1].Wildchars = wildchars
			} else {
				topic.Parts[0].Wildchars = wildchars
			}
			wildchars = 0
		}
	}

	if wildchars > 0 {
		topic.Parts[len(topic.Parts)-1:][0].Wildchars = wildchars
	}
	topic.Depth += depth

	if topic.TopicType != TopicWildcard {
		topic.TopicType = TopicStatic
	}
	return true
}

// unsafeToString is used to convert a slice
// of bytes to a string without incurring overhead.
func unsafeToString(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}
