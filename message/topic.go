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

package message

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"time"
	"unsafe"

	"github.com/unit-io/unitdb/hash"
)

var zeroTime = time.Unix(0, 0)

// Various constant on Topic
const (
	TopicInvalid = uint8(iota)
	TopicStatic
	TopicWildcard
	TopicAnySeparator = '*'
	TopicAllSeparator = "..."
	TopicSeparator    = '.' // The separator character.
	TopicMaxDepth     = 100 // Maximum depth for topic using a separator

	// Wildcard wildcard is hash for wildcard topic such as '*' or '...'
	Wildcard = uint32(857445537)
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
	Hash      uint32
	Wildchars uint8
}

// AddContract adds contract to the parts of a topic.
func (t *Topic) AddContract(contract uint32) {
	part := Part{
		Wildchars: 0,
		Hash:      contract,
	}
	parts := []Part{part}
	t.Parts = append(parts, t.Parts...)
}

// GetHash combines the parts into a single hash.
func (t *Topic) GetHash(contract uint64) uint64 {
	if len(t.Parts) == 1 {
		return contract
	}
	h := t.Parts[0].Hash
	for _, i := range t.Parts[1:] {
		h ^= i.Hash
	}
	return uint64(h)<<32 + (contract << 8) | uint64(t.Depth)
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
	return t.Parts[0].Hash
}

// TTL returns a Time-To-Live option.
func (t *Topic) TTL() (uint32, bool) {
	ttl, sec, ok := t.getOption("ttl")
	if sec > 0 {
		return uint32(time.Duration(sec) * time.Second), ok
	}
	var duration time.Duration
	duration, _ = time.ParseDuration(ttl)
	return uint32(time.Now().Add(duration).Unix()), ok
}

// Last returns the 'last' option, which is a number of messages to retrieve.
func (t *Topic) Last() (time.Time, int, bool) {
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

// toUnix converts the time to Unix Time with validation.
func toUnix(t int64) time.Time {
	if t == 0 {
		return zeroTime
	}

	return time.Unix(t, 0)
}

// getOption retrieves a Uint option
func (t *Topic) getOption(name string) (string, int, bool) {
	for i := 0; i < len(t.Options); i++ {
		if t.Options[i].Key == name {
			val, err := strconv.ParseInt(t.Options[i].Value, 10, 64)
			if err == nil {
				return "", int(val), true
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
	h := t.Parts[0].Hash
	for _, i := range t.Parts[1:] {
		h ^= i.Hash
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
		part.Hash = hash.WithSalt(p, contract)
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
	q := []byte(TopicAllSeparator)
	if bytes.HasSuffix(topic.Topic, q) {
		depth++
		topic.Topic = bytes.TrimRight(topic.Topic, string(TopicAllSeparator))
		topic.TopicType = TopicWildcard
		topic.Depth = TopicMaxDepth
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
				part.Hash = hash.WithSalt(p, contract)
				topic.Parts = append(topic.Parts, part)
			}
			wildchars++
			wildcharcount++
			continue
		}
		part.Hash = hash.WithSalt(p, contract)
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
	if topic.Depth == TopicMaxDepth {
		part.Hash = Wildcard
		topic.Parts = append(topic.Parts, part)
	}
	topic.Depth = depth

	if topic.TopicType != TopicWildcard {
		topic.TopicType = TopicStatic
	}
	return true
}

// Marshal serializes topic to binary
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
		binary.LittleEndian.PutUint32(buf[n:], part.Hash)
		n += 4
	}
	return buf
}

// Unmarshal de-serializes topic from binary data
func (t *Topic) Unmarshal(data []byte) error {
	buf := bytes.NewBuffer(data)

	var parts []Part
	depth := uint8(buf.Next(1)[0])
	for i := 0; i <= int(depth); i++ {
		if buf.Len() == 0 {
			break
		}
		wildchars := uint8(buf.Next(1)[0])
		hash := binary.LittleEndian.Uint32(buf.Next(4))
		parts = append(parts, Part{
			Hash:      hash,
			Wildchars: wildchars,
		})
	}
	t.Depth = depth
	t.Parts = parts
	return nil
}

// unsafeToString is used to convert a slice
// of bytes to a string without incurring overhead.
func unsafeToString(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}
