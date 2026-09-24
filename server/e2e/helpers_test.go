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

package e2e

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync/atomic"
	"time"
)

// A distinct session key per connection. The server's default session key has
// 1-second resolution, so connections made in the same second would otherwise
// share a session; an explicit unique key keeps them separate.
var sessCounter int32 = 1 << 20

func nextSess() int32 {
	return atomic.AddInt32(&sessCounter, 1)
}

// encodePayload frames a message as [seq:4][crc:4][body], so a reader can detect
// loss (missing seq), duplication (repeated seq) and corruption (bad crc) end
// to end, independent of the server's own on-disk checksums.
func encodePayload(seq int, body string) []byte {
	buf := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(buf[:4], uint32(seq))
	copy(buf[8:], body)
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(buf[8:]))
	return buf
}

// decodePayload reverses encodePayload and verifies the crc. ok is false if the
// payload is too short or the crc does not match.
func decodePayload(b []byte) (seq int, body []byte, ok bool) {
	if len(b) < 8 {
		return 0, nil, false
	}
	seq = int(binary.BigEndian.Uint32(b[:4]))
	want := binary.BigEndian.Uint32(b[4:8])
	body = b[8:]
	return seq, body, crc32.ChecksumIEEE(body) == want
}

// collectUnique reads delivered publishes into a seq->body map until it has want
// distinct sequences or a deadline passes. It returns the map, the number of
// duplicate deliveries seen, and an error describing loss/corruption/stall. It
// bounds total time so a redelivery storm (a delivery bug) fails fast instead of
// looping forever on repeated sequences.
func collectUnique(c *client, want int, overall, quiet time.Duration) (map[int]string, int, error) {
	got := make(map[int]string)
	dups := 0
	deadline := time.Now().Add(overall)
	for len(got) < want {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return got, dups, fmt.Errorf("stalled: got %d of %d (%d duplicates) before deadline", len(got), want, dups)
		}
		if remaining > quiet {
			remaining = quiet
		}
		msg, ok := c.waitPub(remaining)
		if !ok {
			return got, dups, fmt.Errorf("no message for %s: got %d of %d (%d duplicates)", quiet, len(got), want, dups)
		}
		for _, m := range msg.Messages {
			seq, body, ok := decodePayload(m.Payload)
			if !ok {
				return got, dups, fmt.Errorf("corrupt payload on %q", m.Topic)
			}
			if _, seen := got[seq]; seen {
				dups++
				continue
			}
			got[seq] = string(body)
		}
	}
	return got, dups, nil
}
