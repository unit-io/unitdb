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

package internal

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
)

// moveLegacyHints moves the hints a v0.6.0 node kept, under a fixed id, for
// the nodes of the cluster to where they are kept now (store/namespaces.go),
// before the cluster starts. A hint holds the id it is stored under, so each
// is stored again under a new id, the copies flushed, and then the old one
// deleted. A crash in between leaves both: the next start finds the copy and
// only deletes the old one.
func moveLegacyHints() error {
	c := Globals.Cluster
	if c == nil {
		return nil
	}
	names := make([]string, 0, len(c.nodes)+1)
	names = append(names, c.thisNodeName)
	for name := range c.nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		n, err := moveLegacyHintsOf(name)
		if n > 0 {
			log.ErrLogger.Info().Str("context", "cluster.moveLegacyHints").Int("hints", n).Msg("moved the hints an older version kept for " + name + " to a $sys topic")
		}
		if err != nil {
			return fmt.Errorf("cluster: moving the hints an older version kept for %s: %w", name, err)
		}
	}
	return nil
}

// The old hints, as the store reads and deletes them; tests replace them.
var (
	legacyHints      = store.Hint.Legacy
	deleteLegacyHint = store.Hint.DeleteLegacy
)

// hintContent is what a hint is stored with, but its id: a hint copied
// before a crash is found by it.
func hintContent(h replicaHint) string {
	h.ID = nil
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(h); err != nil {
		return ""
	}
	return buf.String()
}

// moveLegacyHintsOf moves the hints kept for node, and returns how many it
// stored again.
func moveLegacyHintsOf(node string) (int, error) {
	moved := 0
	deleted, unreadable := make(map[string]bool), make(map[string]bool)
	for {
		ids, raw, err := legacyHints(node)
		if err != nil {
			return moved, err
		}
		if len(ids) == 0 {
			return moved, nil
		}
		// The copies there already, of a move a crash interrupted.
		have, err := store.Hint.Get(node)
		if err != nil {
			return moved, err
		}
		there := make(map[string]int, len(have))
		for _, b := range have {
			var h replicaHint
			if gob.NewDecoder(bytes.NewReader(b)).Decode(&h) == nil {
				there[hintContent(h)]++
			}
		}
		now := time.Now().Unix()
		var gone [][]byte
		for i := len(raw) - 1; i >= 0; i-- {
			if unreadable[string(ids[i])] {
				continue
			}
			if deleted[string(ids[i])] {
				return moved, errors.New("an old hint is still there after it was deleted")
			}
			var h replicaHint
			if err := gob.NewDecoder(bytes.NewReader(raw[i])).Decode(&h); err != nil {
				// As v0.6.0 does: kept, never handed off.
				log.ErrLogger.Error().Err(err).Str("context", "cluster.moveLegacyHints").Msg("unreadable hint for " + node + ": left where it is")
				unreadable[string(ids[i])] = true
				continue
			}
			gone = append(gone, ids[i])
			ttl := sessionHintTTL
			if h.Op == nil {
				ttl = h.Entry.Ttl
				if h.Entry.ExpiresAt != 0 {
					if h.Entry.ExpiresAt <= now {
						continue // expired: deleted below
					}
					ttl = strconv.FormatInt(h.Entry.ExpiresAt-now, 10)
				}
			}
			if k := hintContent(h); there[k] > 0 {
				there[k]--
				continue
			}
			if err := storeHint(node, h, ttl); err != nil {
				return moved, err
			}
			moved++
		}
		if err := store.Flush(); err != nil {
			return moved, err
		}
		if len(gone) == 0 {
			// Only unreadable hints are left.
			return moved, nil
		}
		for _, id := range gone {
			if err := deleteLegacyHint(node, id); err != nil {
				return moved, err
			}
			deleted[string(id)] = true
		}
	}
}
