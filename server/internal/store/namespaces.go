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
	"strings"

	"github.com/unit-io/unitdb/server/internal/pkg/hash"
)

// Since v0.7.0 the store keeps its own records under topics that start with
// "$sys", which no client request can address (security.IsReserved), rather
// than under the contract XOR a fixed id, or under a fixed id: two contracts
// could share such a namespace, and a contract could be drawn equal to a
// fixed id (docs/security-review.md, finding 8). A contract's own records
// (its subscriptions, and the replicas of its messages) stay in its
// namespace, under "$sys.<kind>.<topic>"; the node's own records (hints, the
// topic index, the ids of replicated messages, the security state) are under
// contract 0, sysContract, which is never a client's (uid.NewContract).
//
// A store written by v0.6.0 or before is moved to this layout when it is
// opened (see migrate.go).
const (
	sysContract uint32 = 0

	sysSubscriptions = "sub"
	sysReplicas      = "replica"
	sysHints         = "hint"
	sysIndex         = "index"
	sysSeen          = "seen"
	sysSecurity      = "security"
)

// sysTopic returns the topic the store keeps a record of kind for topic
// under.
func sysTopic(kind, topic string) string {
	if strings.HasPrefix(topic, "...") {
		return "$sys." + kind + topic
	}
	return "$sys." + kind + "." + topic
}

// place is where records are stored: a contract's namespace and a topic.
type place struct {
	contract uint32
	topic    string
}

// storedAt returns where the messages of topic are stored: as its owner,
// and as a replica.
func storedAt(contract uint32, topic string) []place {
	return []place{
		{contract, topic},
		{contract, sysTopic(sysReplicas, topic)},
	}
}

// The namespaces v0.6.0 and before kept the store's own records in: a
// contract's subscriptions and replicas under the contract XOR an id, and the
// node's own records under a fixed id. They are read only to move what they
// hold (migrate.go); uid.NewContract never draws one of the fixed ids.
const (
	legacyConnStoreId    uint32 = 4105991048 // hash("connectionstore")
	legacyReplicaStoreId uint32 = 2654435761
	legacyHintStoreId    uint32 = 2246822519
	legacyIndexStoreId   uint32 = 3266489917
	legacySeenStoreId    uint32 = 2860486313

	legacySeenTopic     = "seen"
	legacySecurityTopic = "security"
)

// legacySecurityStoreId is where v0.6.0 kept the security state.
var legacySecurityStoreId = hash.New([]byte("securitystore"))

// LegacyStoreIDs returns the fixed ids v0.6.0 and before kept the store's own
// records under, which no contract may be.
func LegacyStoreIDs() []uint32 {
	return []uint32{legacyConnStoreId, legacyReplicaStoreId, legacyHintStoreId, legacyIndexStoreId, legacySeenStoreId, legacySecurityStoreId}
}
