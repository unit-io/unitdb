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
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
)

// The server issues v2 client ids and topic keys, which expire and name the
// key of the keyring that issued them (package keys), and still reads v1
// ones. In a cluster it issues v2 ones only once every other node is known
// to read them (capV2Keys): a node checks the topic key of a request another
// node forwards to it, and a client may connect to any node. Until then it
// issues v1 ones, which carry no uuid and never expire.
//
// A node run without capV2Keys (UNITDB_CLUSTER_CAPS, for tests) issues v1
// ones too, as a node of an earlier version does; it still reads v2 ones.

// issuesV2 reports whether the server issues v2 client ids and topic keys.
func issuesV2() bool {
	return hasCapability(capV2Keys) && Globals.Cluster.allKnownToSupport(capV2Keys)
}

// issueClientID seals id as a client id, v2 with the lifetime of its kind
// (primary or not) if the cluster reads v2 ones, else v1.
func (s *_Service) issueClientID(id uid.ID) (string, error) {
	if !issuesV2() {
		return s.keys.EncodeClientIDV1(id), nil
	}
	ttl := s.clientIDTTL
	if id.IsPrimary() {
		ttl = s.primaryIDTTL
	}
	return s.keys.SealClientID(id, ttl)
}

// renewsAt reports whether a client id with claims is renewed when its
// client connects at now, in unix seconds: a v1 id (no claims) always, a v2
// one sealed with another key than issueKey (one being retired) always, and
// one that expires once past 80% of its lifetime.
func renewsAt(claims *uid.Claims, issueKey uint8, now int64) bool {
	if claims == nil || claims.KeyID != issueKey {
		return true
	}
	if claims.ExpiresAt == 0 || claims.ExpiresAt <= claims.IssuedAt {
		return false
	}
	life := int64(claims.ExpiresAt) - int64(claims.IssuedAt)
	return now >= int64(claims.IssuedAt)+life*4/5
}

// renewClientID sends the client a new v2 client id on unitdb/clientid/,
// when it connected with a v1 id, a v2 one of another key than the issue
// key, or one past 80% of its lifetime, and the cluster reads v2 ids. The
// new id is the same id: its contract, permissions and uuid, and so its
// sessions, stay. It is sealed with the issue key, so a client that takes it
// also moves off a key being retired.
func (c *_Conn) renewClientID() {
	if !issuesV2() || !renewsAt(c.idClaims, c.service.keys.IssueKeyID(), time.Now().Unix()) {
		return
	}
	text, err := c.service.issueClientID(c.clientID)
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "conn.renewClientID").Msg("unable to seal a client id")
		return
	}
	c.sendClientID(text)
}
