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

// The server issues and reads v2 client ids and topic keys, which expire and
// name the key of the keyring that issued them (package keys). Since v0.7.0
// it neither issues nor reads v1 ones, nor unsigned keys: a cluster is
// upgraded to v0.7.0 from v0.6.0, whose nodes issue v2 ones once every node
// says it reads them (capV2Keys), as every v0.6.0 and v0.7.0 node does. See
// docs/rolling-deploys.md.

// issueClientID seals id as a v2 client id, with the lifetime of its kind
// (primary or not).
func (s *_Service) issueClientID(id uid.ID) (string, error) {
	ttl := s.clientIDTTL
	if id.IsPrimary() {
		ttl = s.primaryIDTTL
	}
	return s.keys.SealClientID(id, ttl)
}

// renewsAt reports whether a client id with claims is renewed when its
// client connects at now, in unix seconds: one sealed with another key than
// issueKey (one being retired) always, and one that expires once past 80% of
// its lifetime.
func renewsAt(claims uid.Claims, issueKey uint8, now int64) bool {
	if claims.KeyID != issueKey {
		return true
	}
	if claims.ExpiresAt == 0 || claims.ExpiresAt <= claims.IssuedAt {
		return false
	}
	life := int64(claims.ExpiresAt) - int64(claims.IssuedAt)
	return now >= int64(claims.IssuedAt)+life*4/5
}

// renewClientID sends the client a new client id on unitdb/clientid/, when
// it connected with one of another key than the issue key, or one past 80%
// of its lifetime. The new id is the same id: its contract, permissions and
// uuid, and so its sessions, stay. It is sealed with the issue key, so a
// client that takes it also moves off a key being retired.
func (c *_Conn) renewClientID() {
	if !renewsAt(c.idClaims, c.service.keys.IssueKeyID(), time.Now().Unix()) {
		return
	}
	text, err := c.service.issueClientID(c.clientID)
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "conn.renewClientID").Msg("unable to seal a client id")
		return
	}
	c.sendClientID(text)
}
