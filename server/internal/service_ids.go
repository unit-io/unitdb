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
	"encoding/json"
	"time"

	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/pkg/hash"
	"github.com/unit-io/unitdb/server/internal/types"
)

// Trusted services, such as an API server, publish and subscribe on their
// contract's topics without topic keys. What makes a client one is sealed in
// its client id, uid.AllowService, which only server/cmd/mintid issues: not
// the CONNECT insecure flag, which any client can set.
//
// A service's own connections, with its id, skip key checks. A connection a
// service opens for a user, with the user's client id, skips them once the
// service vouches for it: its unitdb/service request carries the service's
// id, of the same contract, which never leaves the service. Topics the
// server keeps for itself (security.IsReserved) stay out of reach either way.

// requestService vouches for the connection (see onService).
var requestService = hash.WithSalt([]byte("service"), message.Contract)

// ServiceRequest is a unitdb/service request.
type ServiceRequest struct {
	// ClientID is a trusted service's client id, of the connection's
	// contract.
	ClientID string `json:"client_id"`
}

// onService takes a service's client id as vouching for the connection,
// whose requests then skip topic key checks. It answers 200, or 403 for an
// id that is not a service's of the connection's contract.
func (c *_Conn) onService(payload []byte) (interface{}, bool) {
	var req ServiceRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return types.ErrBadRequest, false
	}
	id, claims, err := c.service.keys.OpenClientID([]byte(req.ClientID))
	if err != nil || (claims != nil && claims.Expired(time.Now().Unix())) || !id.IsService() || id.Contract() != c.clientID.Contract() {
		return types.ErrForbidden, false
	}
	c.insecure.Store(true)
	c.serviceTrusted.Store(true)
	return &types.ServiceResponse{Status: 200}, true
}
