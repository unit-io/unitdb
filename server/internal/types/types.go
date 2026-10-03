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

package types

import (
	"github.com/unit-io/unitdb/server/internal/message/security"
)

// Error represents an event code which provides a more details.
type Error struct {
	ReturnCode uint8
	Status     int    `json:"status"`
	Message    string `json:"message"`
	ID         int    `json:"id,omitempty"`
}

// Error implements error interface.
func (e *Error) Error() string { return e.Message }

// ErrorCode implements error interface.
func (e *Error) ErrrorCode() uint8 { return e.ReturnCode }

// Represents a set of errors used in the handlers.
var (
	ErrInvalidProto      = &Error{ReturnCode: 0x01, Status: 401, Message: "Unacceptable proto version. The proto version is invalid."}
	ErrInvalidClientID   = &Error{ReturnCode: 0x02, Status: 401, Message: "Identifier rejected. The client ID is invalid or missing. Use a valid client Id or use an auto generated client ID in the connection request."}
	ErrClientIdForbidden = &Error{ReturnCode: 0x03, Status: 403, Message: "Unacceptable identifier, access not allowed use primary client Id to request a secondary client ID."}
	ErrUnauthorized      = &Error{ReturnCode: 0x04, Status: 401, Message: "Security key rejected. The security key provided is not authorized to perform this operation."}
	ErrServerError       = &Error{ReturnCode: 0x05, Status: 500, Message: "An unexpected condition was encountered."}
	ErrBadToken          = &Error{ReturnCode: 0x06, Status: 403, Message: "Authentication failed."}
	ErrForbidden         = &Error{ReturnCode: 0x07, Status: 403, Message: "The request is understood, but it has been refused or access is not allowed."}
	ErrSessionExist      = &Error{ReturnCode: 0x08, Status: 403, Message: "Another connection using the same session ID has an active connection causing this connection to be closed."}
	ErrUnknownEpoch      = &Error{ReturnCode: 0x09, Status: 403, Message: "Unknown authentication epoch."}
	ErrTimteout          = &Error{ReturnCode: 0x10, Status: 504, Message: "The network connection timeout."}
	ErrNotFound          = &Error{ReturnCode: 0x11, Status: 404, Message: "The resource requested does not exist."}
	ErrBadRequest        = &Error{ReturnCode: 0x12, Status: 400, Message: "The request was invalid or cannot be otherwise served."}
	ErrTargetTooLong     = &Error{ReturnCode: 0x13, Status: 400, Message: "Topic can not have more than 23 parts."}
	ErrNotImplemented    = &Error{ReturnCode: 0x14, Status: 501, Message: "The server does not recognize the request method."}
	ErrKeyGenForbidden   = &Error{ReturnCode: 0x15, Status: 403, Message: "Unacceptable identifier, use the primary client Id to generate keys."}
	// Return codes 0x16 and 0x17 were v0.6.0's refusals of a key with a ttl,
	// and of revoking everything a contract issued, while a node of the
	// cluster read no v2 ids and keys; since v0.7.0 the server issues v2
	// ones only, and refuses neither.

	// ErrV1ClientID refuses a v1 client id at CONNECT (the return code of
	// ErrInvalidClientID), without sending a new id: the id's owner seals
	// it again as a v2 id with the same contract (server/cmd/mintid -from),
	// or the client connects once to a v0.6.0 server, which sends it one.
	ErrV1ClientID = &Error{ReturnCode: 0x02, Status: 401, Message: "Identifier rejected. v1 client IDs are no longer accepted: use a v2 client ID."}
	// ErrV1Key refuses a v1 signed topic key or an unsigned one: generate a
	// v2 key with unitdb/keygen.
	ErrV1Key = &Error{ReturnCode: 0x04, Status: 401, Message: "Security key rejected. v1 and unsigned security keys are no longer accepted: generate a v2 key with keygen."}
)

type KeyGenRequest struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	// Ttl is how long the key lasts, as a duration such as "24h"; empty is
	// the server's topic_key_ttl, "0" never expires.
	Ttl string `json:"ttl,omitempty"`
}

func (m *KeyGenRequest) Access() uint32 {
	required := security.AllowNone

	for i := 0; i < len(m.Type); i++ {
		switch c := m.Type[i]; c {
		case 'o':
			required |= security.AllowOwner | security.AllowAdmin | security.AllowReadWrite
		case 'a':
			required |= security.AllowAdmin | security.AllowReadWrite
		case 'r':
			required |= security.AllowRead
		case 'w':
			required |= security.AllowWrite
		}
	}

	return required
}

type KeyGenResponse struct {
	Status int    `json:"status"`
	Key    string `json:"key"`
	Topic  string `json:"topic"`
	// Uuid identifies the key, in decimal, to revoke it (unitdb/revoke).
	Uuid string `json:"uuid,omitempty"`
}

// RevokeRequest is a unitdb/revoke request: it revokes client ids and topic
// keys of the requester's contract. Uuid revokes the one with that uuid, in
// decimal, until Until (unix seconds; 0 for ever). All revokes every one the
// contract issued before now.
type RevokeRequest struct {
	Uuid  string `json:"uuid,omitempty"`
	Until int64  `json:"until,omitempty"`
	All   bool   `json:"all,omitempty"`
}

// RevokeResponse answers a unitdb/revoke request that was taken.
type RevokeResponse struct {
	Status int `json:"status"`
}

// ServiceResponse answers a unitdb/service request that vouched for the
// connection.
type ServiceResponse struct {
	Status int `json:"status"`
}

type ClientIdResponse struct {
	Status   int    `json:"status"`
	ClientId string `json:"key"`
	// Uuid identifies the client id, in decimal, to revoke it
	// (unitdb/revoke).
	Uuid string `json:"uuid,omitempty"`
}
