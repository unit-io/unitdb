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
)

type KeyGenRequest struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
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
}

type ClientIdResponse struct {
	Status   int    `json:"status"`
	ClientId string `json:"key"`
}
