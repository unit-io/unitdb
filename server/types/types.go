package types

import (
	"github.com/unit-io/unitdb/server/message/security"
)

// Error represents an event code which provides a more details.
type Error struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
	ID      int    `json:"id,omitempty"`
}

// Error implements error interface.
func (e *Error) Error() string { return e.Message }

// Represents a set of errors used in the handlers.
var (
	ErrInvalidClientId   = &Error{Status: 401, Message: "The client Id is invalid or missing. Use a valid client Id or use an auto generated client Id in the connection request."}
	ErrClientIdForbidden = &Error{Status: 403, Message: "The request was invalid, use primary client Id to request a secondary client Id."}
	ErrTimteout          = &Error{Status: 504, Message: "The network connection timeout."}
	ErrUnauthorized      = &Error{Status: 401, Message: "The security key provided is not authorized to perform this operation."}
	ErrBadRequest        = &Error{Status: 400, Message: "The request was invalid or cannot be otherwise served."}
	ErrForbidden         = &Error{Status: 403, Message: "The request is understood, but it has been refused or access is not allowed."}
	ErrBadToken          = &Error{Status: 403, Message: "Authentication failed."}
	ErrUnknownEpoch      = &Error{Status: 403, Message: "Unknown authentication epoch."}
	ErrNotFound          = &Error{Status: 404, Message: "The resource requested does not exist."}
	ErrServerError       = &Error{Status: 500, Message: "An unexpected condition was encountered."}
	ErrNotImplemented    = &Error{Status: 501, Message: "The server does not recognize the request method."}
	ErrTargetTooLong     = &Error{Status: 400, Message: "Topic can not have more than 23 parts."}
)

type KeyGenRequest struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
}

func (m *KeyGenRequest) Access() uint32 {
	required := security.AllowNone

	for i := 0; i < len(m.Type); i++ {
		switch c := m.Type[i]; c {
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
