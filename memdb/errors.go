package memdb

import "errors"

var (
	errEntryDeleted      = errors.New("Entry is deleted")
	errEntryDoesNotExist = errors.New("Entry does not exist in memdb")
	errValueEmpty        = errors.New("Payload is empty")
	errValueTooLarge     = errors.New("value is too large")
	errEntryInvalid      = errors.New("Entry is invalid")
	errClosed            = errors.New("The memdb is closed")
	errBadRequest        = errors.New("The request was invalid or cannot be otherwise served")
	errForbidden         = errors.New("The request is understood, but it has been refused or access is not allowed")
)
