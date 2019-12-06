package tracedb

import (
	"errors"
)

var (
	errIdEmpty          = errors.New("Id is empty")
	errIdDoesNotExist   = errors.New("Id does not exist in database")
	errIdPrefixMismatch = errors.New("Id prefix not matched")
	errIdTooLarge       = errors.New("Id is too large")
	errIdExpired        = errors.New("Id has expired")
	errValueTooLarge    = errors.New("value is too large")
	errFull             = errors.New("database is full")
	errCorrupted        = errors.New("database is corrupted")
	errLocked           = errors.New("database is locked")
	errClosed           = errors.New("database is closed")
	errBatchSeqComplete = errors.New("batch seq is complete")
	errWriteConflict    = errors.New("batch write conflict")
	errBadRequest       = errors.New("The request was invalid or cannot be otherwise served")
	errForbidden        = errors.New("The request is understood, but it has been refused or access is not allowed")
)
