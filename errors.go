package tracedb

import (
	"errors"
)

var (
	errKeyEmpty         = errors.New("key is empty")
	errKeyTooLarge      = errors.New("key is too large")
	errKeyExpired       = errors.New("key has expired")
	errValueTooLarge    = errors.New("value is too large")
	errFull             = errors.New("database is full")
	errCorrupted        = errors.New("database is corrupted")
	errLocked           = errors.New("database is locked")
	errClosed           = errors.New("database is closed")
	errBatchSeqComplete = errors.New("batch seq is complete")
)
