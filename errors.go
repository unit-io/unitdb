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

package unitdb

import (
	"errors"
)

var (
	errTopicEmpty          = errors.New("Topic is empty")
	errMsgIdEmpty          = errors.New("Message Id is empty")
	errMsgIdDeleted        = errors.New("Message Id is deleted")
	errMsgIdDoesNotExist   = errors.New("Message Id does not exist in database")
	errMsgIdPrefixMismatch = errors.New("Message Id does not match topic or Contract")
	errTtlTooLarge         = errors.New("TTL is too large")
	errTopicTooLarge       = errors.New("Topic is too large")
	errMsgExpired          = errors.New("Message has expired")
	errValueEmpty          = errors.New("Payload is empty")
	errValueTooLarge       = errors.New("value is too large")
	errEntryInvalid        = errors.New("entry is invalid")
	errImmutable           = errors.New("database is immutable")
	errFull                = errors.New("database is full")
	errCorrupted           = errors.New("database is corrupted")
	errLocked              = errors.New("database is locked")
	errClosed              = errors.New("database is closed")
	errBatchSeqComplete    = errors.New("batch seq is complete")
	errWriteConflict       = errors.New("batch write conflict")
	errBadRequest          = errors.New("The request was invalid or cannot be otherwise served")
	errForbidden           = errors.New("The request is understood, but it has been refused or access is not allowed")
)
