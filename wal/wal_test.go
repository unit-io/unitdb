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

package wal

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

var (
	dbPath = "test"
	logDir = "logs"
)

func newTestWal(del bool) (*WAL, error) {
	logOpts := Options{Path: dbPath + "/" + logDir, BufferSize: 1 << 8}
	if del {
		os.RemoveAll(dbPath)
	}
	// Make sure we have a directory.
	if err := os.MkdirAll(dbPath, 0777); err != nil {
		return nil, errors.New("newTestWal, Unable to create dir")
	}
	return New(logOpts)
}

func TestEmptyLog(t *testing.T) {
	wal, err := newTestWal(true)
	if len(wal.recoveredTimeIDs) != 0 || err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
}

func TestRecovery(t *testing.T) {
	wal, err := newTestWal(true)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	if len(wal.recoveredTimeIDs) != 0 {
		t.Fatalf("Write ahead log non-empty")
	}

	var i uint16
	var n uint16 = 1000

	logWriter, err := wal.NewWriter()
	if err != nil {
		t.Fatal(err)
	}

	for i = 0; i < n; i++ {
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := <-logWriter.Append(val); err != nil {
			t.Fatal(err)
		}
	}

	if err := <-logWriter.SignalInitWrite(int64(n)); err != nil {
		t.Fatal(err)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	wal, err = newTestWal(false)
	if len(wal.recoveredTimeIDs) == 0 || err != nil {
		t.Fatal(err)
	}
}

func TestLogApplied(t *testing.T) {
	wal, err := newTestWal(true)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	var i uint16
	var n uint16 = 1000

	logWriter, err := wal.NewWriter()
	if err != nil {
		t.Fatal(err)
	}

	for i = 0; i < n; i++ {
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := <-logWriter.Append(val); err != nil {
			t.Fatal(err)
		}
	}

	if err := <-logWriter.SignalInitWrite(int64(n)); err != nil {
		t.Fatal(err)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err = newTestWal(false)
	if len(wal.recoveredTimeIDs) == 0 || err != nil {
		t.Fatal(err)
	}

	r, err := wal.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	err = r.Read(func(timeID int64) (bool, error) {
		for {
			_, ok, err := r.Next()
			if !ok || err != nil {
				break
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSimple(t *testing.T) {
	wal, err := newTestWal(true)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	var i uint16
	var n uint16 = 1000

	logWriter, err := wal.NewWriter()
	if err != nil {
		t.Fatal(err)
	}

	for i = 0; i < n; i++ {
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := <-logWriter.Append(val); err != nil {
			t.Fatal(err)
		}
	}

	if err := <-logWriter.SignalInitWrite(int64(n)); err != nil {
		t.Fatal(err)
	}

	if err := wal.SignalLogApplied(int64(n)); err != nil {
		t.Fatal(err)
	}

}
