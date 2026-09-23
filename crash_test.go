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

// Crash tests run the writer in a child process (this test binary re-run
// with TestCrashChild) and kill it with SIGKILL, so nothing is flushed or
// closed. The parent then reopens the DB and checks what was restored.

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	crashScenarioEnv = "UNITDB_CRASH_SCENARIO"
	crashDirEnv      = "UNITDB_CRASH_DIR"
	crashStartEnv    = "UNITDB_CRASH_START"
)

var crashTopic = []byte("unit.crash")

func crashOpts() []Options {
	return []Options{WithBufferSize(1 << 20), WithMemdbSize(1 << 24), WithMutable(), WithDefaultQueryLimit(100000)}
}

// walFlush is long enough for memdb's background WAL write (every 15ms).
const walFlush = 100 * time.Millisecond

// TestCrashChild is the child process; it is skipped unless run by a crash test.
func TestCrashChild(t *testing.T) {
	scenario := os.Getenv(crashScenarioEnv)
	if scenario == "" {
		t.Skip("run by the crash tests")
	}
	dir := os.Getenv(crashDirEnv)
	start, _ := strconv.Atoi(os.Getenv(crashStartEnv))
	opts := crashOpts()
	if scenario == "no-sync" {
		opts = append(opts, WithMaxSyncDuration(time.Hour, 1))
	}
	db, err := Open(dir, opts...)
	if err != nil {
		fmt.Println("error", err)
		os.Exit(2)
	}
	ack := func(format string, v ...interface{}) { fmt.Printf("ack "+format+"\n", v...) }
	put := func(i int) {
		if err := db.Put(crashTopic, []byte(fmt.Sprintf("m%d", i))); err != nil {
			fmt.Println("error", err)
			os.Exit(2)
		}
	}
	syncAllChild := func(want uint64) {
		for db.Count() < want {
			time.Sleep(50 * time.Millisecond)
			if err := db.Sync(); err != nil {
				fmt.Println("error", err)
				os.Exit(2)
			}
		}
	}

	switch scenario {
	case "no-sync", "synced":
		for i := 0; i < 500; i++ {
			put(i)
		}
		if scenario == "synced" {
			syncAllChild(500)
		}
		time.Sleep(walFlush)
		ack("done")
	case "deletes":
		var ids [][]byte
		for i := 0; i < 30; i++ {
			id := db.NewID()
			if err := db.PutEntry(NewEntry(crashTopic, []byte(fmt.Sprintf("m%d", i))).WithID(id)); err != nil {
				fmt.Println("error", err)
				os.Exit(2)
			}
			ids = append(ids, id)
		}
		syncAllChild(20)
		// Delete even messages: 0..19 may be synced, 20..29 are in memory.
		for i := 0; i < 30; i += 2 {
			if err := db.Delete(ids[i], crashTopic); err != nil {
				fmt.Println("error", err)
				os.Exit(2)
			}
		}
		time.Sleep(walFlush)
		ack("done")
	case "writes":
		// Put continuously, acknowledging every message older than a WAL flush.
		for i := start; ; {
			for end := i + 50; i < end; i++ {
				put(i)
			}
			time.Sleep(walFlush)
			ack("%d", i)
		}
	case "batches":
		// Batch returns only after its WAL write, so every returned batch is durable.
		for b := start; ; b++ {
			err := db.Batch(func(bt *Batch, completed <-chan struct{}) error {
				for i := 0; i < 50; i++ {
					if err := bt.Put(crashTopic, []byte(fmt.Sprintf("b%d-%d", b, i))); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				fmt.Println("error", err)
				os.Exit(2)
			}
			ack("%d", b)
		}
	default:
		fmt.Println("error unknown scenario", scenario)
		os.Exit(2)
	}
	select {} // wait to be killed
}

// crashChild runs scenario in a child process on dir and kills it with
// SIGKILL once kill returns true for an acknowledged line. It returns the
// last acknowledgement.
func crashChild(t *testing.T, scenario, dir string, start int, kill func(ack string) bool) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), crashScenarioEnv+"="+scenario, crashDirEnv+"="+dir, crashStartEnv+"="+strconv.Itoa(start))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string)
	go func() {
		s := bufio.NewScanner(stdout)
		for s.Scan() {
			lines <- s.Text()
		}
		close(lines)
	}()

	last := ""
	timeout := time.After(30 * time.Second)
LOOP:
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("child exited before being killed; last ack %q", last)
			}
			if strings.HasPrefix(line, "error") || strings.HasPrefix(line, "panic") {
				cmd.Process.Kill()
				t.Fatalf("child: %s", line)
			}
			if strings.HasPrefix(line, "ack ") {
				last = strings.TrimPrefix(line, "ack ")
				if kill(last) {
					break LOOP
				}
			}
		case <-timeout:
			cmd.Process.Kill()
			t.Fatalf("child timed out; last ack %q", last)
		}
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	for range lines {
	}
	return last
}

// restore reopens dir after a crash and returns all messages, oldest first.
func restore(t *testing.T, dir string) (*DB, []string) {
	t.Helper()
	db, err := Open(dir, crashOpts()...)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	items, err := db.Get(NewQuery(crashTopic))
	if err != nil {
		db.Close()
		t.Fatalf("get after crash: %v", err)
	}
	msgs := make([]string, len(items))
	for i, item := range items {
		msgs[len(items)-1-i] = string(item)
	}
	return db, msgs
}

func assertUnique(t *testing.T, msgs []string) {
	t.Helper()
	seen := make(map[string]bool, len(msgs))
	for _, m := range msgs {
		if seen[m] {
			t.Fatalf("message %q restored twice", m)
		}
		seen[m] = true
	}
}

func expectMsgs(n int) []string {
	msgs := make([]string, n)
	for i := range msgs {
		msgs[i] = fmt.Sprintf("m%d", i)
	}
	return msgs
}

func assertEqualMsgs(t *testing.T, want, got []string) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("expected %d messages; got %d", len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("message %d: expected %q; got %q", i, want[i], got[i])
		}
	}
}

func TestCrashBeforeSync(t *testing.T) {
	dir := t.TempDir()
	crashChild(t, "no-sync", dir, 0, func(string) bool { return true })

	// Only the WAL has the messages; restore must replay it.
	db, msgs := restore(t, dir)
	defer db.Close()
	assertEqualMsgs(t, expectMsgs(500), msgs)
	if count := db.Count(); count != 500 {
		t.Fatalf("expected count 500 after recovery; got %d", count)
	}
}

func TestCrashAfterSync(t *testing.T) {
	dir := t.TempDir()
	crashChild(t, "synced", dir, 0, func(string) bool { return true })

	db, msgs := restore(t, dir)
	defer db.Close()
	assertEqualMsgs(t, expectMsgs(500), msgs)
	if count := db.Count(); count != 500 {
		t.Fatalf("expected count 500; got %d", count)
	}
}

func TestCrashAfterDeletes(t *testing.T) {
	dir := t.TempDir()
	crashChild(t, "deletes", dir, 0, func(string) bool { return true })

	db, msgs := restore(t, dir)
	defer db.Close()
	var want []string
	for i := 1; i < 30; i += 2 {
		want = append(want, fmt.Sprintf("m%d", i))
	}
	assertEqualMsgs(t, want, msgs)
	if count := db.Count(); count != uint64(len(want)) {
		t.Fatalf("expected count %d; got %d", len(want), count)
	}
}

// TestCrashDuringWrites kills a continuous writer at random points, including
// during background syncs, several times on the same DB.
func TestCrashDuringWrites(t *testing.T) {
	dir := t.TempDir()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	next := 0
	for cycle := 0; cycle < 4; cycle++ {
		// Kill after a random number of acknowledgements, 0.3s to 2.5s of writing.
		acks := 3 + rnd.Intn(20)
		n := 0
		last := crashChild(t, "writes", dir, next, func(string) bool { n++; return n >= acks })
		durable, _ := strconv.Atoi(last)

		db, msgs := restore(t, dir)
		assertUnique(t, msgs)
		// Every acknowledged message is restored, in order; later ones may be too.
		if len(msgs) < durable {
			db.Close()
			t.Fatalf("cycle %d: expected at least %d messages; got %d", cycle, durable, len(msgs))
		}
		for i, m := range msgs {
			if want := fmt.Sprintf("m%d", i); m != want {
				db.Close()
				t.Fatalf("cycle %d: message %d: expected %q; got %q", cycle, i, want, m)
			}
		}
		if count := db.Count(); count != uint64(len(msgs)) {
			db.Close()
			t.Fatalf("cycle %d: count %d does not match %d restored messages", cycle, count, len(msgs))
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("cycle %d: acknowledged %d, restored %d", cycle, durable, len(msgs))
		next = len(msgs)
	}
}

// TestCrashBatchAtomic kills a batch writer at random points: every returned
// batch must be restored whole, and no batch may be restored partially.
func TestCrashBatchAtomic(t *testing.T) {
	dir := t.TempDir()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	next := 0
	for cycle := 0; cycle < 3; cycle++ {
		acks := 5 + rnd.Intn(40)
		n := 0
		last := crashChild(t, "batches", dir, next, func(string) bool { n++; return n >= acks })
		acked, _ := strconv.Atoi(last)

		db, msgs := restore(t, dir)
		assertUnique(t, msgs)
		perBatch := make(map[int]int)
		for _, m := range msgs {
			var b, i int
			if _, err := fmt.Sscanf(m, "b%d-%d", &b, &i); err != nil {
				db.Close()
				t.Fatalf("cycle %d: unexpected message %q", cycle, m)
			}
			perBatch[b]++
		}
		for b, count := range perBatch {
			if count != 50 {
				db.Close()
				t.Fatalf("cycle %d: batch %d restored partially: %d of 50 messages", cycle, b, count)
			}
		}
		for b := 0; b <= acked; b++ {
			if perBatch[b] != 50 {
				db.Close()
				t.Fatalf("cycle %d: acknowledged batch %d lost (%d of 50 messages)", cycle, b, perBatch[b])
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("cycle %d: acknowledged batches 0..%d, restored %d batches", cycle, acked, len(perBatch))
		next = len(perBatch)
	}
}
