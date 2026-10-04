package unitdb

// The model tests run random operations on the DB and on a model of it, a
// map of each topic's messages, and check after each that every query
// agrees with the model and the DB passes Verify. TestModelCrash runs them
// in a child process killed at random, and checks that the DB reopens to
// the model after the operations made durable, and maybe some after.
//
// A failing run prints its seed; run it again with
//
//	go test . -run 'TestModel$' -model.seed=<seed> -v

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	modelSeed = flag.Int64("model.seed", 0, "run the model tests with this seed only")
	modelRuns = flag.Int("model.runs", 6, "number of seeds the model tests run")
	modelOps  = flag.Int("model.ops", 300, "operations in each model test run")
)

const modelTopics = 4

var errModelAbort = errors.New("aborted")

func modelTopic(t int) []byte { return []byte(fmt.Sprintf("unit.model.t%d", t)) }

type modelOpKind int

const (
	opPut modelOpKind = iota
	opDelete
	opBatch
	opSync
	opFlush
	opSleep
	opReopen
)

// modelOp is an operation; a delete names the put whose message it deletes.
type modelOp struct {
	kind  modelOpKind
	topic int
	ref   int   // opDelete: the op that put the message
	puts  []int // opBatch: the topic of each message
	keep  bool  // opBatch: commit, or abort
	sleep time.Duration
}

func (o modelOp) String() string {
	switch o.kind {
	case opPut:
		return fmt.Sprintf("Put(t%d)", o.topic)
	case opDelete:
		return fmt.Sprintf("Delete(message of op %d)", o.ref)
	case opBatch:
		return fmt.Sprintf("Batch(topics %v, kept %v)", o.puts, o.keep)
	case opSync:
		return "Sync"
	case opFlush:
		return "Flush"
	case opSleep:
		return fmt.Sprintf("sleep %s", o.sleep)
	}
	return "reopen"
}

// modelPayload is the message an op puts; the j-th of a batch.
func modelPayload(i, j int) string { return fmt.Sprintf("m%d.%d", i, j) }

// genOps returns n random operations; reopen is left out for the child of
// the crash test, which is killed instead.
func genOps(seed int64, n int, reopen bool) []modelOp {
	rnd := rand.New(rand.NewSource(seed))
	ops := make([]modelOp, 0, n)
	var puts []int
	for i := 0; i < n; i++ {
		var o modelOp
		switch p := rnd.Intn(100); {
		case p < 45:
			o = modelOp{kind: opPut, topic: rnd.Intn(modelTopics)}
			puts = append(puts, i)
		case p < 65 && len(puts) > 0:
			o = modelOp{kind: opDelete, ref: puts[rnd.Intn(len(puts))]}
		case p < 72:
			o = modelOp{kind: opBatch, keep: rnd.Intn(4) != 0}
			for j := rnd.Intn(4); j >= 0; j-- {
				o.puts = append(o.puts, rnd.Intn(modelTopics))
			}
			if o.keep {
				puts = append(puts, i)
			}
		case p < 80:
			o = modelOp{kind: opSync}
		case p < 86:
			o = modelOp{kind: opFlush}
		case p < 96 || !reopen:
			o = modelOp{kind: opSleep, sleep: time.Duration(rnd.Intn(30)) * time.Millisecond}
		default:
			o = modelOp{kind: opReopen}
		}
		ops = append(ops, o)
	}
	return ops
}

// rootModel is the topics' messages, oldest first.
type rootModel struct {
	topics  [modelTopics][]string
	topicOf map[string]int
}

func newRootModel() *rootModel { return &rootModel{topicOf: make(map[string]int)} }

func (m *rootModel) apply(i int, o modelOp) {
	switch o.kind {
	case opPut:
		m.put(o.topic, modelPayload(i, 0))
	case opBatch:
		if o.keep {
			for j, t := range o.puts {
				m.put(t, modelPayload(i, j))
			}
		}
	case opDelete:
		// A batch's delete deletes its first message.
		p := modelPayload(o.ref, 0)
		t, ok := m.topicOf[p]
		if !ok {
			return
		}
		for k, q := range m.topics[t] {
			if q == p {
				m.topics[t] = append(m.topics[t][:k:k], m.topics[t][k+1:]...)
				break
			}
		}
		delete(m.topicOf, p)
	}
}

func (m *rootModel) put(t int, p string) {
	m.topics[t] = append(m.topics[t], p)
	m.topicOf[p] = t
}

// check compares the DB's queries with the model.
func (m *rootModel) check(db *DB) error {
	for t := 0; t < modelTopics; t++ {
		got, err := queryAll(db, modelTopic(t))
		if err != nil {
			return fmt.Errorf("Get(t%d): %v", t, err)
		}
		if !equalStrings(got, m.topics[t]) {
			return fmt.Errorf("Get(t%d) = %v; want %v", t, got, m.topics[t])
		}
	}
	return db.Verify()
}

// queryAll returns the topic's messages, oldest first.
func queryAll(db *DB, topic []byte) ([]string, error) {
	items, err := db.Get(NewQuery(topic).WithLimit(100000))
	if err != nil {
		return nil, err
	}
	msgs := make([]string, len(items))
	for i, item := range items {
		msgs[len(items)-1-i] = string(item)
	}
	return msgs, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func modelDBOpts() []Options {
	return append(crashOpts(), WithMaxSyncDuration(50*time.Millisecond, 1))
}

// modelRunner applies operations to a DB, keeping the ids of the messages
// put, which deletes take.
type modelRunner struct {
	dir string
	db  *DB
	ids map[int][]byte
	top map[int]int
}

func (r *modelRunner) apply(i int, o modelOp) error {
	db := r.db
	switch o.kind {
	case opPut:
		id := db.NewID()
		if err := db.PutEntry(NewEntry(modelTopic(o.topic), []byte(modelPayload(i, 0))).WithID(id)); err != nil {
			return err
		}
		r.ids[i], r.top[i] = id, o.topic
	case opDelete:
		id, ok := r.ids[o.ref]
		if !ok {
			return nil
		}
		return db.Delete(id, modelTopic(r.top[o.ref]))
	case opBatch:
		err := db.Batch(func(b *Batch, _ <-chan struct{}) error {
			for j, t := range o.puts {
				id := db.NewID()
				if err := b.PutEntry(NewEntry(modelTopic(t), []byte(modelPayload(i, j))).WithID(id)); err != nil {
					return err
				}
				if j == 0 {
					r.ids[i], r.top[i] = id, t
				}
			}
			if !o.keep {
				return errModelAbort
			}
			return nil
		})
		if !o.keep {
			delete(r.ids, i)
			if err == errModelAbort {
				err = nil
			}
		}
		return err
	case opSync:
		return db.Sync()
	case opFlush:
		return db.Flush()
	case opSleep:
		time.Sleep(o.sleep)
	case opReopen:
		if err := db.Close(); err != nil {
			return err
		}
		db, err := Open(r.dir, modelDBOpts()...)
		if err != nil {
			return err
		}
		r.db = db
	}
	return nil
}

func modelSeeds() []int64 {
	if *modelSeed != 0 {
		return []int64{*modelSeed}
	}
	seeds := make([]int64, *modelRuns)
	base := time.Now().UnixNano()
	for i := range seeds {
		seeds[i] = base + int64(i)
	}
	return seeds
}

func traceOps(ops []modelOp, upTo int) string {
	var b strings.Builder
	start := 0
	if upTo > 40 {
		start = upTo - 40
	}
	for i := start; i <= upTo && i < len(ops); i++ {
		fmt.Fprintf(&b, "  %4d %s\n", i, ops[i])
	}
	return b.String()
}

func TestModel(t *testing.T) {
	for _, seed := range modelSeeds() {
		seed := seed
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ops := genOps(seed, *modelOps, true)
			dir := t.TempDir()
			db, err := Open(dir, modelDBOpts()...)
			if err != nil {
				t.Fatal(err)
			}
			r := &modelRunner{dir: dir, db: db, ids: make(map[int][]byte), top: make(map[int]int)}
			defer func() { r.db.Close() }()
			m := newRootModel()
			for i, o := range ops {
				if err := r.apply(i, o); err != nil {
					t.Fatalf("seed %d: op %d (%s): %v\n%s", seed, i, o, err, traceOps(ops, i))
				}
				m.apply(i, o)
				if err := m.check(r.db); err != nil {
					t.Fatalf("seed %d: op %d (%s): %v\n%s", seed, i, o, err, traceOps(ops, i))
				}
			}
		})
	}
}

const modelSeedEnv = "UNITDB_MODEL_SEED"

// TestModelCrashChild is the child of TestModelCrash: it applies the ops of
// the seed, and acknowledges each Flush and kept Batch once it returns: a
// Flush makes the ops before it durable, and a Batch its own messages.
func TestModelCrashChild(t *testing.T) {
	seedEnv := os.Getenv(modelSeedEnv)
	if seedEnv == "" {
		t.Skip("run by TestModelCrash")
	}
	seed, _ := strconv.ParseInt(seedEnv, 10, 64)
	n, _ := strconv.Atoi(os.Getenv(crashStartEnv))
	dir := os.Getenv(crashDirEnv)
	db, err := Open(dir, modelDBOpts()...)
	if err != nil {
		fmt.Println("error", err)
		os.Exit(2)
	}
	r := &modelRunner{dir: dir, db: db, ids: make(map[int][]byte), top: make(map[int]int)}
	for i, o := range genOps(seed, n, false) {
		if err := r.apply(i, o); err != nil {
			fmt.Println("error", err)
			os.Exit(2)
		}
		if o.kind == opFlush || (o.kind == opBatch && o.keep) {
			fmt.Printf("ack %d\n", i)
		}
	}
	// Every op is durable once done is acknowledged.
	if err := r.db.Flush(); err != nil {
		fmt.Println("error", err)
		os.Exit(2)
	}
	fmt.Println("ack done")
	select {}
}

// TestModelCrash kills a child applying random operations at a random
// acknowledgement, and reopens the DB. Each topic must hold every message of
// the model after the last Flush acknowledged, and of the batches
// acknowledged since, but those deleted since; and no message deleted
// before that Flush. It may hold more of the messages put: the child goes
// on until it is killed, and writes in the background.
func TestModelCrash(t *testing.T) {
	for _, seed := range modelSeeds() {
		seed := seed
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ops := genOps(seed, *modelOps, false)
			acks := 0
			for _, o := range ops {
				if o.kind == opFlush || (o.kind == opBatch && o.keep) {
					acks++
				}
			}
			killAt := 1 + rand.New(rand.NewSource(seed)).Intn(acks+1)
			dir := t.TempDir()
			n, lastFlush := 0, -1
			var batches []int
			crashChildWith(t, []string{modelSeedEnv + "=" + strconv.FormatInt(seed, 10)}, dir, len(ops), func(ack string) bool {
				n++
				if ack == "done" {
					lastFlush = len(ops) - 1
					return true
				}
				i, _ := strconv.Atoi(ack)
				if ops[i].kind == opFlush {
					lastFlush, batches = i, nil
				} else {
					batches = append(batches, i)
				}
				return n >= killAt
			})

			db, err := Open(dir, modelDBOpts()...)
			if err != nil {
				t.Fatalf("seed %d: reopen after the kill: %v", seed, err)
			}
			defer db.Close()
			if err := checkCrash(db, ops, lastFlush, batches); err != nil {
				t.Fatalf("seed %d: killed after the flush of op %d and batches %v: %v\n%s", seed, lastFlush, batches, err, traceOps(ops, lastFlush))
			}
		})
	}
}

// checkCrash checks the DB reopened after a kill; see TestModelCrash.
func checkCrash(db *DB, ops []modelOp, lastFlush int, batches []int) error {
	if err := db.Verify(); err != nil {
		return err
	}
	m := newRootModel()
	for i := 0; i <= lastFlush; i++ {
		m.apply(i, ops[i])
	}
	durable := make(map[string]bool)
	for _, msgs := range m.topics {
		for _, p := range msgs {
			durable[p] = true
		}
	}
	for _, i := range batches {
		for j := range ops[i].puts {
			durable[modelPayload(i, j)] = true
		}
	}
	// Messages a later op deletes may be gone; and any put may be there.
	allowed := make(map[string]int)
	for i, o := range ops {
		switch {
		case o.kind == opPut:
			allowed[modelPayload(i, 0)] = o.topic
		case o.kind == opBatch && o.keep:
			for j, t := range o.puts {
				allowed[modelPayload(i, j)] = t
			}
		case o.kind == opDelete && i > lastFlush:
			delete(durable, modelPayload(o.ref, 0))
		}
	}
	for i := 0; i <= lastFlush; i++ {
		if ops[i].kind == opDelete {
			delete(allowed, modelPayload(ops[i].ref, 0))
		}
	}
	for t := 0; t < modelTopics; t++ {
		got, err := queryAll(db, modelTopic(t))
		if err != nil {
			return fmt.Errorf("Get(t%d): %v", t, err)
		}
		prev := ""
		for _, p := range got {
			if tp, ok := allowed[p]; !ok || tp != t {
				return fmt.Errorf("Get(t%d) holds %s, deleted or never put on the topic: %v", t, p, got)
			}
			if prev != "" && !payloadBefore(prev, p) {
				return fmt.Errorf("Get(t%d) holds %s before %s: %v", t, prev, p, got)
			}
			prev = p
			delete(durable, p)
		}
	}
	for p := range durable {
		return fmt.Errorf("%s, durable, is lost", p)
	}
	return nil
}

// payloadBefore reports whether message p was put before q.
func payloadBefore(p, q string) bool {
	var pi, pj, qi, qj int
	fmt.Sscanf(p, "m%d.%d", &pi, &pj)
	fmt.Sscanf(q, "m%d.%d", &qi, &qj)
	return pi < qi || (pi == qi && pj < qj)
}

// crashChildWith is crashChild for a child given env, running
// TestModelCrashChild.
func crashChildWith(t *testing.T, env []string, dir string, start int, kill func(ack string) bool) string {
	t.Helper()
	saved := crashChildTest
	crashChildTest = "^TestModelCrashChild$"
	crashChildEnv = env
	defer func() { crashChildTest, crashChildEnv = saved, nil }()
	return crashChild(t, "model", dir, start, kill)
}
