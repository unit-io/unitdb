package memdb

// The model tests run random operations on the DB and on a model of it, and
// check after each that the DB agrees with the model and passes Verify. The
// model is the DB's design kept simple: a key has a version in each time
// block it was put in, put again in a block replaces that block's version,
// Get returns the newest block's version, and Delete removes it.
//
// A failing run prints its seed; run it again with
//
//	go test ./memdb -run TestModel -model.seed=<seed> -v

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

var (
	modelSeed = flag.Int64("model.seed", 0, "run the model test with this seed only")
	modelRuns = flag.Int("model.runs", 20, "number of seeds the model test runs")
	modelOps  = flag.Int("model.ops", 400, "operations in each model test run")
	modelFull = flag.Bool("model.trace", false, "print every operation of a failing run")
)

const (
	modelKeys          = 24
	modelBlockDuration = 20 * time.Millisecond
)

type modelVersion struct {
	timeID int64
	val    []byte
}

type model struct {
	versions map[uint64][]modelVersion // oldest first
}

func newModel() *model { return &model{versions: make(map[uint64][]modelVersion)} }

func (m *model) put(timeID int64, key uint64, val []byte) {
	vs := m.versions[key]
	for i := range vs {
		if vs[i].timeID == timeID {
			vs[i].val = val
			return
		}
	}
	vs = append(vs, modelVersion{timeID: timeID, val: val})
	sort.Slice(vs, func(i, j int) bool { return vs[i].timeID < vs[j].timeID })
	m.versions[key] = vs
}

func (m *model) delete(key uint64) bool {
	vs := m.versions[key]
	if len(vs) == 0 {
		return false
	}
	if len(vs) == 1 {
		delete(m.versions, key)
	} else {
		m.versions[key] = vs[:len(vs)-1]
	}
	return true
}

func (m *model) size() int64 {
	n := 0
	for _, vs := range m.versions {
		n += len(vs)
	}
	return int64(n)
}

func (m *model) clone() *model {
	c := newModel()
	for k, vs := range m.versions {
		c.versions[k] = append([]modelVersion(nil), vs...)
	}
	return c
}

// check compares the DB with the model.
func (m *model) check(db *DB) error {
	if err := db.Verify(); err != nil {
		return err
	}
	for key := uint64(0); key < modelKeys; key++ {
		got, err := db.Get(key)
		vs := m.versions[key]
		if len(vs) == 0 {
			if err == nil {
				return fmt.Errorf("Get(%d) = %q; it was deleted or never put", key, got)
			}
			continue
		}
		if want := vs[len(vs)-1].val; err != nil || !bytes.Equal(got, want) {
			return fmt.Errorf("Get(%d) = %q, %v; want %q", key, got, err, want)
		}
	}
	if got, want := db.Size(), m.size(); got != want {
		return fmt.Errorf("Size() = %d; want %d", got, want)
	}
	return nil
}

func modelOpen(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(WithLogFilePath(dir), WithMemdbSize(1<<26), WithBufferSize(1<<20), WithTimeBlockInterval(modelBlockDuration), WithLogInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	return db
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

func TestModel(t *testing.T) {
	for _, seed := range modelSeeds() {
		seed := seed
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			if err := runModel(t, seed, *modelOps); err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
		})
	}
}

// runModel runs ops random operations on a new DB, and returns the first
// disagreement with the model, after the operations that led to it.
func runModel(t *testing.T, seed int64, ops int) error {
	rnd := rand.New(rand.NewSource(seed))
	dir := t.TempDir()
	db := modelOpen(t, dir)
	defer func() { db.Close() }()
	m := newModel()
	var trace []modelOp
	fail := func(err error) error {
		return fmt.Errorf("%v\n%s", err, traceOf(trace, err))
	}
	for i := 0; i < ops; i++ {
		key := uint64(rnd.Intn(modelKeys))
		val := []byte(fmt.Sprintf("v%d", i))
		var op string
		opKeys := []uint64{key}
		switch p := rnd.Intn(100); {
		case p < 45:
			timeID, err := db.Put(key, val)
			if err != nil {
				return fail(fmt.Errorf("op %d: Put(%d): %v", i, key, err))
			}
			m.put(timeID, key, val)
			op = fmt.Sprintf("Put(%d, %s) in %d", key, val, timeID)
		case p < 70:
			err := db.Delete(key)
			want := m.delete(key)
			op = fmt.Sprintf("Delete(%d) = %v", key, err)
			if (err == nil) != want {
				trace = append(trace, modelOp{i, op, opKeys})
				return fail(fmt.Errorf("op %d: Delete(%d) = %v; the key has a version: %v", i, key, err, want))
			}
		case p < 78:
			// A batch of puts, kept or aborted.
			n := 1 + rnd.Intn(4)
			keep := rnd.Intn(4) != 0
			var keys []uint64
			var timeID int64
			err := db.Batch(func(b *Batch, _ <-chan struct{}) error {
				timeID = b.TimeID()
				for j := 0; j < n; j++ {
					k := uint64(rnd.Intn(modelKeys))
					if err := b.Put(k, []byte(fmt.Sprintf("v%d.%d", i, j))); err != nil {
						return err
					}
					keys = append(keys, k)
				}
				if !keep {
					return errAborted
				}
				return nil
			})
			if keep && err != nil {
				return fail(fmt.Errorf("op %d: Batch: %v", i, err))
			}
			if keep {
				for j, k := range keys {
					m.put(timeID, k, []byte(fmt.Sprintf("v%d.%d", i, j)))
				}
			}
			op = fmt.Sprintf("Batch(%v, kept %v) in %d", keys, keep, timeID)
			opKeys = keys
		case p < 88:
			time.Sleep(time.Duration(rnd.Intn(int(2 * modelBlockDuration))))
			op = "sleep"
			opKeys = nil
		case p < 94:
			if err := db.Flush(); err != nil {
				return fail(fmt.Errorf("op %d: Flush: %v", i, err))
			}
			op = "Flush"
			opKeys = nil
		default:
			if err := db.Close(); err != nil {
				return fail(fmt.Errorf("op %d: Close: %v", i, err))
			}
			// The DB is as it was: recovery puts each log back in its block.
			db = modelOpen(t, dir)
			op = "reopen"
			opKeys = nil
		}
		trace = append(trace, modelOp{i, op, opKeys})
		if err := m.check(db); err != nil {
			return fail(fmt.Errorf("op %d (%s): %v", i, op, err))
		}
	}
	return nil
}

var errAborted = fmt.Errorf("aborted")

type modelOp struct {
	i    int
	op   string
	keys []uint64
}

// traceOf returns the operations that bear on the error: those on the key
// it names, if it names one, with the reopens and flushes; or else the last
// ones.
func traceOf(trace []modelOp, err error) string {
	var key uint64
	_, scanErr := fmt.Sscanf(keyOfErr(err.Error()), "%d", &key)
	var b bytes.Buffer
	start := 0
	if scanErr != nil && len(trace) > 40 {
		start = len(trace) - 40
	}
	if *modelFull {
		start, scanErr = 0, fmt.Errorf("all")
	}
	for _, o := range trace[start:] {
		show := scanErr != nil || o.op == "reopen" || o.op == "Flush"
		for _, k := range o.keys {
			show = show || k == key
		}
		if show {
			fmt.Fprintf(&b, "  %4d %s\n", o.i, o.op)
		}
	}
	return b.String()
}

// keyOfErr returns what follows "Get(" or "key " in msg.
func keyOfErr(msg string) string {
	for _, p := range []string{"Get(", "Delete(", "key "} {
		if i := bytes.Index([]byte(msg), []byte(p)); i >= 0 {
			return msg[i+len(p):]
		}
	}
	return ""
}
