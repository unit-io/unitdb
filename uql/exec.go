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

package uql

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/unit-io/unitdb"
	"github.com/unit-io/unitdb/message"
	"github.com/unit-io/unitdb/uid"
)

// DefaultLimit is what a query without LIMIT returns at most: the engine's
// default query limit.
const DefaultLimit = 1000

// scanAll asks the engine for every entry it has for the query. (With SINCE,
// the engine caps it at the database's maximum query limit.)
const scanAll = math.MaxInt32

// DB runs UQL on a unitdb database.
type DB struct {
	db  *unitdb.DB
	now func() time.Time
}

// New returns a UQL runner on db.
func New(db *unitdb.DB) *DB { return &DB{db: db, now: time.Now} }

// Entry is one entry a query returns.
type Entry struct {
	// Topic is the topic the entry was read from. An entry put to a matching
	// wildcard topic is read from it too, and the engine doesn't say which
	// entries those are; deleting one needs the wildcard topic it was put to.
	Topic   string
	ID      []byte
	Time    time.Time // from the entry ID, to the second
	Payload []byte
}

// IDString returns the entry ID as hex, as DELETE ... ID accepts it.
func (e Entry) IDString() string { return hex.EncodeToString(e.ID) }

// Rows are a query's entries, newest first.
type Rows struct {
	entries []Entry
	i       int
	err     error
}

// Next moves to the next entry.
func (r *Rows) Next() bool {
	if r.i >= len(r.entries) {
		return false
	}
	r.i++
	return true
}

// Entry returns the current entry.
func (r *Rows) Entry() Entry { return r.entries[r.i-1] }

// All returns every entry.
func (r *Rows) All() []Entry { return r.entries }

// Len returns how many entries the query returned.
func (r *Rows) Len() int { return len(r.entries) }

// Err returns an error that stopped the rows, if any.
func (r *Rows) Err() error { return r.err }

// Result reports what a PUT or DELETE did.
type Result struct {
	Affected int
}

// Plan says how a query runs.
type Plan struct {
	Statement string   // the query, re-printed with its parameters bound
	Topic     string   // as given to the engine
	Contract  uint32   // 0 is the master contract
	Limit     int      // entries asked of the engine
	Since     string   // engine cutoff, if any
	Until     string   // filtered after reading, if any
	Steps     []string // what happens, in order
}

func (p *Plan) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n", p.Statement)
	for i, s := range p.Steps {
		fmt.Fprintf(&b, "  %d. %s\n", i+1, s)
	}
	return b.String()
}

// Stmt is a parsed statement, run with different parameters.
type Stmt struct {
	db *DB
	st Statement
}

// Prepare parses a statement once.
func (d *DB) Prepare(src string) (*Stmt, error) {
	st, err := Parse(src)
	if err != nil {
		return nil, err
	}
	return &Stmt{db: d, st: st}, nil
}

// Statement returns the parsed statement.
func (s *Stmt) Statement() Statement { return s.st }

// Query runs FROM ... and returns its entries.
func (d *DB) Query(ctx context.Context, src string, args ...any) (*Rows, error) {
	s, err := d.Prepare(src)
	if err != nil {
		return nil, err
	}
	return s.Query(ctx, args...)
}

// Exec runs PUT or DELETE.
func (d *DB) Exec(ctx context.Context, src string, args ...any) (Result, error) {
	s, err := d.Prepare(src)
	if err != nil {
		return Result{}, err
	}
	return s.Exec(ctx, args...)
}

// Explain returns the plan of a query, with or without EXPLAIN in front.
func (d *DB) Explain(src string, args ...any) (*Plan, error) {
	s, err := d.Prepare(src)
	if err != nil {
		return nil, err
	}
	return s.Explain(args...)
}

// Query runs the statement, which must be a query.
func (s *Stmt) Query(ctx context.Context, args ...any) (*Rows, error) {
	q, ok := s.st.(*Query)
	if !ok {
		if _, isExplain := s.st.(*Explain); isExplain {
			return nil, errors.New("uql: use Explain for EXPLAIN")
		}
		return nil, errors.New("uql: Query runs FROM ...; use Exec for PUT and DELETE")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := s.db.bindQuery(q, args)
	if err != nil {
		return nil, err
	}
	return s.db.run(b)
}

// Exec runs the statement, which must be PUT or DELETE.
func (s *Stmt) Exec(ctx context.Context, args ...any) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	switch st := s.st.(type) {
	case *Put:
		return s.db.put(st, args)
	case *Delete:
		return s.db.delete(st, args)
	}
	return Result{}, errors.New("uql: Exec runs PUT and DELETE; use Query for FROM ...")
}

// Explain returns the statement's plan; it must be a query.
func (s *Stmt) Explain(args ...any) (*Plan, error) {
	q, ok := s.st.(*Query)
	if e, isExplain := s.st.(*Explain); isExplain {
		q, ok = e.Query, true
	}
	if !ok {
		return nil, errors.New("uql: only queries have a plan")
	}
	b, err := s.db.bindQuery(q, args)
	if err != nil {
		return nil, err
	}
	return b.plan(), nil
}

// bound is a query with its parameters resolved.
type bound struct {
	text     string
	topic    string
	contract uint32
	limit    int // what the caller wants returned
	ask      int // what the engine is asked for
	since    time.Duration
	hasSince bool
	until    time.Time
	hasUntil bool
	now      time.Time
}

func arg(args []any, n, pos int) (any, error) {
	if n < 1 || n > len(args) {
		return nil, errorf(pos, "parameter $%d has no value (%d given)", n, len(args))
	}
	return args[n-1], nil
}

func (d *DB) topic(p Pattern, args []any) (string, error) {
	if len(p.Parts) == 0 && p.Multi {
		return "...", nil
	}
	parts := make([]string, 0, len(p.Parts))
	for _, x := range p.Parts {
		switch {
		case x.Wild:
			parts = append(parts, "*")
		case x.Param > 0:
			v, err := arg(args, x.Param, p.pos)
			if err != nil {
				return "", err
			}
			s, ok := v.(string)
			if !ok {
				return "", errorf(p.pos, "topic parameter $%d must be a string, not %T", x.Param, v)
			}
			// A parameter is exactly one part: it can't add parts or wildcards.
			if err := checkPart(s); err != nil {
				return "", errorf(p.pos, "parameter $%d: %s", x.Param, err)
			}
			parts = append(parts, s)
		default:
			parts = append(parts, x.Lit)
		}
	}
	t := strings.Join(parts, ".")
	if p.Multi {
		t += "..."
	}
	return t, nil
}

func (d *DB) intArg(n *Int, args []any, what string) (int64, error) {
	if n.Param == 0 {
		return n.Value, nil
	}
	v, err := arg(args, n.Param, n.pos)
	if err != nil {
		return 0, err
	}
	var x int64
	switch t := v.(type) {
	case int:
		x = int64(t)
	case int32:
		x = int64(t)
	case int64:
		x = t
	case uint32:
		x = int64(t)
	case uint64:
		if t > math.MaxInt64 {
			return 0, errorf(n.pos, "%s parameter $%d is too large", what, n.Param)
		}
		x = int64(t)
	default:
		return 0, errorf(n.pos, "%s parameter $%d must be an integer, not %T", what, n.Param, v)
	}
	if x < 0 {
		return 0, errorf(n.pos, "%s can't be negative", what)
	}
	return x, nil
}

func (d *DB) contract(n *Int, args []any) (uint32, error) {
	if n == nil {
		return 0, nil
	}
	c, err := d.intArg(n, args, "CONTRACT")
	if err != nil {
		return 0, err
	}
	if c > math.MaxUint32 {
		return 0, errorf(n.pos, "contracts are 32-bit")
	}
	return uint32(c), nil
}

// instant resolves a Time to a point in time.
func (d *DB) instant(t *Time, args []any, now time.Time) (time.Time, error) {
	switch {
	case t.Param > 0:
		v, err := arg(args, t.Param, t.pos)
		if err != nil {
			return time.Time{}, err
		}
		switch x := v.(type) {
		case time.Time:
			return x, nil
		case time.Duration:
			if x <= 0 {
				return time.Time{}, errorf(t.pos, "durations must be positive")
			}
			return now.Add(-x), nil
		case string:
			at, err := time.Parse(time.RFC3339, x)
			if err != nil {
				return time.Time{}, errorf(t.pos, "time parameter $%d must be RFC 3339: %v", t.Param, err)
			}
			return at, nil
		}
		return time.Time{}, errorf(t.pos, "time parameter $%d must be a time.Time, time.Duration or RFC 3339 string, not %T", t.Param, v)
	case !t.At.IsZero():
		return t.At, nil
	}
	return now.Add(-t.Ago), nil
}

func (d *DB) bindQuery(q *Query, args []any) (*bound, error) {
	b := &bound{limit: DefaultLimit}
	var err error
	if b.topic, err = d.topic(q.From, args); err != nil {
		return nil, err
	}
	if b.contract, err = d.contract(q.Contract, args); err != nil {
		return nil, err
	}
	if q.Limit != nil {
		n, err := d.intArg(q.Limit, args, "LIMIT")
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, errorf(q.Limit.pos, "LIMIT must be at least 1")
		}
		b.limit = int(min(n, scanAll))
	}
	now := d.now() // once, so SINCE and UNTIL agree
	b.now = now
	if q.Since != nil {
		at, err := d.instant(q.Since, args, now)
		if err != nil {
			return nil, err
		}
		b.since, b.hasSince = now.Sub(at), true
		if b.since < 0 {
			b.since = 0
		}
	}
	if q.Until != nil {
		if b.until, err = d.instant(q.Until, args, now); err != nil {
			return nil, err
		}
		b.hasUntil = true
	}
	b.ask = b.limit
	if b.hasUntil {
		// The engine has no upper time bound: read as much as it allows and
		// filter, so LIMIT counts entries inside the window.
		b.ask = scanAll
	}
	bq := *q
	b.text = bq.String()
	return b, nil
}

func (b *bound) engineQuery() *unitdb.Query {
	q := unitdb.NewQuery([]byte(b.topic)).WithLimit(b.ask)
	if b.contract != 0 {
		q.WithContract(b.contract)
	}
	if b.hasSince {
		// WithLast takes a duration back from now, to the second.
		q.WithLast(strconv.FormatInt(int64(b.since/time.Second)+1, 10) + "s")
	}
	return q
}

func (d *DB) run(b *bound) (*Rows, error) {
	ids, items, err := d.db.GetWithIDs(b.engineQuery())
	if err != nil {
		return nil, fmt.Errorf("uql: %w", err)
	}
	rows := &Rows{entries: make([]Entry, 0, min(len(items), b.limit))}
	var cutoff time.Time
	if b.hasSince {
		cutoff = b.now.Add(-b.since).Truncate(time.Second)
	}
	for i, payload := range items {
		at := time.Unix(uid.Time(ids[i][:4]), 0)
		if b.hasUntil && at.After(b.until) {
			continue
		}
		if b.hasSince && at.Before(cutoff) {
			continue // the engine's cutoff is rounded up to the second
		}
		rows.entries = append(rows.entries, Entry{Topic: b.topic, ID: ids[i], Time: at, Payload: payload})
		if len(rows.entries) == b.limit {
			break
		}
	}
	return rows, nil
}

func (b *bound) plan() *Plan {
	p := &Plan{Statement: b.text, Topic: b.topic, Contract: b.contract, Limit: b.ask}
	contract := "the master contract"
	if b.contract != 0 {
		contract = "contract " + strconv.FormatUint(uint64(b.contract), 10)
	}
	p.Steps = append(p.Steps, fmt.Sprintf("Read topic %q, with entries put to wildcard topics that match it, in %s.", b.topic, contract))
	if b.hasSince {
		p.Since = b.since.Truncate(time.Second).String()
		p.Steps = append(p.Steps, fmt.Sprintf("Skip time-window blocks older than %s ago (engine cutoff, to the second).", p.Since))
	}
	ask := strconv.Itoa(b.ask)
	if b.ask == scanAll {
		ask = "every entry (capped by the database's max query limit when SINCE is set)"
	}
	p.Steps = append(p.Steps, "Take entries newest first: "+ask+".")
	if b.hasUntil {
		p.Until = b.until.UTC().Format(time.RFC3339)
		p.Steps = append(p.Steps, fmt.Sprintf("Drop entries after %s (filtered after reading: the engine has no upper bound).", p.Until))
		p.Steps = append(p.Steps, fmt.Sprintf("Stop at %d entries.", b.limit))
	}
	return p
}

// valueArg turns a VALUE parameter into bytes.
func valueArg(v any, n, pos int) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		if len(x) == 0 {
			return nil, errorf(pos, "VALUE $%d is empty", n)
		}
		return x, nil
	case string:
		if x == "" {
			return nil, errorf(pos, "VALUE $%d is empty", n)
		}
		return []byte(x), nil
	}
	return nil, errorf(pos, "VALUE parameter $%d must be []byte or string, not %T", n, v)
}

func (d *DB) put(st *Put, args []any) (Result, error) {
	topic, err := d.topic(st.Topic, args)
	if err != nil {
		return Result{}, err
	}
	v, err := arg(args, st.Value, st.Topic.pos)
	if err != nil {
		return Result{}, err
	}
	payload, err := valueArg(v, st.Value, st.Topic.pos)
	if err != nil {
		return Result{}, err
	}
	c, err := d.contract(st.Contract, args)
	if err != nil {
		return Result{}, err
	}
	e := unitdb.NewEntry([]byte(topic), payload)
	if c != 0 {
		e.WithContract(c)
	}
	if st.TTL > 0 {
		e.WithTTL(strconv.FormatInt(int64(st.TTL/time.Second), 10))
	}
	if err := d.db.PutEntry(e); err != nil {
		return Result{}, fmt.Errorf("uql: %w", err)
	}
	// A statement is durable when it returns.
	if err := d.db.Flush(); err != nil {
		return Result{}, fmt.Errorf("uql: %w", err)
	}
	return Result{Affected: 1}, nil
}

// idArg turns an ID parameter (bytes, or hex as Entry.IDString gives) into
// an entry ID.
func idArg(v any, n, pos int) ([]byte, error) {
	var id []byte
	switch x := v.(type) {
	case []byte:
		id = x
	case string:
		b, err := hex.DecodeString(x)
		if err != nil {
			return nil, errorf(pos, "ID parameter $%d isn't hex: %v", n, err)
		}
		id = b
	default:
		return nil, errorf(pos, "ID parameter $%d must be []byte or a hex string, not %T", n, v)
	}
	if len(id) != message.ID(id).Size() {
		return nil, errorf(pos, "ID parameter $%d must be %d bytes, not %d", n, message.ID(id).Size(), len(id))
	}
	return id, nil
}

func (d *DB) delete(st *Delete, args []any) (Result, error) {
	topic, err := d.topic(st.From, args)
	if err != nil {
		return Result{}, err
	}
	v, err := arg(args, st.ID, st.From.pos)
	if err != nil {
		return Result{}, err
	}
	id, err := idArg(v, st.ID, st.From.pos)
	if err != nil {
		return Result{}, err
	}
	c, err := d.contract(st.Contract, args)
	if err != nil {
		return Result{}, err
	}
	e := unitdb.NewEntry([]byte(topic), nil).WithID(id)
	if c != 0 {
		e.WithContract(c)
	}
	if err := d.db.DeleteEntry(e); err != nil {
		return Result{}, fmt.Errorf("uql: %w", err)
	}
	if err := d.db.Flush(); err != nil {
		return Result{}, fmt.Errorf("uql: %w", err)
	}
	return Result{Affected: 1}, nil
}
