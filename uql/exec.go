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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/unit-io/unitdb"
	"github.com/unit-io/unitdb/message"
	"github.com/unit-io/unitdb/uid"
)

// DefaultLimit is what a query without LIMIT returns at most.
const DefaultLimit = 1000

// DefaultMaxScan is how many entries a query may read before it fails with
// ErrScanLimit. It stays below the engine's default maximum query limit
// (100000), so a read the engine cut short is noticed.
const DefaultMaxScan = 50000

// ErrScanLimit is returned by a query that would read more entries than
// DB.MaxScan; an index, SINCE, LATEST or a narrower pattern helps.
var ErrScanLimit = errors.New("uql: query would scan too many entries")

// catalogTopic holds the names of topics: the engine keeps only hashes.
const catalogTopic = "$uql.topics"

// DB runs UQL on a unitdb database. It watches the database's writes
// (unitdb.DB.OnWrite) to name topics and keep indexes, so every process
// that writes to the database should open one, and Close it.
type DB struct {
	db  *unitdb.DB
	now func() time.Time
	// MaxScan bounds the entries a query reads; keep it below the engine's
	// maximum query limit.
	MaxScan int

	mu     sync.RWMutex
	names  map[uint64]string
	ix     *indexes
	remove func()

	errMu   sync.Mutex
	hookErr error // from the write hook, reported by the next Exec
}

// New opens UQL on db: it loads topic names and indexes, rebuilds indexes
// if the last UQL user didn't Close, and starts watching writes.
func New(db *unitdb.DB) (*DB, error) {
	d := &DB{db: db, now: time.Now, MaxScan: DefaultMaxScan, names: map[uint64]string{}}
	if err := d.loadCatalog(); err != nil {
		return nil, fmt.Errorf("uql: topic names: %w", err)
	}
	d.ix = newIndexes(d)
	if err := d.ix.open(); err != nil {
		return nil, fmt.Errorf("uql: indexes: %w", err)
	}
	d.remove = db.OnWrite(d.onWrite)
	return d, nil
}

// Close stops watching writes and records a clean shutdown, so the next
// New doesn't rebuild indexes. It doesn't close the unitdb database.
func (d *DB) Close() error {
	if d.remove == nil {
		return nil
	}
	d.remove()
	d.remove = nil
	if err := d.ix.setDirty(false); err != nil {
		return err
	}
	return d.db.Flush()
}

func (d *DB) loadCatalog() error {
	items, err := d.db.Get(unitdb.NewQuery([]byte(catalogTopic)).WithLimit(math.MaxInt32))
	if err != nil {
		return err
	}
	for _, b := range items {
		var rec struct {
			H string `json:"h"`
			T string `json:"t"`
		}
		if json.Unmarshal(b, &rec) != nil {
			continue
		}
		if h, err := strconv.ParseUint(rec.H, 16, 64); err == nil {
			d.names[h] = rec.T
		}
	}
	return nil
}

// Name records topic names, for topics written before UQL watched the
// database: their entries show the hash (#...) until then.
func (d *DB) Name(contract uint32, topics ...string) error {
	for _, t := range topics {
		h, err := d.db.TopicHash([]byte(t), contract)
		if err != nil {
			return err
		}
		if err := d.learn(h, t); err != nil {
			return err
		}
	}
	return d.db.Flush()
}

func (d *DB) learn(h uint64, topic string) error {
	d.mu.Lock()
	if _, ok := d.names[h]; ok {
		d.mu.Unlock()
		return nil
	}
	d.names[h] = topic
	d.mu.Unlock()
	b, _ := json.Marshal(map[string]string{"h": strconv.FormatUint(h, 16), "t": topic})
	return d.db.Put([]byte(catalogTopic), b)
}

// topicName returns a topic's name, or #hash when UQL never saw it written.
func (d *DB) topicName(h uint64) string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if n, ok := d.names[h]; ok {
		return n
	}
	return "#" + strconv.FormatUint(h, 16)
}

func (d *DB) onWrite(ev unitdb.WriteEvent) {
	if len(ev.Topic) > 0 && ev.Topic[0] == '$' {
		return // UQL's own topics
	}
	if ev.Op == unitdb.OpPut {
		if err := d.learn(ev.TopicHash, string(ev.Topic)); err != nil {
			d.setHookErr(err)
		}
	}
	if err := d.ix.onWrite(ev); err != nil {
		d.setHookErr(err)
	}
}

func (d *DB) setHookErr(err error) {
	d.errMu.Lock()
	defer d.errMu.Unlock()
	if d.hookErr == nil {
		d.hookErr = err
	}
}

func (d *DB) takeHookErr() error {
	d.errMu.Lock()
	defer d.errMu.Unlock()
	err := d.hookErr
	d.hookErr = nil
	if err != nil {
		return fmt.Errorf("uql: naming topics or keeping indexes: %w", err)
	}
	return nil
}

// matchTopics returns the topics matching a pattern, without UQL's own.
func (d *DB) matchTopics(pattern string, contract uint32) ([]uint64, error) {
	hashes, err := d.db.MatchTopics([]byte(pattern), contract)
	if err != nil || !(pattern == "..." || strings.HasPrefix(pattern, "*")) {
		return hashes, err
	}
	own, err := d.db.MatchTopics([]byte("$uql..."), contract)
	if err != nil || len(own) == 0 {
		return hashes, err
	}
	skip := make(map[uint64]bool, len(own))
	for _, h := range own {
		skip[h] = true
	}
	out := hashes[:0]
	for _, h := range hashes {
		if !skip[h] {
			out = append(out, h)
		}
	}
	return out, nil
}

// Entry is one entry a query returns.
type Entry struct {
	// Topic is the topic the entry was put to: the topic read, or for an
	// entry put to a matching wildcard topic, that wildcard topic. It is
	// #hash for a topic UQL never saw written (see DB.Name).
	Topic     string
	TopicHash uint64
	ID        []byte
	Time      time.Time // from the entry ID, to the second
	Payload   []byte
}

// IDString returns the entry ID as hex, as DELETE ... ID accepts it.
func (e Entry) IDString() string { return hex.EncodeToString(e.ID) }

func (e Entry) seq() uint64 { return message.ID(e.ID).Sequence() }

func (d *DB) entry(it unitdb.Item) Entry {
	return Entry{Topic: d.topicName(it.TopicHash), TopicHash: it.TopicHash, ID: it.ID,
		Time: time.Unix(uid.Time(it.ID[:4]), 0), Payload: it.Payload}
}

// Rows are a query's results.
type Rows struct {
	cols []string
	rows []row
	i    int
}

type row struct {
	entry    Entry
	hasEntry bool
	vals     []any
}

// Next moves to the next row.
func (r *Rows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

// Columns names the values of each row: topic, id, time and payload for a
// query without SELECT.
func (r *Rows) Columns() []string { return r.cols }

// Values returns the current row's values: string, float64, bool, nil,
// time.Time, or []any and map[string]any for JSON arrays and objects.
func (r *Rows) Values() []any { return r.rows[r.i-1].vals }

// Entry returns the entry of the current row; a grouped row has none.
func (r *Rows) Entry() Entry { return r.rows[r.i-1].entry }

// All returns the entries of the rows; grouped rows have none.
func (r *Rows) All() []Entry {
	out := make([]Entry, 0, len(r.rows))
	for _, x := range r.rows {
		if x.hasEntry {
			out = append(out, x.entry)
		}
	}
	return out
}

// Len returns how many rows the query returned.
func (r *Rows) Len() int { return len(r.rows) }

// Err returns an error that stopped the rows. Rows are read before Query
// returns, so it is always nil.
func (r *Rows) Err() error { return nil }

// Scan copies the current row's values into dest: *string, *float64,
// *int64, *int, *bool, *time.Time, *[]byte or *any.
func (r *Rows) Scan(dest ...any) error {
	vals := r.Values()
	if len(dest) != len(vals) {
		return fmt.Errorf("uql: Scan takes %d values, not %d", len(vals), len(dest))
	}
	for i, v := range vals {
		if err := scanValue(dest[i], v); err != nil {
			return fmt.Errorf("uql: column %s: %w", r.cols[i], err)
		}
	}
	return nil
}

func scanValue(dest, v any) error {
	switch p := dest.(type) {
	case *any:
		*p = v
		return nil
	case *string:
		switch x := v.(type) {
		case string:
			*p = x
		case nil:
			*p = ""
		case time.Time:
			*p = x.UTC().Format(time.RFC3339)
		default:
			b, _ := json.Marshal(x)
			*p = string(b)
		}
		return nil
	case *[]byte:
		switch x := v.(type) {
		case string:
			*p = []byte(x)
		case nil:
			*p = nil
		default:
			b, _ := json.Marshal(x)
			*p = b
		}
		return nil
	case *float64:
		if x, ok := v.(float64); ok {
			*p = x
			return nil
		}
	case *int64:
		if x, ok := v.(float64); ok {
			*p = int64(x)
			return nil
		}
	case *int:
		if x, ok := v.(float64); ok {
			*p = int(x)
			return nil
		}
	case *bool:
		if x, ok := v.(bool); ok {
			*p = x
			return nil
		}
	case *time.Time:
		switch x := v.(type) {
		case time.Time:
			*p = x
			return nil
		case string:
			if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
				*p = t
				return nil
			}
		}
	default:
		return fmt.Errorf("can't scan into %T", dest)
	}
	if v == nil {
		return errors.New("value is NULL")
	}
	return fmt.Errorf("can't scan %T into %T", v, dest)
}

// Result reports what a write did.
type Result struct {
	Affected int
}

// Plan says how a query runs.
type Plan struct {
	Statement string   // the query, re-printed canonically
	Topic     string   // the topic or pattern, parameters bound
	Contract  uint32   // 0 is the master contract
	Index     string   // the index used, if any
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

// Query runs SELECT, FROM or TOPICS.
func (d *DB) Query(ctx context.Context, src string, args ...any) (*Rows, error) {
	s, err := d.Prepare(src)
	if err != nil {
		return nil, err
	}
	return s.Query(ctx, args...)
}

// Exec runs PUT, DELETE, CREATE INDEX or DROP INDEX.
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

// Query runs the statement, which must be a query or TOPICS.
func (s *Stmt) Query(ctx context.Context, args ...any) (*Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch st := s.st.(type) {
	case *Query:
		p, err := s.db.bind(st, args)
		if err != nil {
			return nil, err
		}
		return s.db.run(ctx, p)
	case *Topics:
		return s.db.topics(st, args)
	case *Explain:
		return nil, errors.New("uql: use Explain for EXPLAIN")
	}
	return nil, errors.New("uql: Query runs SELECT, FROM and TOPICS; use Exec for writes")
}

// Exec runs the statement, which must be a write.
func (s *Stmt) Exec(ctx context.Context, args ...any) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	var res Result
	var err error
	switch st := s.st.(type) {
	case *Put:
		res, err = s.db.put(st, args)
	case *Delete:
		res, err = s.db.delete(ctx, st, args)
	case *CreateIndex:
		res, err = s.db.ix.create(ctx, st, args)
	case *DropIndex:
		res, err = s.db.ix.drop(st.Name)
	default:
		return Result{}, errors.New("uql: Exec runs PUT, DELETE, CREATE INDEX and DROP INDEX; use Query for queries")
	}
	if err != nil {
		return res, err
	}
	return res, s.db.takeHookErr()
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
	p, err := s.db.bind(q, args)
	if err != nil {
		return nil, err
	}
	return p.explain(s.db.maxScan()), nil
}

// Binding.

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

// plan is a query with its parameters bound.
type plan struct {
	q        *Query
	args     []any
	topic    string
	static   bool
	contract uint32
	now      time.Time
	since    time.Time
	until    time.Time
	hasSince bool
	hasUntil bool
	limit    int
	offset   int
	latest   int // 0 for none
	grouped  bool
	use      *indexUse // an index that serves the read, or nil
}

func (d *DB) bind(q *Query, args []any) (*plan, error) {
	p := &plan{q: q, args: args, static: q.From.Static(), limit: DefaultLimit, now: d.now()}
	var err error
	if p.topic, err = d.topic(q.From, args); err != nil {
		return nil, err
	}
	if p.contract, err = d.contract(q.Contract, args); err != nil {
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
		p.limit = int(min(n, math.MaxInt32))
	}
	if q.Offset != nil {
		n, err := d.intArg(q.Offset, args, "OFFSET")
		if err != nil {
			return nil, err
		}
		p.offset = int(min(n, math.MaxInt32))
	}
	if q.Latest != nil {
		n, err := d.intArg(q.Latest, args, "LATEST")
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, errorf(q.Latest.pos, "LATEST must be at least 1")
		}
		p.latest = int(min(n, math.MaxInt32))
	}
	if q.Since != nil {
		if p.since, err = d.instant(q.Since, args, p.now); err != nil {
			return nil, err
		}
		p.since, p.hasSince = p.since.Truncate(time.Second), true
	}
	if q.Until != nil {
		if p.until, err = d.instant(q.Until, args, p.now); err != nil {
			return nil, err
		}
		p.hasUntil = true
	}
	for _, it := range q.Select {
		if isAggregate(it.Expr) {
			p.grouped = true
		}
	}
	// A missing parameter fails before reading, not on the first entry.
	var missing error
	check := func(e Expr) {
		walkExpr(e, func(x Expr) {
			if pe, ok := x.(*ParamExpr); ok && pe.N > len(args) && missing == nil {
				missing = errorf(pe.pos, "parameter $%d has no value (%d given)", pe.N, len(args))
			}
		})
	}
	check(q.Where)
	for _, it := range q.Select {
		check(it.Expr)
	}
	if missing != nil {
		return nil, missing
	}
	if !p.static {
		p.use = d.ix.choose(p)
	}
	return p, nil
}

// pushdown reports whether the newest limit+offset entries of each topic
// are all the query needs: nothing filters, orders or groups them.
func (p *plan) pushdown() bool {
	return p.q.Where == nil && len(p.q.OrderBy) == 0 && !p.grouped && !p.hasUntil
}

// per returns how many entries of each topic to read.
func (p *plan) per(maxScan int) int {
	switch {
	case p.latest > 0 && !p.hasUntil:
		// LATEST applies before WHERE, ORDER BY and grouping.
		return p.latest
	case p.pushdown():
		return min(p.limit+p.offset, maxScan+1)
	}
	return maxScan + 1
}

// Reading.

func (d *DB) maxScan() int {
	if d.MaxScan <= 0 {
		return DefaultMaxScan
	}
	return d.MaxScan
}

func (d *DB) gather(ctx context.Context, p *plan) ([]Entry, error) {
	maxScan := d.maxScan()
	per := p.per(maxScan)
	var out []Entry
	if p.static {
		q := unitdb.NewQuery([]byte(p.topic)).WithLimit(per)
		if p.contract != 0 {
			q.WithContract(p.contract)
		}
		if p.hasSince {
			// WithLast takes a duration back from now, to the second.
			q.WithLast(strconv.FormatInt(int64(p.now.Sub(p.since)/time.Second)+1, 10) + "s")
		}
		items, err := d.db.GetEntries(q)
		if err != nil {
			return nil, fmt.Errorf("uql: %w", err)
		}
		if len(items) > maxScan {
			return nil, fmt.Errorf("%w: more than %d; add SINCE or LATEST", ErrScanLimit, maxScan)
		}
		for _, it := range items {
			out = append(out, d.entry(it))
		}
		return out, nil
	}
	hashes, err := d.matchTopics(p.topic, p.contract)
	if err != nil {
		return nil, fmt.Errorf("uql: %w", err)
	}
	opts := unitdb.ReadOptions{Limit: per}
	if p.hasSince {
		opts.Since = p.since
	}
	// Without filters, only the newest limit+offset entries are kept, however
	// many topics match.
	keep := 0
	if p.latest == 0 && p.pushdown() {
		keep = per
	}
	for _, h := range hashes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		items, err := d.db.ReadTopic(h, opts)
		if err != nil {
			return nil, fmt.Errorf("uql: %w", err)
		}
		for _, it := range items {
			out = append(out, d.entry(it))
		}
		if keep > 0 && len(out) > 2*keep {
			sortNewest(out)
			out = out[:keep]
		}
		if keep == 0 && len(out) > maxScan {
			return nil, fmt.Errorf("%w: more than %d in %d topics; add an index, SINCE, LATEST or a narrower pattern", ErrScanLimit, maxScan, len(hashes))
		}
	}
	sortNewest(out)
	return out, nil
}

// sortNewest orders entries newest first: by sequence, which the engine
// gives every entry in write order.
func sortNewest(es []Entry) {
	sort.SliceStable(es, func(i, j int) bool { return es[i].seq() > es[j].seq() })
}

func (d *DB) run(ctx context.Context, p *plan) (*Rows, error) {
	if p.use != nil {
		entries, err := d.ix.read(ctx, p)
		if err != nil {
			return nil, err
		}
		return d.process(p, entries, p.use.ordered)
	}
	entries, err := d.gather(ctx, p)
	if err != nil {
		return nil, err
	}
	return d.process(p, entries, false)
}

// window reports whether an entry is within SINCE and UNTIL.
func (p *plan) window(e Entry) bool {
	return !(p.hasSince && e.Time.Before(p.since)) && !(p.hasUntil && e.Time.After(p.until))
}

// process filters, groups, projects, orders and pages entries, newest
// first. ordered is set when they are already in ORDER BY order.
func (d *DB) process(p *plan, entries []Entry, ordered bool) (*Rows, error) {
	q := p.q
	kept := entries[:0]
	for _, e := range entries {
		if p.window(e) {
			kept = append(kept, e)
		}
	}
	entries = kept
	// LATEST n PER TOPIC: the newest n of each topic put to. FROM one topic
	// is one topic, whatever the entries were put to.
	if p.latest > 0 {
		count := map[uint64]int{}
		kept = entries[:0]
		for _, e := range entries {
			k := e.TopicHash
			if p.static {
				k = 0
			}
			if count[k] < p.latest {
				count[k]++
				kept = append(kept, e)
			}
		}
		entries = kept
	}
	envs := make([]*env, 0, len(entries))
	for i := range entries {
		en := &env{entry: &entries[i], args: p.args}
		if q.Where != nil {
			ok, err := en.truth(q.Where)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		envs = append(envs, en)
	}
	var rows *Rows
	var keys [][]any
	var err error
	if p.grouped {
		rows, keys, err = groupRows(q, envs)
	} else {
		rows, keys, err = entryRows(q, envs, ordered)
	}
	if err != nil {
		return nil, err
	}
	if len(q.OrderBy) > 0 && !ordered {
		idx := make([]int, len(rows.rows))
		for i := range idx {
			idx[i] = i
		}
		sort.SliceStable(idx, func(a, b int) bool {
			for k, o := range q.OrderBy {
				c := lessValues(keys[idx[a]][k], keys[idx[b]][k])
				if c == 0 {
					continue
				}
				if o.Desc {
					return c > 0
				}
				return c < 0
			}
			return false
		})
		sorted := make([]row, len(idx))
		for i, j := range idx {
			sorted[i] = rows.rows[j]
		}
		rows.rows = sorted
	}
	if p.offset >= len(rows.rows) {
		rows.rows = nil
	} else {
		rows.rows = rows.rows[p.offset:]
	}
	if len(rows.rows) > p.limit {
		rows.rows = rows.rows[:p.limit]
	}
	return rows, nil
}

func entryRows(q *Query, envs []*env, ordered bool) (*Rows, [][]any, error) {
	rows := &Rows{cols: columns(q)}
	var keys [][]any
	for _, en := range envs {
		r := row{entry: *en.entry, hasEntry: true}
		if q.Select == nil {
			r.vals = []any{en.entry.Topic, en.entry.IDString(), en.entry.Time, string(en.entry.Payload)}
		} else {
			for _, it := range q.Select {
				v, err := en.eval(it.Expr)
				if err != nil {
					return nil, nil, err
				}
				r.vals = append(r.vals, v)
			}
		}
		rows.rows = append(rows.rows, r)
		if len(q.OrderBy) == 0 || ordered {
			continue
		}
		k := make([]any, len(q.OrderBy))
		for i, o := range q.OrderBy {
			if c := selectColumn(q, o.Expr); c >= 0 {
				k[i] = r.vals[c]
				continue
			}
			v, err := en.eval(o.Expr)
			if err != nil {
				return nil, nil, err
			}
			k[i] = v
		}
		keys = append(keys, k)
	}
	return rows, keys, nil
}

func columns(q *Query) []string {
	if q.Select == nil {
		return []string{"topic", "id", "time", "payload"}
	}
	cols := make([]string, len(q.Select))
	for i, it := range q.Select {
		cols[i] = it.Alias
		if cols[i] == "" {
			cols[i] = it.Expr.String()
		}
	}
	return cols
}

// selectColumn returns the SELECT column an ORDER BY key names, by alias or
// by the same expression, or -1.
func selectColumn(q *Query, e Expr) int {
	s := e.String()
	for i, it := range q.Select {
		if p, ok := e.(*Path); ok && len(p.Steps) == 0 && it.Alias != "" && p.Root == it.Alias {
			return i
		}
		if it.Expr.String() == s {
			return i
		}
	}
	return -1
}

type group struct {
	first  *env
	states map[*Call]*aggState
}

func groupRows(q *Query, envs []*env) (*Rows, [][]any, error) {
	var calls []*Call
	collect := func(e Expr) {
		walkExpr(e, func(x Expr) {
			if c, ok := x.(*Call); ok && aggregates[c.Name] {
				calls = append(calls, c)
			}
		})
	}
	for _, it := range q.Select {
		collect(it.Expr)
	}
	for _, o := range q.OrderBy {
		collect(o.Expr)
	}
	newGroup := func(first *env) *group {
		g := &group{first: first, states: map[*Call]*aggState{}}
		for _, c := range calls {
			g.states[c] = &aggState{}
		}
		return g
	}
	var order []string
	groups := map[string]*group{}
	for _, en := range envs {
		key := ""
		if len(q.GroupBy) > 0 {
			vals := make([]any, len(q.GroupBy))
			for i, g := range q.GroupBy {
				v, err := en.eval(g)
				if err != nil {
					return nil, nil, err
				}
				vals[i] = v
			}
			b, _ := json.Marshal(vals)
			key = string(b)
		}
		g, ok := groups[key]
		if !ok {
			g = newGroup(en)
			groups[key] = g
			order = append(order, key)
		}
		for _, c := range calls {
			if err := g.states[c].add(c, en); err != nil {
				return nil, nil, err
			}
		}
	}
	// Aggregates over no entries are one row, as in SQL; with GROUP BY,
	// no rows.
	if len(envs) == 0 && len(q.GroupBy) == 0 {
		groups[""] = newGroup(&env{entry: &Entry{}})
		order = append(order, "")
	}
	rows := &Rows{cols: columns(q)}
	var keys [][]any
	for _, k := range order {
		g := groups[k]
		r := row{}
		for _, it := range q.Select {
			v, err := evalAgg(it.Expr, g)
			if err != nil {
				return nil, nil, err
			}
			r.vals = append(r.vals, v)
		}
		rows.rows = append(rows.rows, r)
		if len(q.OrderBy) == 0 {
			continue
		}
		kk := make([]any, len(q.OrderBy))
		for i, o := range q.OrderBy {
			if c := selectColumn(q, o.Expr); c >= 0 {
				kk[i] = r.vals[c]
				continue
			}
			v, err := evalAgg(o.Expr, g)
			if err != nil {
				return nil, nil, err
			}
			kk[i] = v
		}
		keys = append(keys, kk)
	}
	return rows, keys, nil
}

// evalAgg evaluates an expression for a group: aggregates from the group's
// state, other fields from its first entry.
func evalAgg(e Expr, g *group) (any, error) {
	if c, ok := e.(*Call); ok && aggregates[c.Name] {
		return g.states[c].result(c), nil
	}
	if !isAggregate(e) {
		return g.first.eval(e)
	}
	lit := func(x Expr) (Expr, error) {
		v, err := evalAgg(x, g)
		return &Lit{V: v}, err
	}
	switch x := e.(type) {
	case *Binary:
		l, err := lit(x.L)
		if err != nil {
			return nil, err
		}
		r, err := lit(x.R)
		if err != nil {
			return nil, err
		}
		return g.first.eval(&Binary{Op: x.Op, L: l, R: r, Not: x.Not})
	case *Not:
		v, err := lit(x.X)
		if err != nil {
			return nil, err
		}
		return g.first.eval(&Not{X: v})
	case *IsNull:
		v, err := lit(x.X)
		if err != nil {
			return nil, err
		}
		return g.first.eval(&IsNull{X: v, Not: x.Not})
	case *Call:
		args := make([]Expr, len(x.Args))
		for i, a := range x.Args {
			v, err := lit(a)
			if err != nil {
				return nil, err
			}
			args[i] = v
		}
		return g.first.eval(&Call{Name: x.Name, Args: args, pos: x.pos})
	}
	return nil, fmt.Errorf("uql: can't use aggregates inside %s", e)
}

func (p *plan) explain(maxScan int) *Plan {
	out := &Plan{Statement: p.q.String(), Topic: p.topic, Contract: p.contract}
	contract := "the master contract"
	if p.contract != 0 {
		contract = "contract " + strconv.FormatUint(uint64(p.contract), 10)
	}
	switch {
	case p.use != nil:
		out.Index = p.use.def.Name
		out.Steps = append(out.Steps, p.use.describe())
	case p.static:
		out.Steps = append(out.Steps, fmt.Sprintf("Read topic %q in %s, with the entries put to wildcard topics that match it.", p.topic, contract))
	default:
		out.Steps = append(out.Steps, fmt.Sprintf("Match topics %q in %s, in the topic trie.", p.topic, contract))
	}
	if p.use == nil {
		each := " of each topic"
		if p.static {
			each = ""
		}
		if per := p.per(maxScan); per > maxScan {
			out.Steps = append(out.Steps, fmt.Sprintf("Read every entry%s, newest first, failing past %d (MaxScan).", each, maxScan))
		} else {
			out.Steps = append(out.Steps, fmt.Sprintf("Read the newest %d entries%s.", per, each))
		}
	}
	if p.hasSince {
		out.Steps = append(out.Steps, "Keep entries since "+p.since.UTC().Format(time.RFC3339)+" (the engine skips older time blocks).")
	}
	if p.hasUntil {
		out.Steps = append(out.Steps, "Drop entries after "+p.until.UTC().Format(time.RFC3339)+" (filtered after reading).")
	}
	if p.latest > 0 {
		out.Steps = append(out.Steps, fmt.Sprintf("Keep the newest %d of each topic.", p.latest))
	}
	if p.q.Where != nil {
		out.Steps = append(out.Steps, "Filter by "+p.q.Where.String()+".")
	}
	if p.grouped {
		out.Steps = append(out.Steps, "Group and aggregate.")
	}
	if len(p.q.OrderBy) > 0 {
		if p.use != nil && p.use.ordered {
			out.Steps = append(out.Steps, "Keep the range index's order (no sort).")
		} else {
			out.Steps = append(out.Steps, "Sort.")
		}
	}
	out.Steps = append(out.Steps, fmt.Sprintf("Return up to %d rows, skipping %d.", p.limit, p.offset))
	return out
}

// TOPICS.

func (d *DB) topics(st *Topics, args []any) (*Rows, error) {
	pattern, err := d.topic(st.Pattern, args)
	if err != nil {
		return nil, err
	}
	contract, err := d.contract(st.Contract, args)
	if err != nil {
		return nil, err
	}
	limit := DefaultLimit
	if st.Limit != nil {
		n, err := d.intArg(st.Limit, args, "LIMIT")
		if err != nil {
			return nil, err
		}
		limit = int(min(n, math.MaxInt32))
	}
	hashes, err := d.matchTopics(pattern, contract)
	if err != nil {
		return nil, fmt.Errorf("uql: %w", err)
	}
	rows := &Rows{cols: []string{"topic", "hash"}}
	for _, h := range hashes {
		rows.rows = append(rows.rows, row{vals: []any{d.topicName(h), strconv.FormatUint(h, 16)}})
	}
	sort.Slice(rows.rows, func(i, j int) bool { return rows.rows[i].vals[0].(string) < rows.rows[j].vals[0].(string) })
	if len(rows.rows) > limit {
		rows.rows = rows.rows[:limit]
	}
	return rows, nil
}

// Writes.

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
	// A statement is durable when it returns, with the index entries it made.
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

func (d *DB) delete(ctx context.Context, st *Delete, args []any) (Result, error) {
	topic, err := d.topic(st.From, args)
	if err != nil {
		return Result{}, err
	}
	c, err := d.contract(st.Contract, args)
	if err != nil {
		return Result{}, err
	}
	if st.Before == nil && st.KeepLatest == nil {
		v, err := arg(args, st.ID, st.From.pos)
		if err != nil {
			return Result{}, err
		}
		id, err := idArg(v, st.ID, st.From.pos)
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
	var hashes []uint64
	if st.From.Static() {
		h, err := d.db.TopicHash([]byte(topic), c)
		if err != nil {
			return Result{}, fmt.Errorf("uql: %w", err)
		}
		hashes = []uint64{h}
	} else if hashes, err = d.matchTopics(topic, c); err != nil {
		return Result{}, fmt.Errorf("uql: %w", err)
	}
	var before time.Time
	keep := 0
	if st.Before != nil {
		if before, err = d.instant(st.Before, args, d.now()); err != nil {
			return Result{}, err
		}
	} else {
		n, err := d.intArg(st.KeepLatest, args, "KEEP LATEST")
		if err != nil {
			return Result{}, err
		}
		keep = int(min(n, math.MaxInt32))
	}
	res := Result{}
	for _, h := range hashes {
		// A read returns at most the engine's query limit: read again until
		// nothing is left to delete.
		for {
			if err := ctx.Err(); err != nil {
				return res, err
			}
			items, err := d.db.ReadTopic(h, unitdb.ReadOptions{Limit: math.MaxInt32})
			if err != nil {
				return res, fmt.Errorf("uql: %w", err)
			}
			deleted := 0
			for i, it := range items {
				if st.Before != nil && !time.Unix(uid.Time(it.ID[:4]), 0).Before(before) {
					continue
				}
				if st.KeepLatest != nil && i < keep {
					continue
				}
				if err := d.db.DeleteTopicEntry(h, it.ID); err != nil {
					return res, fmt.Errorf("uql: %w", err)
				}
				deleted++
			}
			res.Affected += deleted
			if deleted == 0 {
				break
			}
		}
	}
	if err := d.db.Flush(); err != nil {
		return res, fmt.Errorf("uql: %w", err)
	}
	return res, nil
}

// matchTopic reports whether a topic name matches a pattern: "*" is one
// part, a trailing "..." any parts after, including none.
func matchTopic(pattern, topic string) bool {
	if pattern == "..." {
		return true
	}
	rest := strings.HasSuffix(pattern, "...")
	pattern = strings.TrimSuffix(strings.TrimSuffix(pattern, "..."), ".")
	pp := strings.Split(pattern, ".")
	tp := strings.Split(topic, ".")
	if len(tp) < len(pp) || (!rest && len(tp) != len(pp)) {
		return false
	}
	for i, x := range pp {
		if x != "*" && x != tp[i] {
			return false
		}
	}
	return true
}
