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
	"fmt"
	"hash/fnv"
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

// Indexes are kept in topics of their own, next to the data:
//
//	$uql.idx                           index definitions, newest first
//	$uql.state                         whether a UQL user is open (dirty)
//	$uql.ix.<name>.<gen>.<valuehash>   an index entry per entry indexed
//	$uql.rx.<name>.<gen>.<bucket>      a range index's entries, in 64 buckets
//
// An index entry names an entry (its topic's hash and id) and its indexed
// values. Index entries are never trusted: each is checked against the
// topic it names before it is used, so an entry deleted, expired or (for a
// LATEST index) replaced since is skipped, and its index entry removed.
// A range index is also kept in memory, in value order, loaded from its
// buckets by New.
//
// The write hook keeps indexes as entries are written. If a process wrote
// without UQL open and didn't Close it, New finds $uql.state dirty and
// rebuilds every index, in a new generation.
const (
	indexDefTopic = "$uql.idx"
	stateTopic    = "$uql.state"
	rangeBuckets  = 64
)

type indexDef struct {
	Name     string   `json:"name"`
	On       string   `json:"on"`
	Paths    []string `json:"paths"`
	Latest   bool     `json:"latest,omitempty"`
	Range    bool     `json:"range,omitempty"`
	Contract uint32   `json:"contract,omitempty"`
	Gen      int      `json:"gen"`
	Dropped  bool     `json:"dropped,omitempty"`

	paths []*Path
	rx    *rangeSet // a range index's entries, in value order
}

// values returns the indexed fields of a payload.
func (def *indexDef) values(payload []byte) []any {
	en := &env{entry: &Entry{Payload: payload}}
	out := make([]any, len(def.paths))
	for i, p := range def.paths {
		out[i] = en.path(p)
	}
	return out
}

func (def *indexDef) matches(topic string, contract uint32) bool {
	return resolved(def.Contract) == resolved(contract) && matchTopic(def.On, topic)
}

func (def *indexDef) prefix() string {
	kind := "ix"
	if def.Range {
		kind = "rx"
	}
	return "$uql." + kind + "." + def.Name + "." + strconv.Itoa(def.Gen)
}

// resolved returns the contract as the engine stores it.
func resolved(c uint32) uint32 {
	if c == 0 {
		return message.MasterContract
	}
	return c
}

func valueHash(vals []any) string {
	b, _ := json.Marshal(vals)
	h := fnv.New64a()
	h.Write(b)
	return strconv.FormatUint(h.Sum64(), 16)
}

// indexRecord is an index entry's payload.
type indexRecord struct {
	T string `json:"t"` // topic hash, hex
	I string `json:"i"` // entry id, hex
	V []any  `json:"v"`
}

func parsePath(s string) (*Path, error) {
	toks, err := lex(s)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	e, err := p.path(p.next())
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tEOF {
		return nil, fmt.Errorf("uql: bad index field %q", s)
	}
	return e.(*Path), nil
}

type indexes struct {
	d    *DB
	mu   sync.RWMutex
	defs map[string]*indexDef
	gens map[string]int // the newest generation of each name, dropped too
}

func newIndexes(d *DB) *indexes {
	return &indexes{d: d, defs: map[string]*indexDef{}, gens: map[string]int{}}
}

func (ix *indexes) db() *unitdb.DB { return ix.d.db }

func (ix *indexes) open() error {
	items, err := ix.db().Get(unitdb.NewQuery([]byte(indexDefTopic)).WithLimit(math.MaxInt32))
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, b := range items { // newest first
		var def indexDef
		if json.Unmarshal(b, &def) != nil {
			continue
		}
		ix.gens[def.Name] = max(ix.gens[def.Name], def.Gen)
		if seen[def.Name] {
			continue
		}
		seen[def.Name] = true
		if def.Dropped {
			continue
		}
		for _, s := range def.Paths {
			p, err := parsePath(s)
			if err != nil {
				return err
			}
			def.paths = append(def.paths, p)
		}
		ix.defs[def.Name] = &def
	}
	dirty, err := ix.dirty()
	if err != nil {
		return err
	}
	if dirty && len(ix.defs) > 0 {
		if err := ix.rebuildAll(context.Background()); err != nil {
			return err
		}
	} else {
		for _, def := range ix.defs {
			if def.Range {
				if err := ix.loadRange(def); err != nil {
					return err
				}
			}
		}
	}
	return ix.setDirty(true)
}

func (ix *indexes) dirty() (bool, error) {
	items, err := ix.db().Get(unitdb.NewQuery([]byte(stateTopic)).WithLimit(1))
	if err != nil || len(items) == 0 {
		return false, err
	}
	var st struct {
		Dirty bool `json:"dirty"`
	}
	_ = json.Unmarshal(items[0], &st)
	return st.Dirty, nil
}

func (ix *indexes) setDirty(dirty bool) error {
	b, _ := json.Marshal(map[string]bool{"dirty": dirty})
	if err := ix.db().Put([]byte(stateTopic), b); err != nil {
		return err
	}
	return ix.db().Flush()
}

func (ix *indexes) saveDef(def *indexDef) error {
	b, _ := json.Marshal(def)
	if err := ix.db().Put([]byte(indexDefTopic), b); err != nil {
		return err
	}
	return ix.db().Flush()
}

func (ix *indexes) create(ctx context.Context, st *CreateIndex, args []any) (Result, error) {
	on, err := ix.d.topic(st.On, args)
	if err != nil {
		return Result{}, err
	}
	if st.On.Static() {
		return Result{}, errorf(st.On.pos, "index a pattern such as teams.*.ch1: one topic is read directly")
	}
	c, err := ix.d.contract(st.Contract, args)
	if err != nil {
		return Result{}, err
	}
	def := &indexDef{Name: st.Name, On: on, Latest: st.Latest, Range: st.Range, Contract: c, paths: st.Paths}
	for _, p := range st.Paths {
		def.Paths = append(def.Paths, p.String())
	}
	ix.mu.Lock()
	if _, ok := ix.defs[def.Name]; ok {
		ix.mu.Unlock()
		return Result{}, fmt.Errorf("uql: index %s exists", def.Name)
	}
	ix.gens[def.Name]++
	def.Gen = ix.gens[def.Name]
	if def.Range {
		def.rx = newRangeSet(def.Latest)
	}
	// Registered before it is built, so writes meanwhile are indexed too.
	ix.defs[def.Name] = def
	ix.mu.Unlock()
	n, err := ix.build(ctx, def)
	if err == nil {
		err = ix.saveDef(def)
	}
	if err != nil {
		ix.mu.Lock()
		delete(ix.defs, def.Name)
		ix.mu.Unlock()
		ix.purge(def)
		return Result{}, err
	}
	return Result{Affected: n}, nil
}

func (ix *indexes) drop(name string) (Result, error) {
	ix.mu.Lock()
	def, ok := ix.defs[name]
	if ok {
		delete(ix.defs, name)
	}
	ix.mu.Unlock()
	if !ok {
		return Result{}, fmt.Errorf("uql: no index %s", name)
	}
	gone := *def
	gone.Dropped = true
	if err := ix.saveDef(&gone); err != nil {
		return Result{}, err
	}
	ix.purge(def)
	return Result{Affected: 1}, ix.db().Flush()
}

// purge deletes a generation's index entries, if the database is mutable.
func (ix *indexes) purge(def *indexDef) {
	hashes, err := ix.db().MatchTopics([]byte(def.prefix()+"..."), 0)
	if err != nil {
		return
	}
	for _, h := range hashes {
		for {
			items, err := ix.db().ReadTopic(h, unitdb.ReadOptions{Limit: math.MaxInt32})
			if err != nil || len(items) == 0 {
				break
			}
			for _, it := range items {
				if ix.db().DeleteTopicEntry(h, it.ID) != nil {
					return
				}
			}
		}
	}
}

// build indexes the entries already written, returning how many.
func (ix *indexes) build(ctx context.Context, def *indexDef) (int, error) {
	hashes, err := ix.d.matchTopics(def.On, def.Contract)
	if err != nil {
		return 0, err
	}
	opts := unitdb.ReadOptions{Limit: math.MaxInt32}
	if def.Latest {
		opts.Limit = 1
	}
	n := 0
	for _, h := range hashes {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		items, err := ix.db().ReadTopic(h, opts)
		if err != nil {
			return n, err
		}
		for _, it := range items {
			if err := ix.add(def, h, it.ID, it.Payload); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, ix.db().Flush()
}

func (ix *indexes) rebuildAll(ctx context.Context) error {
	ix.mu.Lock()
	defs := make([]*indexDef, 0, len(ix.defs))
	for _, def := range ix.defs {
		defs = append(defs, def)
	}
	ix.mu.Unlock()
	for _, old := range defs {
		def := *old
		ix.mu.Lock()
		ix.gens[def.Name]++
		def.Gen = ix.gens[def.Name]
		if def.Range {
			def.rx = newRangeSet(def.Latest)
		}
		ix.defs[def.Name] = &def
		ix.mu.Unlock()
		if _, err := ix.build(ctx, &def); err != nil {
			return fmt.Errorf("rebuilding index %s: %w", def.Name, err)
		}
		if err := ix.saveDef(&def); err != nil {
			return err
		}
		ix.purge(old)
	}
	return nil
}

// Reindex rebuilds every index from the entries it covers: for when a
// process wrote to the database without UQL watching.
func (d *DB) Reindex(ctx context.Context) error {
	return d.ix.rebuildAll(ctx)
}

// add writes an index entry for an entry.
func (ix *indexes) add(def *indexDef, topicHash uint64, id, payload []byte) error {
	vals := def.values(payload)
	rec, _ := json.Marshal(indexRecord{T: strconv.FormatUint(topicHash, 16), I: hex.EncodeToString(id), V: vals})
	topic := def.prefix() + "."
	if def.Range {
		topic += strconv.FormatUint(topicHash%rangeBuckets, 10)
		def.rx.insert(rangeItem{v: vals[0], topic: topicHash, id: id})
	} else {
		topic += valueHash(vals)
	}
	return ix.db().Put([]byte(topic), rec)
}

func (ix *indexes) loadRange(def *indexDef) error {
	def.rx = newRangeSet(def.Latest)
	for b := 0; b < rangeBuckets; b++ {
		topic := def.prefix() + "." + strconv.Itoa(b)
		items, err := ix.db().Get(unitdb.NewQuery([]byte(topic)).WithLimit(math.MaxInt32))
		if err != nil {
			return err
		}
		for _, raw := range items {
			it, ok := parseRecord(raw)
			if ok && len(it.vals) == 1 {
				def.rx.insert(rangeItem{v: it.vals[0], topic: it.topic, id: it.id})
			}
		}
	}
	return nil
}

type candidate struct {
	topic uint64
	id    []byte
	vals  []any
}

func parseRecord(raw []byte) (candidate, bool) {
	var rec indexRecord
	if json.Unmarshal(raw, &rec) != nil {
		return candidate{}, false
	}
	h, err := strconv.ParseUint(rec.T, 16, 64)
	if err != nil {
		return candidate{}, false
	}
	id, err := hex.DecodeString(rec.I)
	if err != nil || len(id) != 16 {
		return candidate{}, false
	}
	for i := range rec.V {
		rec.V[i] = normalise(rec.V[i])
	}
	return candidate{topic: h, id: id, vals: rec.V}, true
}

func (ix *indexes) onWrite(ev unitdb.WriteEvent) error {
	ix.mu.RLock()
	if len(ix.defs) == 0 {
		ix.mu.RUnlock()
		return nil
	}
	defs := make([]*indexDef, 0, len(ix.defs))
	for _, def := range ix.defs {
		defs = append(defs, def)
	}
	ix.mu.RUnlock()
	// Entries put to a wildcard topic are read through the topics it
	// matches, not by FROM a pattern: they aren't indexed.
	if ev.Wildcard() {
		return nil
	}
	topic := string(ev.Topic)
	if ev.Op == unitdb.OpDelete && topic == "" {
		topic = ix.d.topicName(ev.TopicHash)
	}
	for _, def := range defs {
		if !def.matches(topic, ev.Contract) {
			continue
		}
		switch {
		case ev.Op == unitdb.OpPut:
			if err := ix.add(def, ev.TopicHash, ev.ID, ev.Payload); err != nil {
				return err
			}
		case def.Latest:
			// The topic's latest entry may now be an older one.
			items, err := ix.db().ReadTopic(ev.TopicHash, unitdb.ReadOptions{Limit: 1})
			if err != nil {
				return err
			}
			for _, it := range items {
				if err := ix.add(def, ev.TopicHash, it.ID, it.Payload); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Planning.

// indexUse is how a query reads through an index.
type indexUse struct {
	def     *indexDef
	eq      []any // a hash index's values
	lo, hi  *bound
	ordered bool // a range index gives ORDER BY's order
	desc    bool
}

type bound struct {
	v    any
	incl bool
}

func (u *indexUse) describe() string {
	kind := "index"
	if u.def.Latest {
		kind = "LATEST index"
	}
	if !u.def.Range {
		parts := make([]string, len(u.eq))
		for i, v := range u.eq {
			parts[i] = u.def.Paths[i] + " = " + (&Lit{V: v}).String()
		}
		return fmt.Sprintf("Look up %s %s for %s, and check each entry it names.", kind, u.def.Name, strings.Join(parts, " AND "))
	}
	var r []string
	if u.lo != nil {
		op := ">"
		if u.lo.incl {
			op = ">="
		}
		r = append(r, u.def.Paths[0]+" "+op+" "+(&Lit{V: u.lo.v}).String())
	}
	if u.hi != nil {
		op := "<"
		if u.hi.incl {
			op = "<="
		}
		r = append(r, u.def.Paths[0]+" "+op+" "+(&Lit{V: u.hi.v}).String())
	}
	what := "every value"
	if len(r) > 0 {
		what = strings.Join(r, " AND ")
	}
	order := ""
	if u.ordered {
		order = ", in ORDER BY order, stopping once LIMIT rows match"
		if u.desc {
			order = ", in descending ORDER BY order, stopping once LIMIT rows match"
		}
	}
	return fmt.Sprintf("Scan range %s %s for %s%s, and check each entry it names.", kind, u.def.Name, what, order)
}

// conjuncts splits a condition at its top-level ANDs.
func conjuncts(e Expr, out []Expr) []Expr {
	if b, ok := e.(*Binary); ok && b.Op == "AND" {
		return conjuncts(b.R, conjuncts(b.L, out))
	}
	if e != nil {
		out = append(out, e)
	}
	return out
}

var flipped = map[string]string{"=": "=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}

// constant returns a literal's or parameter's value, for an index lookup.
func constant(e Expr, args []any) (any, bool) {
	switch x := e.(type) {
	case *Lit:
		return x.V, x.V != nil
	case *ParamExpr:
		if x.N > len(args) {
			return nil, false
		}
		v := argValue(args[x.N-1])
		switch v.(type) {
		case string, float64, bool:
			return v, true
		}
	}
	return nil, false
}

// comparisons returns the conjuncts of WHERE that compare a field with a
// constant, as field -> (op, value).
type comparison struct {
	op string
	v  any
}

func comparisons(where Expr, args []any) map[string][]comparison {
	out := map[string][]comparison{}
	for _, c := range conjuncts(where, nil) {
		b, ok := c.(*Binary)
		if !ok || flipped[b.Op] == "" || b.Not {
			continue
		}
		if p, ok := b.L.(*Path); ok {
			if v, ok := constant(b.R, args); ok {
				out[p.String()] = append(out[p.String()], comparison{b.Op, v})
			}
		} else if p, ok := b.R.(*Path); ok {
			if v, ok := constant(b.L, args); ok {
				out[p.String()] = append(out[p.String()], comparison{flipped[b.Op], v})
			}
		}
	}
	return out
}

// choose picks an index for a query over a pattern: one on the same
// pattern and contract, whose fields WHERE compares with constants or (a
// range index) that ORDER BY sorts by. A LATEST index serves only LATEST 1
// PER TOPIC, and another index only queries without LATEST, as LATEST
// applies before WHERE.
func (ix *indexes) choose(p *plan) *indexUse {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if len(ix.defs) == 0 {
		return nil
	}
	cmp := comparisons(p.q.Where, p.args)
	names := make([]string, 0, len(ix.defs))
	for n := range ix.defs {
		names = append(names, n)
	}
	sort.Strings(names)
	var best *indexUse
	for _, n := range names {
		def := ix.defs[n]
		if def.On != p.topic || resolved(def.Contract) != resolved(p.contract) {
			continue
		}
		if def.Latest != (p.latest == 1 && !p.hasUntil) || (!def.Latest && p.latest > 0) {
			continue
		}
		if !def.Range {
			eq := make([]any, len(def.Paths))
			full := true
			for i, path := range def.Paths {
				found := false
				for _, c := range cmp[path] {
					if c.op == "=" {
						eq[i], found = c.v, true
						break
					}
				}
				full = full && found
			}
			if full {
				return &indexUse{def: def, eq: eq} // an exact lookup: the best there is
			}
			continue
		}
		u := &indexUse{def: def}
		for _, c := range cmp[def.Paths[0]] {
			switch c.op {
			case "=":
				u.lo, u.hi = &bound{c.v, true}, &bound{c.v, true}
			case ">", ">=":
				if u.lo == nil {
					u.lo = &bound{c.v, c.op == ">="}
				}
			case "<", "<=":
				if u.hi == nil {
					u.hi = &bound{c.v, c.op == "<="}
				}
			}
		}
		if len(p.q.OrderBy) == 1 && !p.grouped {
			key := p.q.OrderBy[0].Expr
			if c := selectColumn(p.q, key); c >= 0 && p.q.Select[c].Alias != "" {
				key = p.q.Select[c].Expr
			}
			if key.String() == def.Paths[0] {
				u.ordered, u.desc = true, p.q.OrderBy[0].Desc
			}
		}
		if (u.lo != nil || u.hi != nil || u.ordered) && best == nil {
			best = u
		}
	}
	return best
}

// Reading through an index.

func (ix *indexes) read(ctx context.Context, p *plan) ([]Entry, error) {
	if p.use.def.Range {
		return ix.readRange(ctx, p)
	}
	u := p.use
	maxScan := ix.d.maxScan()
	topic := u.def.prefix() + "." + valueHash(u.eq)
	items, err := ix.db().GetEntries(unitdb.NewQuery([]byte(topic)).WithLimit(maxScan + 1))
	if err != nil {
		return nil, fmt.Errorf("uql: %w", err)
	}
	if len(items) > maxScan {
		return nil, fmt.Errorf("%w: index %s has more than %d entries for these values", ErrScanLimit, u.def.Name, maxScan)
	}
	v := newVerifier(ix.d, u.def.Latest)
	seen := map[uint64]bool{}
	var out []Entry
	for _, it := range items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c, ok := parseRecord(it.Payload)
		if !ok {
			continue
		}
		seq := message.ID(c.id).Sequence()
		if seen[seq] {
			continue
		}
		seen[seq] = true
		e, ok, err := v.get(c.topic, c.id)
		if err != nil {
			return nil, err
		}
		if !ok {
			// Deleted, expired or replaced: drop the index entry.
			_ = ix.db().DeleteTopicEntry(it.TopicHash, it.ID)
			continue
		}
		out = append(out, e)
	}
	sortNewest(out)
	return out, nil
}

func (ix *indexes) readRange(ctx context.Context, p *plan) ([]Entry, error) {
	u := p.use
	maxScan := ix.d.maxScan()
	v := newVerifier(ix.d, u.def.Latest)
	// With the index's order, stop once enough rows match.
	want := -1
	if u.ordered {
		want = p.limit + p.offset
	}
	var out []Entry
	var stale []rangeItem
	scanned := 0
	var err error
	u.def.rx.scan(u.lo, u.hi, u.desc, func(it rangeItem) bool {
		if err = ctx.Err(); err != nil {
			return false
		}
		if scanned++; scanned > maxScan {
			err = fmt.Errorf("%w: range index %s has more than %d entries in range", ErrScanLimit, u.def.Name, maxScan)
			return false
		}
		var e Entry
		var ok bool
		if e, ok, err = v.get(it.topic, it.id); err != nil {
			return false
		}
		if !ok {
			stale = append(stale, it)
			return true
		}
		if want >= 0 {
			if !p.window(e) {
				return true
			}
			if p.q.Where != nil {
				var match bool
				en := &env{entry: &e, args: p.args}
				if match, err = en.truth(p.q.Where); err != nil {
					return false
				}
				if !match {
					return true
				}
			}
		}
		out = append(out, e)
		return want < 0 || len(out) < want
	})
	for _, it := range stale {
		u.def.rx.remove(it)
	}
	if err != nil {
		return nil, err
	}
	if !u.ordered {
		sortNewest(out)
	}
	return out, nil
}

// verifier checks that index entries name live entries, reading each
// topic at most once or twice per query.
type verifier struct {
	d      *DB
	latest bool
	topics map[uint64]*topicRead
}

type topicRead struct {
	since int64 // unix seconds read from; -1 for the latest entry only
	items map[uint64]unitdb.Item
}

func newVerifier(d *DB, latest bool) *verifier {
	return &verifier{d: d, latest: latest, topics: map[uint64]*topicRead{}}
}

func (v *verifier) get(topic uint64, id []byte) (Entry, bool, error) {
	at := uid.Time(id[:4])
	tr := v.topics[topic]
	if tr == nil || (!v.latest && at < tr.since) {
		opts := unitdb.ReadOptions{Limit: 1}
		since := int64(-1)
		if !v.latest {
			since = at
			if tr != nil {
				since = min(at, tr.since)
			}
			opts = unitdb.ReadOptions{Since: time.Unix(since, 0), Limit: math.MaxInt32}
		}
		items, err := v.d.db.ReadTopic(topic, opts)
		if err != nil {
			return Entry{}, false, fmt.Errorf("uql: %w", err)
		}
		tr = &topicRead{since: since, items: make(map[uint64]unitdb.Item, len(items))}
		for _, it := range items {
			tr.items[message.ID(it.ID).Sequence()] = it
		}
		v.topics[topic] = tr
	}
	it, ok := tr.items[message.ID(id).Sequence()]
	if !ok {
		return Entry{}, false, nil
	}
	return v.d.entry(it), true, nil
}

// rangeSet is a range index in memory: entries in value order, then by
// sequence. A LATEST one keeps one entry per topic.
type rangeSet struct {
	mu     sync.RWMutex
	items  []rangeItem
	seqs   map[uint64]bool
	latest map[uint64]rangeItem // by topic, for a LATEST index
}

type rangeItem struct {
	v     any
	topic uint64
	id    []byte
}

func (it rangeItem) seq() uint64 { return message.ID(it.id).Sequence() }

func newRangeSet(latest bool) *rangeSet {
	s := &rangeSet{seqs: map[uint64]bool{}}
	if latest {
		s.latest = map[uint64]rangeItem{}
	}
	return s
}

func (s *rangeSet) less(a, b rangeItem) bool {
	if c := lessValues(a.v, b.v); c != 0 {
		return c < 0
	}
	return a.seq() < b.seq()
}

func (s *rangeSet) insert(it rangeItem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seqs[it.seq()] {
		return
	}
	if s.latest != nil {
		if old, ok := s.latest[it.topic]; ok {
			if old.seq() > it.seq() {
				return
			}
			s.removeLocked(old)
		}
		s.latest[it.topic] = it
	}
	s.seqs[it.seq()] = true
	i := sort.Search(len(s.items), func(i int) bool { return s.less(it, s.items[i]) })
	s.items = append(s.items, rangeItem{})
	copy(s.items[i+1:], s.items[i:])
	s.items[i] = it
}

func (s *rangeSet) remove(it rangeItem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeLocked(it)
}

func (s *rangeSet) removeLocked(it rangeItem) {
	if !s.seqs[it.seq()] {
		return
	}
	delete(s.seqs, it.seq())
	if s.latest != nil {
		if cur, ok := s.latest[it.topic]; ok && cur.seq() == it.seq() {
			delete(s.latest, it.topic)
		}
	}
	i := sort.Search(len(s.items), func(i int) bool { return !s.less(s.items[i], it) })
	if i < len(s.items) && s.items[i].seq() == it.seq() {
		s.items = append(s.items[:i], s.items[i+1:]...)
	}
}

// inside reports whether v is on the inner side of a bound, as WHERE
// compares: a value of another kind is not.
func inside(v any, b *bound, upper bool) bool {
	if b == nil {
		return true
	}
	c, ok := compare(v, b.v)
	if !ok {
		return false
	}
	if upper {
		return c < 0 || (c == 0 && b.incl)
	}
	return c > 0 || (c == 0 && b.incl)
}

// scan calls fn with the entries between lo and hi, in value order (desc
// for descending), until fn returns false. It works on a snapshot, so fn
// may change the set.
func (s *rangeSet) scan(lo, hi *bound, desc bool, fn func(rangeItem) bool) {
	s.mu.RLock()
	// lessValues orders every value, so the bounds are binary searches;
	// keep then drops values of another kind than the bounds'.
	start, end := 0, len(s.items)
	if lo != nil {
		start = sort.Search(len(s.items), func(i int) bool {
			c := lessValues(s.items[i].v, lo.v)
			return c > 0 || (c == 0 && lo.incl)
		})
	}
	if hi != nil {
		end = sort.Search(len(s.items), func(i int) bool {
			c := lessValues(s.items[i].v, hi.v)
			return c > 0 || (c == 0 && !hi.incl)
		})
	}
	var snap []rangeItem
	if start < end {
		snap = append(snap, s.items[start:end]...)
	}
	s.mu.RUnlock()
	if desc {
		for i := len(snap) - 1; i >= 0; i-- {
			if !keep(snap[i], lo, hi) {
				continue
			}
			if !fn(snap[i]) {
				return
			}
		}
		return
	}
	for _, it := range snap {
		if !keep(it, lo, hi) {
			continue
		}
		if !fn(it) {
			return
		}
	}
}

// keep drops values of another kind than the bounds'.
func keep(it rangeItem, lo, hi *bound) bool {
	return inside(it.v, lo, false) && inside(it.v, hi, true)
}
