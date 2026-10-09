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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrNotAvailable is returned for syntax a later UQL level brings.
var ErrNotAvailable = errors.New("uql: not available yet")

// Statement is a parsed statement: *Query, *Topics, *Put, *Delete,
// *CreateIndex, *DropIndex or *Explain.
type Statement interface {
	stmt()
	String() string
}

// Part is one part of a pattern: a literal, a "*" wildcard, or a parameter.
type Part struct {
	Lit   string
	Wild  bool
	Param int // 1-based, or 0
}

// Pattern is a topic pattern, such as teams.*.ch1 or teams.alpha...
type Pattern struct {
	Parts []Part
	Multi bool // ends in "...": the rest of the parts, or none
	pos   int
}

// Static reports whether the pattern names one topic.
func (p Pattern) Static() bool {
	if p.Multi {
		return false
	}
	for _, x := range p.Parts {
		if x.Wild {
			return false
		}
	}
	return true
}

// Time is a point in time: a duration back from now, a timestamp, or a
// parameter.
type Time struct {
	Ago   time.Duration
	At    time.Time
	Param int
	pos   int
}

// Int is an integer or a parameter.
type Int struct {
	Value int64
	Param int
	pos   int
}

// SelectItem is one column of SELECT.
type SelectItem struct {
	Expr  Expr
	Alias string
}

// OrderItem is one key of ORDER BY.
type OrderItem struct {
	Expr Expr // a *Path "time" for ORDER BY TIME
	Desc bool
}

// Query is
//
//	[SELECT items] FROM pattern [LATEST n PER TOPIC] [SINCE t] [UNTIL t]
//	[WHERE cond] [GROUP BY exprs] [ORDER BY keys] [LIMIT n [OFFSET m]]
//	[IN CONTRACT c]
//
// FROM one topic reads it as unitdb does: with the entries put to wildcard
// topics that match it. FROM a pattern reads the entries put to each topic
// that matches it.
type Query struct {
	Select   []SelectItem // nil for every entry
	From     Pattern
	Latest   *Int
	Since    *Time
	Until    *Time
	Where    Expr
	GroupBy  []Expr
	OrderBy  []OrderItem
	Limit    *Int
	Offset   *Int
	Contract *Int
}

// Topics is TOPICS pattern [LIMIT n] [IN CONTRACT c].
type Topics struct {
	Pattern  Pattern
	Limit    *Int
	Contract *Int
}

// Put is PUT topic VALUE $n [TTL d] [IN CONTRACT c]. A wildcard topic
// (teams.*.ch1, teams.alpha...) writes one entry that every matching topic
// returns.
type Put struct {
	Topic    Pattern
	Value    int // parameter
	TTL      time.Duration
	Contract *Int
}

// Delete is DELETE FROM pattern (ID $n | BEFORE t | KEEP LATEST n)
// [IN CONTRACT c]. With ID, the topic is the one the entry was put to: a
// wildcard topic for a broadcast entry. BEFORE and KEEP LATEST delete the
// entries put to each matching topic.
type Delete struct {
	From       Pattern
	ID         int   // parameter, for DELETE ... ID
	Before     *Time // DELETE ... BEFORE t
	KeepLatest *Int  // DELETE ... KEEP LATEST n
	Contract   *Int
}

// CreateIndex is CREATE [RANGE] INDEX name ON pattern (paths) [LATEST]
// [IN CONTRACT c].
type CreateIndex struct {
	Name     string
	On       Pattern
	Paths    []*Path
	Latest   bool
	Range    bool // ordered by value: ranges and ORDER BY (level 3)
	Contract *Int
}

// DropIndex is DROP INDEX name.
type DropIndex struct {
	Name string
}

// Explain is EXPLAIN query.
type Explain struct {
	Query *Query
}

func (*Query) stmt()       {}
func (*Topics) stmt()      {}
func (*Put) stmt()         {}
func (*Delete) stmt()      {}
func (*CreateIndex) stmt() {}
func (*DropIndex) stmt()   {}
func (*Explain) stmt()     {}

type parser struct {
	toks []token
	i    int
}

// Parse parses one statement.
func Parse(src string) (Statement, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	st, err := p.statement()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != tEOF {
		return nil, p.unexpected(t, "end of query")
	}
	return st, nil
}

func (p *parser) peek() token { return p.toks[p.i] }
func (p *parser) next() token {
	t := p.toks[p.i]
	if t.kind != tEOF {
		p.i++
	}
	return t
}

func keyword(t token) string {
	if t.kind != tWord {
		return ""
	}
	return strings.ToUpper(t.text)
}

func (p *parser) unexpected(t token, want string) error {
	got := t.kind.String()
	if t.kind == tWord || t.kind == tString || t.kind == tOp {
		got = fmt.Sprintf("%q", t.text)
	}
	return errorf(t.pos, "expected %s, found %s", want, got)
}

// accept consumes the keyword kw if it is next.
func (p *parser) accept(kw string) bool {
	if keyword(p.peek()) == kw {
		p.i++
		return true
	}
	return false
}

func (p *parser) expect(kw string) error {
	if !p.accept(kw) {
		return p.unexpected(p.peek(), kw)
	}
	return nil
}

func (p *parser) statement() (Statement, error) {
	t := p.peek()
	switch keyword(t) {
	case "FROM", "SELECT":
		return p.query()
	case "EXPLAIN":
		p.next()
		if kw := keyword(p.peek()); kw != "FROM" && kw != "SELECT" {
			return nil, p.unexpected(p.peek(), "a query (FROM or SELECT)")
		}
		q, err := p.query()
		if err != nil {
			return nil, err
		}
		return &Explain{Query: q}, nil
	case "TOPICS":
		return p.topics()
	case "PUT":
		return p.put()
	case "DELETE":
		return p.delete()
	case "CREATE":
		return p.createIndex()
	case "DROP":
		p.next()
		if err := p.expect("INDEX"); err != nil {
			return nil, err
		}
		name, err := p.name()
		if err != nil {
			return nil, err
		}
		return &DropIndex{Name: name}, nil
	}
	return nil, p.unexpected(t, "SELECT, FROM, TOPICS, PUT, DELETE, CREATE INDEX, DROP INDEX or EXPLAIN")
}

func (p *parser) query() (*Query, error) {
	q := &Query{}
	var err error
	if p.accept("SELECT") {
		if q.Select, err = p.selectItems(); err != nil {
			return nil, err
		}
	}
	if err := p.expect("FROM"); err != nil {
		return nil, err
	}
	if q.From, err = p.pattern(); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	once := func(t token, what string) error {
		if seen[what] {
			return errorf(t.pos, "%s is given twice", what)
		}
		seen[what] = true
		return nil
	}
	for {
		t := p.peek()
		kw := keyword(t)
		switch kw {
		case "LATEST":
			if err := once(t, kw); err != nil {
				return nil, err
			}
			p.next()
			q.Latest = &Int{Value: 1, pos: t.pos}
			if keyword(p.peek()) != "PER" {
				if q.Latest, err = p.int("LATEST"); err != nil {
					return nil, err
				}
			}
			if err := p.expect("PER"); err != nil {
				return nil, err
			}
			if err := p.expect("TOPIC"); err != nil {
				return nil, err
			}
		case "SINCE":
			if err := once(t, kw); err != nil {
				return nil, err
			}
			p.next()
			if q.Since, err = p.time(); err != nil {
				return nil, err
			}
		case "UNTIL":
			if err := once(t, kw); err != nil {
				return nil, err
			}
			p.next()
			if q.Until, err = p.time(); err != nil {
				return nil, err
			}
		case "WHERE":
			if err := once(t, kw); err != nil {
				return nil, err
			}
			p.next()
			if q.Where, err = p.expr(); err != nil {
				return nil, err
			}
			if isAggregate(q.Where) {
				return nil, errorf(t.pos, "WHERE can't use aggregates")
			}
		case "GROUP":
			if err := once(t, "GROUP BY"); err != nil {
				return nil, err
			}
			p.next()
			if err := p.expect("BY"); err != nil {
				return nil, err
			}
			for {
				e, err := p.expr()
				if err != nil {
					return nil, err
				}
				q.GroupBy = append(q.GroupBy, e)
				if p.peek().kind != tComma {
					break
				}
				p.next()
			}
		case "ORDER":
			if err := once(t, "ORDER BY"); err != nil {
				return nil, err
			}
			p.next()
			if err := p.expect("BY"); err != nil {
				return nil, err
			}
			for {
				e, err := p.expr()
				if err != nil {
					return nil, err
				}
				item := OrderItem{Expr: e}
				if p.accept("DESC") {
					item.Desc = true
				} else {
					p.accept("ASC")
				}
				q.OrderBy = append(q.OrderBy, item)
				if p.peek().kind != tComma {
					break
				}
				p.next()
			}
		case "LIMIT":
			if err := once(t, kw); err != nil {
				return nil, err
			}
			p.next()
			if q.Limit, err = p.int("LIMIT"); err != nil {
				return nil, err
			}
			if p.accept("OFFSET") {
				if q.Offset, err = p.int("OFFSET"); err != nil {
					return nil, err
				}
			}
		case "IN":
			if err := once(t, "IN CONTRACT"); err != nil {
				return nil, err
			}
			if q.Contract, err = p.contract(); err != nil {
				return nil, err
			}
		default:
			if t.kind == tEOF {
				return q, q.check()
			}
			return nil, p.unexpected(t, "LATEST, SINCE, UNTIL, WHERE, GROUP BY, ORDER BY, LIMIT, IN CONTRACT or end of query")
		}
	}
}

// check rejects queries whose clauses don't fit together.
func (q *Query) check() error {
	agg := false
	for _, it := range q.Select {
		if isAggregate(it.Expr) {
			agg = true
		}
	}
	if len(q.GroupBy) > 0 && !agg {
		return errorf(q.From.pos, "GROUP BY needs SELECT with an aggregate, such as COUNT(*)")
	}
	for _, e := range q.GroupBy {
		if isAggregate(e) {
			return errorf(q.From.pos, "GROUP BY can't use aggregates")
		}
	}
	return nil
}

func (p *parser) selectItems() ([]SelectItem, error) {
	if p.peek().kind == tStar {
		p.next()
		return nil, nil // SELECT * is every entry, as FROM alone
	}
	var items []SelectItem
	for {
		e, err := p.expr()
		if err != nil {
			return nil, err
		}
		it := SelectItem{Expr: e}
		if p.accept("AS") {
			if it.Alias, err = p.name(); err != nil {
				return nil, err
			}
		}
		items = append(items, it)
		if p.peek().kind != tComma {
			return items, nil
		}
		p.next()
	}
}

func (p *parser) name() (string, error) {
	t := p.next()
	if t.kind != tWord && t.kind != tString {
		return "", p.unexpected(t, "a name")
	}
	if !isName(t.text) {
		return "", errorf(t.pos, "names are letters, digits, '_' and '-': %q", t.text)
	}
	return t.text, nil
}

func isName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isWordByte(s[i]) {
			return false
		}
	}
	return true
}

func (p *parser) topics() (*Topics, error) {
	p.next() // TOPICS
	st := &Topics{}
	var err error
	if st.Pattern, err = p.pattern(); err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		switch keyword(t) {
		case "LIMIT":
			if st.Limit != nil {
				return nil, errorf(t.pos, "LIMIT is given twice")
			}
			p.next()
			if st.Limit, err = p.int("LIMIT"); err != nil {
				return nil, err
			}
		case "IN":
			if st.Contract != nil {
				return nil, errorf(t.pos, "IN CONTRACT is given twice")
			}
			if st.Contract, err = p.contract(); err != nil {
				return nil, err
			}
		default:
			if t.kind == tEOF {
				return st, nil
			}
			return nil, p.unexpected(t, "LIMIT, IN CONTRACT or end of query")
		}
	}
}

func (p *parser) put() (*Put, error) {
	p.next() // PUT
	st := &Put{}
	var err error
	if st.Topic, err = p.pattern(); err != nil {
		return nil, err
	}
	if err := p.expect("VALUE"); err != nil {
		return nil, err
	}
	t := p.next()
	if t.kind != tParam {
		return nil, errorf(t.pos, "VALUE takes a parameter, such as $1; values are never written in the query")
	}
	st.Value = t.param
	for {
		t := p.peek()
		switch keyword(t) {
		case "TTL":
			if st.TTL != 0 {
				return nil, errorf(t.pos, "TTL is given twice")
			}
			p.next()
			d := p.next()
			if d.kind != tWord {
				return nil, p.unexpected(d, "a duration such as 1h")
			}
			if st.TTL, err = duration(d); err != nil {
				return nil, err
			}
		case "IN":
			if st.Contract != nil {
				return nil, errorf(t.pos, "IN CONTRACT is given twice")
			}
			if st.Contract, err = p.contract(); err != nil {
				return nil, err
			}
		default:
			if t.kind == tEOF {
				return st, nil
			}
			return nil, p.unexpected(t, "TTL, IN CONTRACT or end of query")
		}
	}
}

func (p *parser) delete() (*Delete, error) {
	p.next() // DELETE
	if err := p.expect("FROM"); err != nil {
		return nil, err
	}
	st := &Delete{}
	var err error
	if st.From, err = p.pattern(); err != nil {
		return nil, err
	}
	t := p.next()
	switch keyword(t) {
	case "ID":
		pt := p.next()
		if pt.kind != tParam {
			return nil, errorf(pt.pos, "ID takes a parameter, such as $1")
		}
		st.ID = pt.param
	case "BEFORE":
		if st.Before, err = p.time(); err != nil {
			return nil, err
		}
	case "KEEP":
		if err := p.expect("LATEST"); err != nil {
			return nil, err
		}
		if st.KeepLatest, err = p.int("KEEP LATEST"); err != nil {
			return nil, err
		}
	default:
		return nil, p.unexpected(t, "ID, BEFORE or KEEP LATEST")
	}
	if keyword(p.peek()) == "IN" {
		if st.Contract, err = p.contract(); err != nil {
			return nil, err
		}
	}
	return st, nil
}

func (p *parser) createIndex() (*CreateIndex, error) {
	p.next() // CREATE
	st := &CreateIndex{}
	st.Range = p.accept("RANGE")
	if err := p.expect("INDEX"); err != nil {
		return nil, err
	}
	var err error
	if st.Name, err = p.name(); err != nil {
		return nil, err
	}
	if err := p.expect("ON"); err != nil {
		return nil, err
	}
	if st.On, err = p.pattern(); err != nil {
		return nil, err
	}
	for _, x := range st.On.Parts {
		if x.Param > 0 {
			return nil, errorf(st.On.pos, "an index pattern can't have parameters")
		}
	}
	if t := p.next(); t.kind != tLParen {
		return nil, p.unexpected(t, "'(' and the indexed fields")
	}
	for {
		t := p.next()
		if t.kind != tWord {
			return nil, p.unexpected(t, "a field")
		}
		e, err := p.path(t)
		if err != nil {
			return nil, err
		}
		path := e.(*Path)
		if builtins[path.Root] && len(path.Steps) == 0 {
			return nil, errorf(t.pos, "indexes are on payload fields; %s is not one", path.Root)
		}
		st.Paths = append(st.Paths, path)
		c := p.next()
		if c.kind == tRParen {
			break
		}
		if c.kind != tComma {
			return nil, p.unexpected(c, "',' or ')'")
		}
	}
	if st.Range && len(st.Paths) != 1 {
		return nil, errorf(st.On.pos, "a range index is on one field")
	}
	st.Latest = p.accept("LATEST")
	if keyword(p.peek()) == "IN" {
		if st.Contract, err = p.contract(); err != nil {
			return nil, err
		}
	}
	return st, nil
}

func (p *parser) contract() (*Int, error) {
	p.next() // IN
	if err := p.expect("CONTRACT"); err != nil {
		return nil, err
	}
	return p.int("CONTRACT")
}

// pattern parses part { "." part } [ "..." ], or a bare "...".
func (p *parser) pattern() (Pattern, error) {
	pat := Pattern{pos: p.peek().pos}
	if p.peek().kind == tEllipsis {
		p.next()
		pat.Multi = true
		return pat, nil
	}
	for {
		t := p.next()
		switch t.kind {
		case tWord:
			pat.Parts = append(pat.Parts, Part{Lit: t.text})
		case tString:
			if err := checkPart(t.text); err != nil {
				return pat, errorf(t.pos, "%s", err)
			}
			pat.Parts = append(pat.Parts, Part{Lit: t.text})
		case tParam:
			pat.Parts = append(pat.Parts, Part{Param: t.param})
		case tStar:
			pat.Parts = append(pat.Parts, Part{Wild: true})
		default:
			return pat, p.unexpected(t, "a topic part")
		}
		switch p.peek().kind {
		case tDot:
			p.next()
		case tEllipsis:
			p.next()
			pat.Multi = true
			return pat, nil
		default:
			return pat, nil
		}
	}
}

// checkPart rejects text that would change a topic's shape in unitdb: a dot
// adds parts, '*' and "..." are wildcards, '?' starts topic options and '/'
// separates a key. A part can't start with '$': those topics are UQL's own.
func checkPart(s string) error {
	switch {
	case s == "":
		return errors.New("a topic part can't be empty")
	case strings.ContainsAny(s, ".*?/"):
		return fmt.Errorf("a topic part can't contain '.', '*', '?' or '/': %q", s)
	case strings.HasPrefix(s, "$"):
		return fmt.Errorf("topic parts starting with '$' are reserved: %q", s)
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return fmt.Errorf("a topic part can't contain control characters: %q", s)
		}
	}
	return nil
}

func (p *parser) time() (*Time, error) {
	t := p.next()
	switch t.kind {
	case tParam:
		return &Time{Param: t.param, pos: t.pos}, nil
	case tWord:
		d, err := duration(t)
		if err != nil {
			return nil, err
		}
		return &Time{Ago: d, pos: t.pos}, nil
	case tString:
		at, err := time.Parse(time.RFC3339, t.text)
		if err != nil {
			return nil, errorf(t.pos, "times are durations (1h) or RFC 3339 timestamps ('2026-10-08T12:00:00Z')")
		}
		return &Time{At: at, pos: t.pos}, nil
	}
	return nil, p.unexpected(t, "a duration, a timestamp or a parameter")
}

func (p *parser) int(what string) (*Int, error) {
	t := p.next()
	switch t.kind {
	case tParam:
		return &Int{Param: t.param, pos: t.pos}, nil
	case tWord:
		n, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil || n < 0 {
			return nil, errorf(t.pos, "%s takes a whole number, found %q", what, t.text)
		}
		return &Int{Value: n, pos: t.pos}, nil
	}
	return nil, p.unexpected(t, "a number or a parameter")
}

// duration parses Go durations ("90s", "1h30m") and days ("7d").
func duration(t token) (time.Duration, error) {
	s := t.text
	if strings.HasSuffix(s, "d") {
		n, err := strconv.ParseInt(strings.TrimSuffix(s, "d"), 10, 32)
		if err == nil && n > 0 {
			return time.Duration(n) * 24 * time.Hour, nil
		}
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, errorf(t.pos, "expected a duration such as 30s, 15m, 1h or 7d, found %q", s)
	}
	return d, nil
}

// String forms re-print statements canonically.

func (p Pattern) String() string {
	var parts []string
	for _, x := range p.Parts {
		switch {
		case x.Wild:
			parts = append(parts, "*")
		case x.Param > 0:
			parts = append(parts, "$"+strconv.Itoa(x.Param))
		case strings.HasPrefix(x.Lit, "--") || // would start a comment
			strings.Contains(x.Lit, "->") ||
			strings.IndexFunc(x.Lit, func(r rune) bool { return r > 127 || !isWordByte(byte(r)) }) >= 0:
			parts = append(parts, quoteString(x.Lit))
		default:
			parts = append(parts, x.Lit)
		}
	}
	s := strings.Join(parts, ".")
	if p.Multi {
		s += "..."
	}
	return s
}

func (t *Time) String() string {
	switch {
	case t.Param > 0:
		return "$" + strconv.Itoa(t.Param)
	case !t.At.IsZero():
		return "'" + t.At.Format(time.RFC3339) + "'"
	}
	return t.Ago.String()
}

func (n *Int) String() string {
	if n.Param > 0 {
		return "$" + strconv.Itoa(n.Param)
	}
	return strconv.FormatInt(n.Value, 10)
}

func nameString(s string) string {
	if isName(s) && !strings.HasPrefix(s, "--") && !strings.Contains(s, "->") && !reserved[strings.ToUpper(s)] {
		return s
	}
	return quoteString(s)
}

func (q *Query) String() string {
	var b strings.Builder
	if q.Select != nil {
		b.WriteString("SELECT ")
		for i, it := range q.Select {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(it.Expr.String())
			if it.Alias != "" {
				b.WriteString(" AS " + nameString(it.Alias))
			}
		}
		b.WriteString(" ")
	}
	b.WriteString("FROM " + q.From.String())
	if q.Latest != nil {
		b.WriteString(" LATEST " + q.Latest.String() + " PER TOPIC")
	}
	if q.Since != nil {
		b.WriteString(" SINCE " + q.Since.String())
	}
	if q.Until != nil {
		b.WriteString(" UNTIL " + q.Until.String())
	}
	if q.Where != nil {
		b.WriteString(" WHERE " + q.Where.String())
	}
	if len(q.GroupBy) > 0 {
		parts := make([]string, len(q.GroupBy))
		for i, e := range q.GroupBy {
			parts[i] = e.String()
		}
		b.WriteString(" GROUP BY " + strings.Join(parts, ", "))
	}
	if len(q.OrderBy) > 0 {
		parts := make([]string, len(q.OrderBy))
		for i, o := range q.OrderBy {
			parts[i] = o.Expr.String()
			if o.Desc {
				parts[i] += " DESC"
			}
		}
		b.WriteString(" ORDER BY " + strings.Join(parts, ", "))
	}
	if q.Limit != nil {
		b.WriteString(" LIMIT " + q.Limit.String())
		if q.Offset != nil {
			b.WriteString(" OFFSET " + q.Offset.String())
		}
	}
	if q.Contract != nil {
		b.WriteString(" IN CONTRACT " + q.Contract.String())
	}
	return b.String()
}

func (st *Topics) String() string {
	s := "TOPICS " + st.Pattern.String()
	if st.Limit != nil {
		s += " LIMIT " + st.Limit.String()
	}
	if st.Contract != nil {
		s += " IN CONTRACT " + st.Contract.String()
	}
	return s
}

func (st *Put) String() string {
	s := "PUT " + st.Topic.String() + " VALUE $" + strconv.Itoa(st.Value)
	if st.TTL != 0 {
		s += " TTL " + st.TTL.String()
	}
	if st.Contract != nil {
		s += " IN CONTRACT " + st.Contract.String()
	}
	return s
}

func (st *Delete) String() string {
	s := "DELETE FROM " + st.From.String()
	switch {
	case st.Before != nil:
		s += " BEFORE " + st.Before.String()
	case st.KeepLatest != nil:
		s += " KEEP LATEST " + st.KeepLatest.String()
	default:
		s += " ID $" + strconv.Itoa(st.ID)
	}
	if st.Contract != nil {
		s += " IN CONTRACT " + st.Contract.String()
	}
	return s
}

func (st *CreateIndex) String() string {
	paths := make([]string, len(st.Paths))
	for i, p := range st.Paths {
		paths[i] = p.String()
	}
	kind := "CREATE INDEX "
	if st.Range {
		kind = "CREATE RANGE INDEX "
	}
	s := kind + nameString(st.Name) + " ON " + st.On.String() + " (" + strings.Join(paths, ", ") + ")"
	if st.Latest {
		s += " LATEST"
	}
	if st.Contract != nil {
		s += " IN CONTRACT " + st.Contract.String()
	}
	return s
}

func (st *DropIndex) String() string { return "DROP INDEX " + nameString(st.Name) }

func (e *Explain) String() string { return "EXPLAIN " + e.Query.String() }
