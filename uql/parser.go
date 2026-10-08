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

// ErrNotAvailable is returned for syntax of a later UQL level.
var ErrNotAvailable = errors.New("uql: not available at level 0")

// Statement is a parsed statement: *Query, *Put, *Delete or *Explain.
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
	Multi bool // ends in "...": every part after
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

// Query is FROM topic [SINCE t] [UNTIL t] [LIMIT n] [IN CONTRACT c]. The
// topic has no wildcards; its entries include those put to wildcard topics
// that match it.
type Query struct {
	From     Pattern
	Since    *Time
	Until    *Time
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

// Delete is DELETE FROM topic ID $n [IN CONTRACT c]. The topic is the one
// the entry was put to: a wildcard topic for a broadcast entry.
type Delete struct {
	From     Pattern
	ID       int // parameter
	Contract *Int
}

// Explain is EXPLAIN query.
type Explain struct {
	Query *Query
}

func (*Query) stmt()   {}
func (*Put) stmt()     {}
func (*Delete) stmt()  {}
func (*Explain) stmt() {}

// later-level keywords, with the level that brings them.
var laterLevel = map[string]int{
	"SELECT": 1, "WHERE": 1, "LATEST": 1, "ORDER": 1, "GROUP": 1, "TOPICS": 1, "OFFSET": 1,
	"BEFORE": 1, "KEEP": 1, "CREATE": 2, "DROP": 2, "INDEX": 2,
}

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
	if kw := keyword(t); laterLevel[kw] > 0 {
		return fmt.Errorf("%w: %s comes with level %d (at offset %d)", ErrNotAvailable, kw, laterLevel[kw], t.pos)
	}
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
	case "FROM":
		return p.query()
	case "EXPLAIN":
		p.next()
		if keyword(p.peek()) != "FROM" {
			return nil, p.unexpected(p.peek(), "FROM")
		}
		q, err := p.query()
		if err != nil {
			return nil, err
		}
		return &Explain{Query: q}, nil
	case "PUT":
		return p.put()
	case "DELETE":
		return p.delete()
	}
	return nil, p.unexpected(t, "FROM, PUT, DELETE or EXPLAIN")
}

func (p *parser) query() (*Query, error) {
	if err := p.expect("FROM"); err != nil {
		return nil, err
	}
	q := &Query{}
	var err error
	if q.From, err = p.pattern(); err != nil {
		return nil, err
	}
	if !q.From.Static() {
		// unitdb's wildcards are for writing: an entry put to teams.*.ch1 is
		// read from every matching topic. Reading many topics at once needs
		// an engine change.
		return nil, fmt.Errorf("%w: reading many topics with a wildcard comes with level 1; FROM takes one topic, and its entries include those put to matching wildcard topics (at offset %d)", ErrNotAvailable, q.From.pos)
	}
	for {
		t := p.peek()
		switch keyword(t) {
		case "SINCE":
			if q.Since != nil {
				return nil, errorf(t.pos, "SINCE is given twice")
			}
			p.next()
			if q.Since, err = p.time(); err != nil {
				return nil, err
			}
		case "UNTIL":
			if q.Until != nil {
				return nil, errorf(t.pos, "UNTIL is given twice")
			}
			p.next()
			if q.Until, err = p.time(); err != nil {
				return nil, err
			}
		case "LIMIT":
			if q.Limit != nil {
				return nil, errorf(t.pos, "LIMIT is given twice")
			}
			p.next()
			if q.Limit, err = p.int("LIMIT"); err != nil {
				return nil, err
			}
		case "IN":
			if q.Contract != nil {
				return nil, errorf(t.pos, "IN CONTRACT is given twice")
			}
			if q.Contract, err = p.contract(); err != nil {
				return nil, err
			}
		default:
			if t.kind == tEOF {
				return q, nil
			}
			return nil, p.unexpected(t, "SINCE, UNTIL, LIMIT, IN CONTRACT or end of query")
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
	if err := p.expect("ID"); err != nil {
		return nil, err
	}
	t := p.next()
	if t.kind != tParam {
		return nil, errorf(t.pos, "ID takes a parameter, such as $1")
	}
	st.ID = t.param
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
// separates a key.
func checkPart(s string) error {
	switch {
	case s == "":
		return errors.New("a topic part can't be empty")
	case strings.ContainsAny(s, ".*?/"):
		return fmt.Errorf("a topic part can't contain '.', '*', '?' or '/': %q", s)
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
			strings.IndexFunc(x.Lit, func(r rune) bool { return r > 127 || !isWordByte(byte(r)) }) >= 0:
			parts = append(parts, "'"+strings.ReplaceAll(x.Lit, "'", "''")+"'")
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

func (q *Query) String() string {
	s := "FROM " + q.From.String()
	if q.Since != nil {
		s += " SINCE " + q.Since.String()
	}
	if q.Until != nil {
		s += " UNTIL " + q.Until.String()
	}
	if q.Limit != nil {
		s += " LIMIT " + q.Limit.String()
	}
	if q.Contract != nil {
		s += " IN CONTRACT " + q.Contract.String()
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
	s := "DELETE FROM " + st.From.String() + " ID $" + strconv.Itoa(st.ID)
	if st.Contract != nil {
		s += " IN CONTRACT " + st.Contract.String()
	}
	return s
}

func (e *Explain) String() string { return "EXPLAIN " + e.Query.String() }
