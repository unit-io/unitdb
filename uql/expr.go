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
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Expr is an expression in SELECT, WHERE, GROUP BY and ORDER BY.
type Expr interface {
	String() string
}

// Path is a field: topic, id, time, payload, or a payload field such as
// data.workspaceId or tags[0].name.
type Path struct {
	Root  string // first name
	Steps []Step
	pos   int
}

// Step is ".name" or "[n]".
type Step struct {
	Name  string
	Index int
	IsIdx bool
}

// Lit is a literal value: string, float64, bool or nil.
type Lit struct{ V any }

// ParamExpr is $n.
type ParamExpr struct {
	N   int
	pos int
}

// Binary is a comparison or a logical operation.
type Binary struct {
	Op   string // = != < <= > >= AND OR LIKE
	L, R Expr
	Not  bool // NOT LIKE
}

// Not is NOT x.
type Not struct{ X Expr }

// In is x [NOT] IN (a, b, ...).
type In struct {
	X    Expr
	List []Expr
	Not  bool
}

// IsNull is x IS [NOT] NULL.
type IsNull struct {
	X   Expr
	Not bool
}

// Any is ANY(path, x -> cond): true if cond holds for some element of the
// array at path.
type Any struct {
	List Expr
	Var  string
	Cond Expr
}

// Call is a function: LOWER, UPPER, LENGTH, or an aggregate COUNT, SUM, MIN,
// MAX, AVG.
type Call struct {
	Name string
	Args []Expr
	Star bool // COUNT(*)
	pos  int
}

var aggregates = map[string]bool{"COUNT": true, "SUM": true, "MIN": true, "MAX": true, "AVG": true}
var scalars = map[string]int{"LOWER": 1, "UPPER": 1, "LENGTH": 1}

func isAggregate(e Expr) bool {
	found := false
	walkExpr(e, func(x Expr) {
		if c, ok := x.(*Call); ok && aggregates[c.Name] {
			found = true
		}
	})
	return found
}

func walkExpr(e Expr, fn func(Expr)) {
	if e == nil {
		return
	}
	fn(e)
	switch x := e.(type) {
	case *Binary:
		walkExpr(x.L, fn)
		walkExpr(x.R, fn)
	case *Not:
		walkExpr(x.X, fn)
	case *In:
		walkExpr(x.X, fn)
		for _, y := range x.List {
			walkExpr(y, fn)
		}
	case *IsNull:
		walkExpr(x.X, fn)
	case *Any:
		walkExpr(x.List, fn)
		walkExpr(x.Cond, fn)
	case *Call:
		for _, a := range x.Args {
			walkExpr(a, fn)
		}
	}
}

// usesPayload reports whether evaluating e needs the decoded payload.
func usesPayload(e Expr) bool {
	found := false
	walkExpr(e, func(x Expr) {
		if p, ok := x.(*Path); ok && !builtins[p.Root] {
			found = true
		}
	})
	return found
}

var builtins = map[string]bool{"topic": true, "id": true, "time": true}

// Parsing. Precedence, lowest first: OR, AND, NOT, comparison, primary.

func (p *parser) expr() (Expr, error) { return p.or() }

func (p *parser) or() (Expr, error) {
	l, err := p.and()
	if err != nil {
		return nil, err
	}
	for p.accept("OR") {
		r, err := p.and()
		if err != nil {
			return nil, err
		}
		l = &Binary{Op: "OR", L: l, R: r}
	}
	return l, nil
}

func (p *parser) and() (Expr, error) {
	l, err := p.not()
	if err != nil {
		return nil, err
	}
	for p.accept("AND") {
		r, err := p.not()
		if err != nil {
			return nil, err
		}
		l = &Binary{Op: "AND", L: l, R: r}
	}
	return l, nil
}

func (p *parser) not() (Expr, error) {
	if p.accept("NOT") {
		x, err := p.not()
		if err != nil {
			return nil, err
		}
		return &Not{X: x}, nil
	}
	return p.comparison()
}

func (p *parser) comparison() (Expr, error) {
	l, err := p.primary()
	if err != nil {
		return nil, err
	}
	t := p.peek()
	if t.kind == tOp {
		p.next()
		r, err := p.primary()
		if err != nil {
			return nil, err
		}
		return &Binary{Op: t.text, L: l, R: r}, nil
	}
	switch keyword(t) {
	case "IS":
		p.next()
		not := p.accept("NOT")
		if err := p.expect("NULL"); err != nil {
			return nil, err
		}
		return &IsNull{X: l, Not: not}, nil
	case "IN":
		p.next()
		list, err := p.list()
		if err != nil {
			return nil, err
		}
		return &In{X: l, List: list}, nil
	case "LIKE":
		p.next()
		r, err := p.primary()
		if err != nil {
			return nil, err
		}
		return &Binary{Op: "LIKE", L: l, R: r}, nil
	case "NOT":
		// x NOT IN (...) / x NOT LIKE y
		save := p.i
		p.next()
		switch keyword(p.peek()) {
		case "IN":
			p.next()
			list, err := p.list()
			if err != nil {
				return nil, err
			}
			return &In{X: l, List: list, Not: true}, nil
		case "LIKE":
			p.next()
			r, err := p.primary()
			if err != nil {
				return nil, err
			}
			return &Binary{Op: "LIKE", L: l, R: r, Not: true}, nil
		}
		p.i = save
	}
	return l, nil
}

func (p *parser) list() ([]Expr, error) {
	if t := p.next(); t.kind != tLParen {
		return nil, p.unexpected(t, "'('")
	}
	var list []Expr
	for {
		x, err := p.primary()
		if err != nil {
			return nil, err
		}
		list = append(list, x)
		t := p.next()
		if t.kind == tRParen {
			return list, nil
		}
		if t.kind != tComma {
			return nil, p.unexpected(t, "',' or ')'")
		}
	}
}

var numberRE = regexp.MustCompile(`^-?[0-9]+$`)

func (p *parser) primary() (Expr, error) {
	t := p.next()
	switch t.kind {
	case tLParen:
		x, err := p.expr()
		if err != nil {
			return nil, err
		}
		if c := p.next(); c.kind != tRParen {
			return nil, p.unexpected(c, "')'")
		}
		return x, nil
	case tString:
		return &Lit{V: t.text}, nil
	case tParam:
		return &ParamExpr{N: t.param, pos: t.pos}, nil
	case tWord:
		if numberRE.MatchString(t.text) {
			// 1.5 lexes as 1 . 5 with no space between.
			s := t.text
			if d := p.peek(); d.kind == tDot && d.pos == t.pos+len(t.text) {
				if f := p.toks[p.i+1]; f.kind == tWord && f.pos == d.pos+1 && numberRE.MatchString(f.text) && !strings.HasPrefix(f.text, "-") {
					p.i += 2
					s += "." + f.text
				}
			}
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, errorf(t.pos, "bad number %q", s)
			}
			return &Lit{V: v}, nil
		}
		switch strings.ToUpper(t.text) {
		case "TRUE":
			return &Lit{V: true}, nil
		case "FALSE":
			return &Lit{V: false}, nil
		case "NULL":
			return &Lit{V: nil}, nil
		case "TOPIC", "TIME":
			if p.peek().kind != tDot && p.peek().kind != tLParen {
				return &Path{Root: strings.ToLower(t.text), pos: t.pos}, nil
			}
		case "ANY":
			if p.peek().kind == tLParen {
				return p.anyExpr()
			}
		}
		if p.peek().kind == tLParen {
			return p.call(t)
		}
		return p.path(t)
	}
	return nil, p.unexpected(t, "a value, a field or '('")
}

func (p *parser) path(first token) (Expr, error) {
	path := &Path{Root: first.text, pos: first.pos}
	for {
		t := p.peek()
		switch {
		case t.kind == tDot:
			p.next()
			n := p.next()
			if n.kind != tWord && n.kind != tString {
				return nil, p.unexpected(n, "a field name")
			}
			path.Steps = append(path.Steps, Step{Name: n.text})
		case t.kind == tLBrack:
			p.next()
			n := p.next()
			idx, err := strconv.Atoi(n.text)
			if n.kind != tWord || err != nil || idx < 0 {
				return nil, errorf(n.pos, "array indexes are whole numbers")
			}
			if c := p.next(); c.kind != tRBrack {
				return nil, p.unexpected(c, "']'")
			}
			path.Steps = append(path.Steps, Step{Index: idx, IsIdx: true})
		default:
			return path, nil
		}
	}
}

func (p *parser) call(name token) (Expr, error) {
	fn := strings.ToUpper(name.text)
	p.next() // (
	c := &Call{Name: fn, pos: name.pos}
	if !aggregates[fn] && scalars[fn] == 0 {
		return nil, errorf(name.pos, "unknown function %s", name.text)
	}
	if fn == "COUNT" && p.peek().kind == tStar {
		p.next()
		c.Star = true
	} else if p.peek().kind != tRParen {
		for {
			a, err := p.expr()
			if err != nil {
				return nil, err
			}
			c.Args = append(c.Args, a)
			if p.peek().kind != tComma {
				break
			}
			p.next()
		}
	}
	if t := p.next(); t.kind != tRParen {
		return nil, p.unexpected(t, "')'")
	}
	want := scalars[fn]
	if aggregates[fn] {
		want = 1
	}
	if !c.Star && len(c.Args) != want {
		return nil, errorf(name.pos, "%s takes %d argument", fn, want)
	}
	return c, nil
}

func (p *parser) anyExpr() (Expr, error) {
	p.next() // (
	t := p.next()
	if t.kind != tWord {
		return nil, p.unexpected(t, "a field")
	}
	list, err := p.path(t)
	if err != nil {
		return nil, err
	}
	if c := p.next(); c.kind != tComma {
		return nil, p.unexpected(c, "','")
	}
	v := p.next()
	if v.kind != tWord {
		return nil, p.unexpected(v, "a name")
	}
	if quoteIdent(v.text) != v.text || builtins[strings.ToLower(v.text)] || strings.EqualFold(v.text, "payload") {
		return nil, errorf(v.pos, "%q can't name an element", v.text)
	}
	if a := p.next(); a.kind != tArrow {
		return nil, p.unexpected(a, "'->'")
	}
	cond, err := p.expr()
	if err != nil {
		return nil, err
	}
	if c := p.next(); c.kind != tRParen {
		return nil, p.unexpected(c, "')'")
	}
	return &Any{List: list, Var: v.text, Cond: cond}, nil
}

// String forms.

func (x *Path) String() string {
	var b strings.Builder
	if root := quoteIdent(x.Root); root != x.Root {
		// A quoted name would read as a string: payload.'x' is field x.
		b.WriteString("payload." + root)
	} else {
		b.WriteString(root)
	}
	for _, s := range x.Steps {
		if s.IsIdx {
			fmt.Fprintf(&b, "[%d]", s.Index)
		} else {
			b.WriteString("." + quoteIdent(s.Name))
		}
	}
	return b.String()
}

func quoteIdent(s string) string {
	if s != "" && !strings.HasPrefix(s, "--") && !numberRE.MatchString(s) &&
		strings.IndexFunc(s, func(r rune) bool { return r > 127 || !isWordByte(byte(r)) }) < 0 && !reserved[strings.ToUpper(s)] {
		return s
	}
	return quoteString(s)
}

func quoteString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// reserved words can't be field names without quotes.
var reserved = map[string]bool{"AND": true, "OR": true, "NOT": true, "IN": true, "IS": true, "NULL": true, "LIKE": true,
	"TRUE": true, "FALSE": true, "ANY": true, "FROM": true, "WHERE": true, "LIMIT": true, "OFFSET": true, "ORDER": true,
	"GROUP": true, "SINCE": true, "UNTIL": true, "LATEST": true, "AS": true, "ASC": true, "DESC": true, "BY": true,
	"COUNT": true, "SUM": true, "MIN": true, "MAX": true, "AVG": true, "LOWER": true, "UPPER": true, "LENGTH": true}

func (x *Lit) String() string {
	switch v := x.V.(type) {
	case nil:
		return "NULL"
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return quoteString(v)
	}
	return fmt.Sprint(x.V)
}

func (x *ParamExpr) String() string { return "$" + strconv.Itoa(x.N) }

func (x *Binary) String() string {
	op := x.Op
	if x.Not {
		op = "NOT " + op
	}
	return "(" + x.L.String() + " " + op + " " + x.R.String() + ")"
}

func (x *Not) String() string { return "(NOT " + x.X.String() + ")" }

func (x *In) String() string {
	parts := make([]string, len(x.List))
	for i, y := range x.List {
		parts[i] = y.String()
	}
	op := " IN "
	if x.Not {
		op = " NOT IN "
	}
	return "(" + x.X.String() + op + "(" + strings.Join(parts, ", ") + "))"
}

func (x *IsNull) String() string {
	if x.Not {
		return "(" + x.X.String() + " IS NOT NULL)"
	}
	return "(" + x.X.String() + " IS NULL)"
}

func (x *Any) String() string {
	return "ANY(" + x.List.String() + ", " + x.Var + " -> " + x.Cond.String() + ")"
}

func (x *Call) String() string {
	if x.Star {
		return x.Name + "(*)"
	}
	args := make([]string, len(x.Args))
	for i, a := range x.Args {
		args[i] = a.String()
	}
	return x.Name + "(" + strings.Join(args, ", ") + ")"
}

// Evaluation.

// env is what an expression is evaluated against: one entry.
type env struct {
	entry   *Entry
	payload any // decoded JSON, or nil
	decoded bool
	isJSON  bool
	args    []any
	vars    map[string]any // ANY's element variables
}

func (en *env) json() any {
	if !en.decoded {
		en.decoded = true
		var v any
		dec := json.NewDecoder(strings.NewReader(string(en.entry.Payload)))
		dec.UseNumber()
		if dec.Decode(&v) == nil {
			en.payload, en.isJSON = normalise(v), true
		}
	}
	return en.payload
}

// normalise turns json.Number into float64, recursively.
func normalise(v any) any {
	switch x := v.(type) {
	case json.Number:
		f, _ := x.Float64()
		return f
	case []any:
		for i := range x {
			x[i] = normalise(x[i])
		}
	case map[string]any:
		for k := range x {
			x[k] = normalise(x[k])
		}
	}
	return v
}

func step(v any, s Step) any {
	if s.IsIdx {
		if a, ok := v.([]any); ok && s.Index < len(a) {
			return a[s.Index]
		}
		return nil
	}
	if m, ok := v.(map[string]any); ok {
		return m[s.Name]
	}
	return nil
}

func (en *env) path(x *Path) any {
	var v any
	switch {
	case en.vars != nil && en.vars[x.Root] != nil:
		v = en.vars[x.Root]
	case x.Root == "topic" && len(x.Steps) == 0:
		return en.entry.Topic
	case x.Root == "id" && len(x.Steps) == 0:
		return en.entry.IDString()
	case x.Root == "time" && len(x.Steps) == 0:
		return en.entry.Time
	case x.Root == "payload":
		v = en.json()
		if !en.isJSON && len(x.Steps) == 0 {
			return string(en.entry.Payload) // not JSON: the text
		}
	default:
		v = step(en.json(), Step{Name: x.Root})
	}
	for _, s := range x.Steps {
		v = step(v, s)
	}
	return v
}

// argValue converts a parameter to an expression value.
func argValue(v any) any {
	switch x := v.(type) {
	case int:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case uint32:
		return float64(x)
	case uint64:
		return float64(x)
	case float32:
		return float64(x)
	case []byte:
		return string(x)
	}
	return v
}

func (en *env) eval(e Expr) (any, error) {
	switch x := e.(type) {
	case *Lit:
		return x.V, nil
	case *ParamExpr:
		if x.N > len(en.args) {
			return nil, errorf(x.pos, "parameter $%d has no value (%d given)", x.N, len(en.args))
		}
		return argValue(en.args[x.N-1]), nil
	case *Path:
		return en.path(x), nil
	case *Not:
		v, err := en.eval(x.X)
		if err != nil {
			return nil, err
		}
		b, ok := v.(bool)
		return ok && !b, nil
	case *IsNull:
		v, err := en.eval(x.X)
		if err != nil {
			return nil, err
		}
		return (v == nil) != x.Not, nil
	case *In:
		v, err := en.eval(x.X)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return false, nil
		}
		for _, y := range x.List {
			w, err := en.eval(y)
			if err != nil {
				return nil, err
			}
			if c, ok := compare(v, w); ok && c == 0 {
				return !x.Not, nil
			}
		}
		return x.Not, nil
	case *Any:
		list, err := en.eval(x.List)
		if err != nil {
			return nil, err
		}
		arr, _ := list.([]any)
		outer := en.vars
		defer func() { en.vars = outer }()
		for _, el := range arr {
			vars := map[string]any{}
			for k, v := range outer {
				vars[k] = v
			}
			vars[x.Var] = el
			en.vars = vars
			ok, err := en.eval(x.Cond)
			if err != nil {
				return nil, err
			}
			if b, _ := ok.(bool); b {
				return true, nil
			}
		}
		return false, nil
	case *Call:
		if aggregates[x.Name] {
			return nil, errorf(x.pos, "%s is an aggregate: use it in SELECT", x.Name)
		}
		v, err := en.eval(x.Args[0])
		if err != nil {
			return nil, err
		}
		switch x.Name {
		case "LOWER":
			if s, ok := v.(string); ok {
				return strings.ToLower(s), nil
			}
		case "UPPER":
			if s, ok := v.(string); ok {
				return strings.ToUpper(s), nil
			}
		case "LENGTH":
			switch y := v.(type) {
			case string:
				return float64(len([]rune(y))), nil
			case []any:
				return float64(len(y)), nil
			case map[string]any:
				return float64(len(y)), nil
			}
		}
		return nil, nil
	case *Binary:
		switch x.Op {
		case "AND", "OR":
			l, err := en.eval(x.L)
			if err != nil {
				return nil, err
			}
			lb, _ := l.(bool)
			if x.Op == "AND" && !lb {
				return false, nil
			}
			if x.Op == "OR" && lb {
				return true, nil
			}
			r, err := en.eval(x.R)
			if err != nil {
				return nil, err
			}
			rb, _ := r.(bool)
			return rb, nil
		}
		l, err := en.eval(x.L)
		if err != nil {
			return nil, err
		}
		r, err := en.eval(x.R)
		if err != nil {
			return nil, err
		}
		if x.Op == "LIKE" {
			s, ok1 := l.(string)
			pat, ok2 := r.(string)
			if !ok1 || !ok2 {
				return false, nil
			}
			return like(s, pat) != x.Not, nil
		}
		c, ok := compare(l, r)
		if !ok {
			// NULL or mismatched types compare as unknown: false.
			return false, nil
		}
		switch x.Op {
		case "=":
			return c == 0, nil
		case "!=":
			return c != 0, nil
		case "<":
			return c < 0, nil
		case "<=":
			return c <= 0, nil
		case ">":
			return c > 0, nil
		case ">=":
			return c >= 0, nil
		}
	}
	return nil, fmt.Errorf("uql: can't evaluate %s", e)
}

// compare orders two values of the same kind. Times compare with times and
// RFC 3339 strings. ok is false for NULL or different kinds.
func compare(a, b any) (int, bool) {
	if a == nil || b == nil {
		return 0, false
	}
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		if !ok {
			return 0, false
		}
		switch {
		case x < y:
			return -1, true
		case x > y:
			return 1, true
		}
		return 0, true
	case string:
		if t, ok := b.(time.Time); ok {
			at, err := time.Parse(time.RFC3339Nano, x)
			if err != nil {
				return 0, false
			}
			return compare(at, t)
		}
		y, ok := b.(string)
		if !ok {
			return 0, false
		}
		return strings.Compare(x, y), true
	case bool:
		y, ok := b.(bool)
		if !ok {
			return 0, false
		}
		switch {
		case x == y:
			return 0, true
		case !x:
			return -1, true
		}
		return 1, true
	case time.Time:
		var y time.Time
		switch t := b.(type) {
		case time.Time:
			y = t
		case string:
			at, err := time.Parse(time.RFC3339Nano, t)
			if err != nil {
				return 0, false
			}
			y = at
		default:
			return 0, false
		}
		return x.Compare(y), true
	}
	return 0, false
}

// like matches SQL LIKE: % any run, _ any one character.
func like(s, pat string) bool {
	var b strings.Builder
	b.WriteString("^")
	for _, r := range pat {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	return err == nil && re.MatchString(s)
}

// truth evaluates a condition: only true is true.
func (en *env) truth(e Expr) (bool, error) {
	v, err := en.eval(e)
	if err != nil {
		return false, err
	}
	b, _ := v.(bool)
	return b, nil
}

// lessValues orders two values for ORDER BY; NULL sorts first.
func lessValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	if c, ok := compare(a, b); ok {
		return c
	}
	// Different kinds: order by kind name for a stable result.
	return strings.Compare(fmt.Sprintf("%T", a), fmt.Sprintf("%T", b))
}

// aggregate state for one aggregate call in one group.
type aggState struct {
	count int64
	sum   float64
	min   any
	max   any
	nums  int64
}

func (s *aggState) add(c *Call, en *env) error {
	if c.Star {
		s.count++
		return nil
	}
	v, err := en.eval(c.Args[0])
	if err != nil {
		return err
	}
	if v == nil {
		return nil
	}
	s.count++
	if f, ok := v.(float64); ok {
		s.sum += f
		s.nums++
	}
	if s.min == nil || lessValues(v, s.min) < 0 {
		s.min = v
	}
	if s.max == nil || lessValues(v, s.max) > 0 {
		s.max = v
	}
	return nil
}

func (s *aggState) result(c *Call) any {
	switch c.Name {
	case "COUNT":
		return float64(s.count)
	case "SUM":
		if s.nums == 0 {
			return nil
		}
		return s.sum
	case "AVG":
		if s.nums == 0 {
			return nil
		}
		return s.sum / float64(s.nums)
	case "MIN":
		return s.min
	case "MAX":
		return s.max
	}
	return nil
}
