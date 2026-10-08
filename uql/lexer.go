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
	"fmt"
	"strconv"
	"strings"
)

type tokenKind uint8

const (
	tEOF      tokenKind = iota
	tWord               // letters, digits, '_' and '-': keywords, topic parts, numbers, durations
	tString             // 'text' or "text"
	tParam              // $1
	tDot                // .
	tEllipsis           // ...
	tStar               // *
	tComma              // ,
	tLParen             // (
	tRParen             // )
	tOp                 // = != < <= > >= (level 1)
)

func (k tokenKind) String() string {
	return [...]string{"end of query", "word", "string", "parameter", "'.'", "'...'", "'*'", "','", "'('", "')'", "operator"}[k]
}

type token struct {
	kind  tokenKind
	text  string // the word, the unquoted string, or the operator
	param int    // for tParam
	pos   int    // byte offset in the query
}

// Error is a syntax or binding error, with the byte offset it was found at.
type Error struct {
	Pos int
	Msg string
}

func (e *Error) Error() string { return fmt.Sprintf("uql: %s (at offset %d)", e.Msg, e.Pos) }

func errorf(pos int, format string, a ...any) *Error {
	return &Error{Pos: pos, Msg: fmt.Sprintf(format, a...)}
}

func isWordByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-'
}

// lex splits a query into tokens.
func lex(src string) ([]token, error) {
	var toks []token
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < len(src) && src[i+1] == '-':
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case c == '.':
			if strings.HasPrefix(src[i:], "...") {
				toks = append(toks, token{kind: tEllipsis, text: "...", pos: i})
				i += 3
			} else {
				toks = append(toks, token{kind: tDot, text: ".", pos: i})
				i++
			}
		case c == '*':
			toks = append(toks, token{kind: tStar, text: "*", pos: i})
			i++
		case c == ',':
			toks = append(toks, token{kind: tComma, text: ",", pos: i})
			i++
		case c == '(':
			toks = append(toks, token{kind: tLParen, text: "(", pos: i})
			i++
		case c == ')':
			toks = append(toks, token{kind: tRParen, text: ")", pos: i})
			i++
		case c == '=' || c == '<' || c == '>' || c == '!':
			start := i
			i++
			if i < len(src) && src[i] == '=' {
				i++
			}
			op := src[start:i]
			if op == "!" {
				return nil, errorf(start, "unexpected '!'")
			}
			toks = append(toks, token{kind: tOp, text: op, pos: start})
		case c == '$':
			start := i
			i++
			for i < len(src) && src[i] >= '0' && src[i] <= '9' {
				i++
			}
			n, err := strconv.Atoi(src[start+1 : i])
			if err != nil || n < 1 {
				return nil, errorf(start, "parameters are $1, $2, ...")
			}
			toks = append(toks, token{kind: tParam, param: n, text: src[start:i], pos: start})
		case c == '\'' || c == '"':
			start := i
			i++
			var b strings.Builder
			for {
				if i >= len(src) {
					return nil, errorf(start, "unterminated string")
				}
				if src[i] == c {
					// A doubled quote is a quote.
					if i+1 < len(src) && src[i+1] == c {
						b.WriteByte(c)
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(src[i])
				i++
			}
			toks = append(toks, token{kind: tString, text: b.String(), pos: start})
		case isWordByte(c):
			start := i
			for i < len(src) && isWordByte(src[i]) {
				i++
			}
			toks = append(toks, token{kind: tWord, text: src[start:i], pos: start})
		default:
			return nil, errorf(i, "unexpected character %q", c)
		}
	}
	return append(toks, token{kind: tEOF, pos: len(src)}), nil
}
