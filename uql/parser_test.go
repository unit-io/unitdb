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
	"strings"
	"testing"
)

func TestParseCanonical(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"FROM teams.alpha.ch1", "FROM teams.alpha.ch1"},
		{"from teams.alpha.ch1 since 1h limit 100", "FROM teams.alpha.ch1 SINCE 1h0m0s LIMIT 100"},
		{"FROM teams.ch1 LIMIT 5 IN CONTRACT 42", "FROM teams.ch1 LIMIT 5 IN CONTRACT 42"},
		{"PUT teams.*.ch1 VALUE $1", "PUT teams.*.ch1 VALUE $1"},
		{"PUT teams.alpha... VALUE $1 TTL 1h", "PUT teams.alpha... VALUE $1 TTL 1h0m0s"},
		{"PUT ... VALUE $1", "PUT ... VALUE $1"},
		{"DELETE FROM teams.*.ch1 ID $1", "DELETE FROM teams.*.ch1 ID $1"},
		{"FROM ls.d.$1.$2 SINCE $3 UNTIL $4 LIMIT $5 IN CONTRACT $6", "FROM ls.d.$1.$2 SINCE $3 UNTIL $4 LIMIT $5 IN CONTRACT $6"},
		{"FROM a.b SINCE '2026-10-08T12:00:00Z' UNTIL 7d", "FROM a.b SINCE '2026-10-08T12:00:00Z' UNTIL 168h0m0s"},
		{"FROM 'with space'.b -- comment\n LIMIT 1", "FROM 'with space'.b LIMIT 1"},
		{"FROM a.limit LIMIT 2", "FROM a.limit LIMIT 2"},
		{"PUT ls.d.project.p1 VALUE $1", "PUT ls.d.project.p1 VALUE $1"},
		{"put a.b value $1 ttl 90s in contract $2", "PUT a.b VALUE $1 TTL 1m30s IN CONTRACT $2"},
		{"DELETE FROM a.b ID $1", "DELETE FROM a.b ID $1"},
		{"EXPLAIN FROM a.b LIMIT 3", "EXPLAIN FROM a.b LIMIT 3"},
	} {
		st, err := Parse(tc.in)
		if err != nil {
			t.Errorf("Parse(%q): %v", tc.in, err)
			continue
		}
		if got := st.String(); got != tc.want {
			t.Errorf("Parse(%q) = %q, want %q", tc.in, got, tc.want)
		}
		// The canonical form parses to itself.
		again, err := Parse(st.String())
		if err != nil || again.String() != st.String() {
			t.Errorf("round trip of %q: %v %v", st.String(), again, err)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", "expected FROM, PUT, DELETE or EXPLAIN"},
		{"FROM", "expected a topic part"},
		{"FROM a.", "expected a topic part"},
		{"FROM a..b", "expected a topic part"},
		{"PUT a... .b VALUE $1", "expected VALUE"},
		{"FROM a LIMIT", "expected a number"},
		{"FROM a LIMIT -1", "whole number"},
		{"FROM a LIMIT 1 LIMIT 2", "LIMIT is given twice"},
		{"FROM a SINCE yesterday", "expected a duration"},
		{"FROM a SINCE '2026-13-01'", "RFC 3339"},
		{"FROM 'a.b'", "can't contain '.'"},
		{"FROM 'a?ttl=1h'", "can't contain"},
		{"FROM ''", "can't be empty"},
		{"FROM 'oops", "unterminated string"},
		{"FROM a $0", "parameters are $1"},
		{"FROM a#b", "unexpected character"},
		{"PUT a VALUE 'inline'", "VALUE takes a parameter"},
		{"PUT a VALUE $1 TTL 0s", "expected a duration"},
		{"DELETE FROM a ID 12", "ID takes a parameter"},
		{"EXPLAIN PUT a VALUE $1", "expected FROM"},
		{"FROM a IN 5", "expected CONTRACT"},
	} {
		_, err := Parse(tc.in)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("Parse(%q) error = %v, want %q", tc.in, err, tc.want)
		}
	}
}

func TestLaterLevels(t *testing.T) {
	for _, in := range []string{
		"SELECT data.name FROM a",
		"FROM a WHERE data.x = 1",
		"FROM a.b LATEST PER TOPIC",
		"FROM a.*",
		"FROM teams...",
		"FROM ...",
		"FROM a ORDER BY time",
		"TOPICS a.*",
		"CREATE INDEX i ON a.* (data.x)",
	} {
		_, err := Parse(in)
		if !errors.Is(err, ErrNotAvailable) {
			t.Errorf("Parse(%q) = %v, want ErrNotAvailable", in, err)
		}
	}
}

func TestErrorPosition(t *testing.T) {
	_, err := Parse("FROM a LIMIT x")
	var e *Error
	if !errors.As(err, &e) || e.Pos != 13 {
		t.Fatalf("error = %#v, want offset 13", err)
	}
}

func FuzzParse(f *testing.F) {
	for _, s := range []string{"FROM a.b SINCE 1h LIMIT 10", "PUT a.* VALUE $1 TTL 1h", "DELETE FROM a... ID $1", "EXPLAIN FROM a", "FROM 'x''y'.z"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		st, err := Parse(s)
		if err != nil {
			return
		}
		// What parses prints to a form that parses to the same thing.
		again, err := Parse(st.String())
		if err != nil {
			t.Fatalf("canonical form %q of %q doesn't parse: %v", st.String(), s, err)
		}
		if again.String() != st.String() {
			t.Fatalf("canonical form changed: %q -> %q", st.String(), again.String())
		}
	})
}
