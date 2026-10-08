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
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/unit-io/unitdb"
)

func openDB(t *testing.T) *DB {
	t.Helper()
	db, err := unitdb.Open(filepath.Join(t.TempDir(), "uql"), unitdb.WithDefaultOptions(), unitdb.WithMutable())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return New(db)
}

func payloads(r *Rows) []string {
	var out []string
	for r.Next() {
		out = append(out, string(r.Entry().Payload))
	}
	return out
}

func TestPutQueryDelete(t *testing.T) {
	ctx := context.Background()
	d := openDB(t)
	for i := 1; i <= 3; i++ {
		if _, err := d.Exec(ctx, "PUT teams.alpha.ch1 VALUE $1", fmt.Sprintf("m%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	d.Exec(ctx, "PUT teams.beta.ch1 VALUE $1", []byte("b1"))

	r, err := d.Query(ctx, "FROM teams.alpha.ch1")
	if err != nil {
		t.Fatal(err)
	}
	if got := payloads(r); strings.Join(got, ",") != "m3,m2,m1" {
		t.Fatalf("newest first = %v", got)
	}
	r, _ = d.Query(ctx, "FROM teams.alpha.ch1 LIMIT 2")
	if r.Len() != 2 || r.All()[0].Topic != "teams.alpha.ch1" || r.All()[0].Time.IsZero() {
		t.Fatalf("limit = %+v", r.All())
	}
	// A wildcard write is read from every matching topic.
	if _, err := d.Exec(ctx, "PUT teams.*.ch1 VALUE $1", "to-every-ch1"); err != nil {
		t.Fatal(err)
	}
	r, _ = d.Query(ctx, "FROM teams.beta.ch1")
	if got := payloads(r); strings.Join(got, ",") != "to-every-ch1,b1" {
		t.Fatalf("beta with broadcast = %v", got)
	}
	r, _ = d.Query(ctx, "FROM teams.gamma.ch1")
	if got := payloads(r); strings.Join(got, ",") != "to-every-ch1" {
		t.Fatalf("gamma = %v", got)
	}
	// It is deleted through the wildcard topic it was put to.
	bid := r.All()[0].IDString()
	if _, err := d.Exec(ctx, "DELETE FROM teams.*.ch1 ID $1", bid); err != nil {
		t.Fatal(err)
	}
	if r, _ = d.Query(ctx, "FROM teams.gamma.ch1"); r.Len() != 0 {
		t.Fatalf("broadcast still read after delete: %v", payloads(r))
	}

	// Delete the newest by the ID the query returned, as hex.
	r, _ = d.Query(ctx, "FROM teams.alpha.ch1 LIMIT 1")
	if _, err := d.Exec(ctx, "DELETE FROM teams.alpha.ch1 ID $1", r.All()[0].IDString()); err != nil {
		t.Fatal(err)
	}
	r, _ = d.Query(ctx, "FROM teams.alpha.ch1")
	if got := payloads(r); strings.Join(got, ",") != "m2,m1" {
		t.Fatalf("after delete = %v", got)
	}
}

func TestParametersAndInjection(t *testing.T) {
	ctx := context.Background()
	d := openDB(t)
	stmt, err := d.Prepare("PUT ls.d.$1.$2 VALUE $3")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"p1", "p2"} {
		if _, err := stmt.Exec(ctx, "project", id, `{"id":"`+id+`"}`); err != nil {
			t.Fatal(err)
		}
	}
	r, err := d.Query(ctx, "FROM ls.d.$1.$2", "project", "p2")
	if err != nil || r.Len() != 1 || r.All()[0].Topic != "ls.d.project.p2" {
		t.Fatalf("param topic = %+v %v", r, err)
	}
	// A parameter is one part: it can't widen the topic.
	for _, bad := range []string{"p1.x", "*", "...", "a?last=1h", "a/b", ""} {
		if _, err := d.Query(ctx, "FROM ls.d.project.$1", bad); err == nil {
			t.Errorf("parameter %q accepted as a topic part", bad)
		}
	}
	for _, tc := range []struct {
		q    string
		args []any
		want string
	}{
		{"FROM a.$1", nil, "has no value"},
		{"FROM a.$1", []any{42}, "must be a string"},
		{"FROM a LIMIT $1", []any{"ten"}, "must be an integer"},
		{"FROM a LIMIT $1", []any{0}, "at least 1"},
		{"FROM a SINCE $1", []any{3}, "time.Time"},
		{"FROM a IN CONTRACT $1", []any{int64(1) << 40}, "32-bit"},
		{"PUT a VALUE $1", []any{42}, "[]byte or string"},
		{"PUT a VALUE $1", []any{""}, "empty"},
		{"DELETE FROM a ID $1", []any{"zz"}, "isn't hex"},
		{"DELETE FROM a ID $1", []any{[]byte{1, 2}}, "16 bytes"},
	} {
		var err error
		if strings.HasPrefix(tc.q, "FROM") {
			_, err = d.Query(ctx, tc.q, tc.args...)
		} else {
			_, err = d.Exec(ctx, tc.q, tc.args...)
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s %v: error = %v, want %q", tc.q, tc.args, err, tc.want)
		}
	}
	if _, err := d.Query(ctx, "PUT a VALUE $1", "x"); err == nil {
		t.Error("Query ran a PUT")
	}
	if _, err := d.Exec(ctx, "FROM a"); err == nil {
		t.Error("Exec ran a query")
	}
}

func TestTimeWindows(t *testing.T) {
	ctx := context.Background()
	d := openDB(t)
	d.Exec(ctx, "PUT a.b VALUE $1", "first")
	written := time.Now()
	// Pretend an hour has passed, so the entry is an hour old.
	d.now = func() time.Time { return written.Add(time.Hour) }
	if r, _ := d.Query(ctx, "FROM a.b SINCE 30m"); r.Len() != 0 {
		t.Fatalf("SINCE 30m found %d entries an hour old", r.Len())
	}
	if r, _ := d.Query(ctx, "FROM a.b SINCE 2h"); r.Len() != 1 {
		t.Fatalf("SINCE 2h found %d", r.Len())
	}
	if r, _ := d.Query(ctx, "FROM a.b SINCE $1", written.Add(-time.Minute)); r.Len() != 1 {
		t.Fatalf("SINCE timestamp found %d", r.Len())
	}
	if r, _ := d.Query(ctx, "FROM a.b UNTIL 2h"); r.Len() != 0 {
		t.Fatalf("UNTIL 2h ago found %d", r.Len())
	}
	if r, _ := d.Query(ctx, "FROM a.b UNTIL 30m LIMIT 1"); r.Len() != 1 {
		t.Fatalf("UNTIL 30m ago found %d", r.Len())
	}
}

func TestContracts(t *testing.T) {
	ctx := context.Background()
	d := openDB(t)
	d.Exec(ctx, "PUT shared.topic VALUE $1 IN CONTRACT 7", "seven")
	d.Exec(ctx, "PUT shared.topic VALUE $1", "master")
	r, _ := d.Query(ctx, "FROM shared.topic IN CONTRACT $1", uint32(7))
	if got := payloads(r); strings.Join(got, ",") != "seven" {
		t.Fatalf("contract 7 = %v", got)
	}
	r, _ = d.Query(ctx, "FROM shared.topic")
	if got := payloads(r); strings.Join(got, ",") != "master" {
		t.Fatalf("master contract = %v", got)
	}
}

func TestExplain(t *testing.T) {
	d := openDB(t)
	p, err := d.Explain("EXPLAIN FROM teams.alpha SINCE 1h UNTIL 10m LIMIT 5 IN CONTRACT 9")
	if err != nil {
		t.Fatal(err)
	}
	s := p.String()
	for _, want := range []string{"Read topic \"teams.alpha\"", "wildcard topics that match it", "contract 9", "older than 1h0m0s", "filtered after reading", "Stop at 5"} {
		if !strings.Contains(s, want) {
			t.Errorf("plan lacks %q:\n%s", want, s)
		}
	}
	if p.Contract != 9 || p.Limit != scanAll {
		t.Errorf("plan = %+v", p)
	}
	p, _ = d.Explain("FROM a.b LIMIT $1", 3)
	if p.Limit != 3 || !strings.Contains(p.String(), "master contract") {
		t.Errorf("plan = %s", p)
	}
	if _, err := d.Explain("PUT a VALUE $1", "x"); err == nil {
		t.Error("PUT has a plan")
	}
}

func TestContextCancelled(t *testing.T) {
	d := openDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Query(ctx, "FROM a"); err == nil {
		t.Error("query ran with a cancelled context")
	}
}
