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
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/unit-io/unitdb"
)

func put(t *testing.T, d *DB, topic, payload string) {
	t.Helper()
	if _, err := d.Exec(context.Background(), "PUT "+topic+" VALUE $1", payload); err != nil {
		t.Fatal(err)
	}
}

func query(t *testing.T, d *DB, q string, args ...any) *Rows {
	t.Helper()
	r, err := d.Query(context.Background(), q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}

// cells prints rows as "a|b,c|d".
func cells(r *Rows) string {
	var rows []string
	for r.Next() {
		var vals []string
		for _, v := range r.Values() {
			switch x := v.(type) {
			case float64:
				vals = append(vals, fmt.Sprint(x))
			case nil:
				vals = append(vals, "NULL")
			default:
				vals = append(vals, fmt.Sprint(x))
			}
		}
		rows = append(rows, strings.Join(vals, "|"))
	}
	return strings.Join(rows, ",")
}

func seedProjects(t *testing.T, d *DB) {
	for _, p := range []struct {
		id, ws, title string
		n             int
	}{
		{"p1", "w1", "Alpha", 3},
		{"p2", "w2", "beta", 1},
		{"p3", "w1", "Gamma", 7},
		{"p4", "w2", "delta", 5},
	} {
		put(t, d, "ls.d.project."+p.id, fmt.Sprintf(`{"id":%q,"workspaceId":%q,"title":%q,"n":%d,"tags":[{"name":"t-%s"}]}`, p.id, p.ws, p.title, p.n, p.ws))
	}
}

func TestWildcardFrom(t *testing.T) {
	d := openDB(t)
	seedProjects(t, d)
	put(t, d, "ls.d.asset.a1", `{"id":"a1"}`)
	put(t, d, "ls.d.project.p1", `{"id":"p1","workspaceId":"w1","title":"Alpha 2","n":4}`)
	// A broadcast is read through the topics it matches, not by a pattern.
	put(t, d, "ls.d.project.*", `{"broadcast":true}`)

	if got := cells(query(t, d, "SELECT payload.id FROM ls.d.project.*")); got != "p1,p4,p3,p2,p1" {
		t.Fatalf("newest first across topics = %s", got)
	}
	if got := cells(query(t, d, "SELECT topic, title FROM ls.d.project.* LATEST PER TOPIC")); got != "ls.d.project.p1|Alpha 2,ls.d.project.p4|delta,ls.d.project.p3|Gamma,ls.d.project.p2|beta" {
		t.Fatalf("latest = %s", got)
	}
	if got := cells(query(t, d, "SELECT payload.id FROM ls.d...")); got != "p1,a1,p4,p3,p2,p1" {
		t.Fatalf("ls.d... = %s", got)
	}
	if n := query(t, d, "FROM ..."); n.Len() != 6 {
		t.Fatalf("FROM ... read %d entries; UQL's own topics must not show", n.Len())
	}
	if got := cells(query(t, d, "SELECT payload.id FROM ls.d.project.* LIMIT 2 OFFSET 1")); got != "p4,p3" {
		t.Fatalf("offset = %s", got)
	}
	if got := cells(query(t, d, "TOPICS ls.d.*.*")); got != "ls.d.asset.a1|"+hashOf(t, d, "ls.d.asset.a1")+",ls.d.project.p1|"+hashOf(t, d, "ls.d.project.p1")+",ls.d.project.p2|"+hashOf(t, d, "ls.d.project.p2")+",ls.d.project.p3|"+hashOf(t, d, "ls.d.project.p3")+",ls.d.project.p4|"+hashOf(t, d, "ls.d.project.p4") {
		t.Fatalf("topics = %s", got)
	}
}

func hashOf(t *testing.T, d *DB, topic string) string {
	h, err := d.db.TopicHash([]byte(topic), 0)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", h)
}

func TestWhereSelectGroupOrder(t *testing.T) {
	d := openDB(t)
	seedProjects(t, d)
	for _, tc := range []struct {
		q    string
		args []any
		want string
	}{
		{"SELECT payload.id FROM ls.d.project.* WHERE workspaceId = $1", []any{"w1"}, "p3,p1"},
		{"SELECT payload.id FROM ls.d.project.* WHERE n > 2 AND n <= 5 ORDER BY n", nil, "p1,p4"},
		{"SELECT payload.id FROM ls.d.project.* WHERE n >= $1", []any{5}, "p4,p3"},
		{"SELECT payload.id FROM ls.d.project.* WHERE LOWER(title) LIKE 'a%' OR payload.id IN ('p2')", nil, "p2,p1"},
		{"SELECT payload.id FROM ls.d.project.* WHERE NOT workspaceId = 'w1'", nil, "p4,p2"},
		{"SELECT payload.id FROM ls.d.project.* WHERE ANY(tags, x -> x.name = 't-w2')", nil, "p4,p2"},
		{"SELECT payload.id FROM ls.d.project.* WHERE missing IS NULL AND tags[0].name IS NOT NULL LIMIT 1", nil, "p4"},
		{"SELECT payload.id, n FROM ls.d.project.* ORDER BY n DESC LIMIT 2", nil, "p3|7,p4|5"},
		{"SELECT payload.id FROM ls.d.project.* ORDER BY workspaceId, title", nil, "p1,p3,p2,p4"},
		{"SELECT UPPER(payload.id) AS u FROM ls.d.project.* ORDER BY u DESC LIMIT 1", nil, "P4"},
		{"SELECT workspaceId, COUNT(*) AS c, SUM(n), MAX(title), AVG(n) FROM ls.d.project.* GROUP BY workspaceId ORDER BY workspaceId", nil, "w1|2|10|Gamma|5,w2|2|6|delta|3"},
		{"SELECT COUNT(*) FROM ls.d.project.* WHERE n > 100", nil, "0"},
		{"SELECT COUNT(*) AS c FROM ls.d.project.* WHERE n > 100 GROUP BY workspaceId", nil, ""},
		{"SELECT COUNT(*) + 1 FROM ls.d.project.*", nil, ""},
		{"SELECT LENGTH(title) FROM ls.d.project.p3", nil, "5"},
		{"SELECT COUNT(*) FROM ls.d.project.* WHERE time > $1", []any{"2000-01-01T00:00:00Z"}, "4"},
	} {
		r, err := d.Query(context.Background(), tc.q, tc.args...)
		if tc.q == "SELECT COUNT(*) + 1 FROM ls.d.project.*" {
			if err == nil {
				t.Errorf("%s: arithmetic isn't UQL, want a parse error", tc.q)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.q, err)
			continue
		}
		if got := cells(r); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.q, got, tc.want)
		}
	}
	// Scan into Go values.
	r := query(t, d, "SELECT payload.id, n, time FROM ls.d.project.p3")
	r.Next()
	var id string
	var n int
	var at any
	if err := r.Scan(&id, &n, &at); err != nil || id != "p3" || n != 7 || at == nil {
		t.Fatalf("Scan = %q %d %v %v", id, n, at, err)
	}
	put(t, d, "raw.x", "plain text")
	if got := cells(query(t, d, "SELECT payload, payload.a FROM raw.x")); got != "plain text|NULL" {
		t.Fatalf("non-JSON payload = %s", got)
	}
	if r.Columns()[1] != "n" {
		t.Fatalf("columns = %v", r.Columns())
	}
	if _, err := d.Query(context.Background(), "FROM ls.d.project.* WHERE id = $2", "x"); err == nil || !strings.Contains(err.Error(), "$2 has no value") {
		t.Fatalf("missing parameter error = %v", err)
	}
}

func TestScanLimit(t *testing.T) {
	d := openDB(t)
	seedProjects(t, d)
	d.MaxScan = 3
	if _, err := d.Query(context.Background(), "FROM ls.d.project.* WHERE n > 0"); !errors.Is(err, ErrScanLimit) {
		t.Fatalf("error = %v, want ErrScanLimit", err)
	}
	// Without filters, only LIMIT entries of each topic are read.
	if r := query(t, d, "FROM ls.d.project.* LIMIT 1"); r.Len() != 1 {
		t.Fatalf("limit 1 = %d", r.Len())
	}
	if r := query(t, d, "FROM ls.d.project.p1 LATEST PER TOPIC WHERE n > 0"); r.Len() != 1 {
		t.Fatalf("latest = %d", r.Len())
	}
}

func TestDeleteBeforeKeepLatest(t *testing.T) {
	ctx := context.Background()
	d := openDB(t)
	for i := 0; i < 5; i++ {
		put(t, d, "log.a", fmt.Sprint(i))
		put(t, d, "log.b", fmt.Sprint(i))
	}
	res, err := d.Exec(ctx, "DELETE FROM log.* KEEP LATEST 2")
	if err != nil || res.Affected != 6 {
		t.Fatalf("keep latest = %+v %v", res, err)
	}
	if got := cells(query(t, d, "SELECT payload FROM log.*")); got != "4,4,3,3" {
		t.Fatalf("kept = %s", got)
	}
	res, err = d.Exec(ctx, "DELETE FROM log.a BEFORE $1", d.now().Add(1e9*3600))
	if err != nil || res.Affected != 2 {
		t.Fatalf("before = %+v %v", res, err)
	}
	if got := cells(query(t, d, "SELECT topic FROM log.*")); got != "log.b,log.b" {
		t.Fatalf("after before = %s", got)
	}
}

func reopen(t *testing.T, dir string, clean bool, prev *DB) *DB {
	t.Helper()
	if prev != nil {
		if clean {
			if err := prev.Close(); err != nil {
				t.Fatal(err)
			}
		} else {
			prev.remove() // a crash: the hook goes with the process
		}
		if err := prev.db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	db, err := unitdb.Open(dir, unitdb.WithDefaultOptions(), unitdb.WithMutable())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	d, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestCatalogSurvivesReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	d := reopen(t, dir, true, nil)
	put(t, d, "a.b", "1")
	d = reopen(t, dir, true, d)
	if got := cells(query(t, d, "SELECT topic FROM a.*")); got != "a.b" {
		t.Fatalf("topic name after reopen = %s", got)
	}
	// A topic written without UQL shows its hash until named.
	d.remove()
	d.remove = func() {}
	if err := d.db.Put([]byte("x.y"), []byte("raw")); err != nil {
		t.Fatal(err)
	}
	d = reopen(t, dir, false, d)
	got := cells(query(t, d, "SELECT topic FROM x.*"))
	if !strings.HasPrefix(got, "#") {
		t.Fatalf("unnamed topic = %s", got)
	}
	if err := d.Name(0, "x.y"); err != nil {
		t.Fatal(err)
	}
	if got := cells(query(t, d, "SELECT topic FROM x.*")); got != "x.y" {
		t.Fatalf("named topic = %s", got)
	}
}

func TestHashIndex(t *testing.T) {
	ctx := context.Background()
	d := openDB(t)
	seedProjects(t, d)
	res, err := d.Exec(ctx, "CREATE INDEX by_ws ON ls.d.project.* (workspaceId)")
	if err != nil || res.Affected != 4 {
		t.Fatalf("create = %+v %v", res, err)
	}
	if _, err := d.Exec(ctx, "CREATE INDEX by_ws ON ls.d.project.* (title)"); err == nil {
		t.Fatal("index created twice")
	}
	q := "SELECT payload.id FROM ls.d.project.* WHERE workspaceId = $1"
	p, _ := d.Explain(q, "w1")
	if p.Index != "by_ws" {
		t.Fatalf("plan doesn't use the index:\n%s", p)
	}
	// Index entries are written as entries are.
	put(t, d, "ls.d.project.p5", `{"id":"p5","workspaceId":"w1","n":0}`)
	d.MaxScan = 4 // the index reads 3, not 5
	if got := cells(query(t, d, q, "w1")); got != "p5,p3,p1" {
		t.Fatalf("by index = %s", got)
	}
	// A deleted entry is checked away.
	r := query(t, d, "FROM ls.d.project.p3")
	if _, err := d.Exec(ctx, "DELETE FROM ls.d.project.p3 ID $1", r.All()[0].IDString()); err != nil {
		t.Fatal(err)
	}
	if got := cells(query(t, d, q, "w1")); got != "p5,p1" {
		t.Fatalf("after delete = %s", got)
	}
	// Without LATEST, an old version is still an entry.
	put(t, d, "ls.d.project.p1", `{"id":"p1","workspaceId":"w2"}`)
	if got := cells(query(t, d, q, "w1")); got != "p5,p1" {
		t.Fatalf("history = %s", got)
	}
	if got := cells(query(t, d, "SELECT payload.id FROM ls.d.project.* WHERE workspaceId = 'w2'")); got != "p1,p4,p2" {
		t.Fatalf("w2 = %s", got)
	}
	if _, err := d.Exec(ctx, "DROP INDEX by_ws"); err != nil {
		t.Fatal(err)
	}
	if p, _ := d.Explain(q, "w1"); p.Index != "" {
		t.Fatalf("dropped index used:\n%s", p)
	}
	if _, err := d.Exec(ctx, "DROP INDEX by_ws"); err == nil {
		t.Fatal("dropped twice")
	}
}

func TestLatestIndex(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	d := reopen(t, dir, true, nil)
	seedProjects(t, d)
	if _, err := d.Exec(ctx, "CREATE INDEX cur_ws ON ls.d.project.* (workspaceId) LATEST"); err != nil {
		t.Fatal(err)
	}
	q := "SELECT payload.id FROM ls.d.project.* LATEST PER TOPIC WHERE workspaceId = $1"
	if p, _ := d.Explain(q, "w1"); p.Index != "cur_ws" {
		t.Fatalf("plan:\n%s", p)
	}
	if p, _ := d.Explain("FROM ls.d.project.* WHERE workspaceId = 'w1'"); p.Index != "" {
		t.Fatalf("a LATEST index served a query without LATEST:\n%s", p)
	}
	// p1 moves to w2: it is no longer in w1.
	put(t, d, "ls.d.project.p1", `{"id":"p1","workspaceId":"w2"}`)
	if got := cells(query(t, d, q, "w1")); got != "p3" {
		t.Fatalf("w1 = %s", got)
	}
	if got := cells(query(t, d, q, "w2")); got != "p1,p4,p2" {
		t.Fatalf("w2 = %s", got)
	}
	// Deleting p1's newest version makes the old one current again.
	r := query(t, d, "FROM ls.d.project.p1 LIMIT 1")
	if _, err := d.Exec(ctx, "DELETE FROM ls.d.project.p1 ID $1", r.All()[0].IDString()); err != nil {
		t.Fatal(err)
	}
	if got := cells(query(t, d, q, "w1")); got != "p3,p1" {
		t.Fatalf("w1 after delete = %s", got)
	}
	// Written without UQL watching, then reopened after a crash: rebuilt.
	d.remove()
	if err := d.db.Put([]byte("ls.d.project.p9"), []byte(`{"id":"p9","workspaceId":"w1"}`)); err != nil {
		t.Fatal(err)
	}
	d.remove = func() {}
	d = reopen(t, dir, false, d)
	d.Name(0, "ls.d.project.p9")
	if got := cells(query(t, d, q, "w1")); got != "p9,p3,p1" {
		t.Fatalf("after rebuild = %s", got)
	}
	// And after a clean close, the index is loaded as it is.
	d = reopen(t, dir, true, d)
	if got := cells(query(t, d, q, "w1")); got != "p9,p3,p1" {
		t.Fatalf("after clean reopen = %s", got)
	}
}

func TestRangeIndex(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	d := reopen(t, dir, true, nil)
	for i := 0; i < 50; i++ {
		put(t, d, fmt.Sprintf("s.u%02d", i), fmt.Sprintf(`{"score":%d,"name":"u%02d"}`, (i*37)%50, i))
	}
	put(t, d, "s.text", `{"score":"high"}`)
	put(t, d, "s.none", `{"name":"none"}`)
	if _, err := d.Exec(ctx, "CREATE RANGE INDEX by_score ON s.* (score)"); err != nil {
		t.Fatal(err)
	}
	q := "SELECT score FROM s.* ORDER BY score DESC LIMIT 3"
	p, _ := d.Explain(q)
	if p.Index != "by_score" || !strings.Contains(p.String(), "no sort") {
		t.Fatalf("plan:\n%s", p)
	}
	d.MaxScan = 10 // ordered reads stop at LIMIT
	if got := cells(query(t, d, q)); got != "high,49,48" {
		t.Fatalf("top = %s", got)
	}
	if got := cells(query(t, d, "SELECT score FROM s.* WHERE score >= 10 AND score < 13")); got != strings.Join(newestFirst(d, 10, 11, 12), ",") {
		t.Fatalf("range = %s", got)
	}
	if got := cells(query(t, d, "SELECT score FROM s.* WHERE score > 5 ORDER BY score LIMIT 2 OFFSET 1")); got != "7,8" {
		t.Fatalf("range ordered = %s", got)
	}
	if got := cells(query(t, d, "SELECT score FROM s.* WHERE score = 20")); got != "20" {
		t.Fatalf("equal = %s", got)
	}
	// New entries and deletes.
	put(t, d, "s.new", `{"score":100}`)
	if got := cells(query(t, d, "SELECT name FROM s.* WHERE score > 48 ORDER BY score DESC")); got != "NULL,u27" {
		t.Fatalf("after put = %s", got)
	}
	r := query(t, d, "FROM s.new")
	d.Exec(ctx, "DELETE FROM s.new ID $1", r.All()[0].IDString())
	if got := cells(query(t, d, "SELECT score FROM s.* WHERE score > 48")); got != "49" {
		t.Fatalf("after delete = %s", got)
	}
	// Same answers without the index.
	d.MaxScan = 0
	d = reopen(t, dir, true, d)
	if got := cells(query(t, d, "SELECT score FROM s.* WHERE score > 46 ORDER BY score")); got != "47,48,49" {
		t.Fatalf("after reopen = %s", got)
	}
	d.Exec(ctx, "DROP INDEX by_score")
	if got := cells(query(t, d, "SELECT score FROM s.* WHERE score > 46 ORDER BY score")); got != "47,48,49" {
		t.Fatalf("without index = %s", got)
	}
}

// newestFirst returns scores in the order a query returns them: by when
// they were written. Score s was written by i with (i*37)%50 == s.
func newestFirst(_ *DB, scores ...int) []string {
	at := map[int]int{}
	for i := 0; i < 50; i++ {
		at[(i*37)%50] = i
	}
	sort.Slice(scores, func(a, b int) bool { return at[scores[a]] > at[scores[b]] })
	out := make([]string, len(scores))
	for i, s := range scores {
		out[i] = fmt.Sprint(s)
	}
	return out
}

func TestRangeLatestIndex(t *testing.T) {
	ctx := context.Background()
	d := openDB(t)
	for i := 0; i < 5; i++ {
		put(t, d, fmt.Sprintf("p.%d", i), fmt.Sprintf(`{"v":%d}`, i))
	}
	if _, err := d.Exec(ctx, "CREATE RANGE INDEX cur_v ON p.* (v) LATEST"); err != nil {
		t.Fatal(err)
	}
	put(t, d, "p.0", `{"v":10}`)
	q := "SELECT topic, v FROM p.* LATEST PER TOPIC ORDER BY v DESC LIMIT 2"
	if p, _ := d.Explain(q); p.Index != "cur_v" {
		t.Fatalf("plan:\n%s", p)
	}
	if got := cells(query(t, d, q)); got != "p.0|10,p.4|4" {
		t.Fatalf("latest by v = %s", got)
	}
	if got := cells(query(t, d, "SELECT topic FROM p.* LATEST PER TOPIC WHERE v < 1")); got != "" {
		t.Fatalf("old version found = %s", got)
	}
}

func TestIndexContract(t *testing.T) {
	ctx := context.Background()
	d := openDB(t)
	d.Exec(ctx, "PUT c.a VALUE $1 IN CONTRACT 7", `{"k":1}`)
	d.Exec(ctx, "PUT c.a VALUE $1", `{"k":1}`)
	if _, err := d.Exec(ctx, "CREATE INDEX k7 ON c.* (k) IN CONTRACT 7"); err != nil {
		t.Fatal(err)
	}
	if p, _ := d.Explain("FROM c.* WHERE k = 1"); p.Index != "" {
		t.Fatal("contract 7's index used for the master contract")
	}
	r := query(t, d, "FROM c.* WHERE k = 1 IN CONTRACT 7")
	if r.Len() != 1 {
		t.Fatalf("contract 7 = %d", r.Len())
	}
	if r := query(t, d, "FROM c.* WHERE k = 1"); r.Len() != 1 {
		t.Fatalf("master = %d", r.Len())
	}
}
