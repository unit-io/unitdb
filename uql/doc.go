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

// Package uql is a small query language for unitdb.
//
// This is level 0: it says in text what the Go API already does, using only
// the public API of the engine.
//
//	FROM teams.alpha.ch1 SINCE 1h LIMIT 100
//	FROM teams.alpha.ch1 SINCE '2026-10-08T00:00:00Z' UNTIL 30m IN CONTRACT $1
//	PUT teams.alpha.ch1 VALUE $1 TTL 1h
//	PUT teams.*.ch1 VALUE $1          -- read from every teams.<x>.ch1
//	DELETE FROM teams.alpha.ch1 ID $1
//	EXPLAIN FROM teams.alpha.ch1 LIMIT 10
//
// A topic is dot-separated parts. A part is a word, a quoted string or a
// parameter. In PUT and DELETE a part may also be "*" (one part), and a
// topic may end in "..." (every part after): that is unitdb's wildcard
// write, an entry every matching topic returns. FROM reads one topic, and
// its entries include such wildcard entries; reading many topics at once is
// level 1.
//
// Values are always passed as parameters ($1, $2, ...), never spliced into
// the text, and a parameter in a topic must be exactly one part.
//
// Keywords are not case sensitive. "--" starts a comment.
//
// Later levels (SELECT, WHERE, LATEST PER TOPIC, ORDER BY, GROUP BY, TOPICS,
// indexes) are parsed far enough to say they are not available yet. The tests show each
// level at work.
package uql
