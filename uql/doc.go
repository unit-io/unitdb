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
//	FROM teams.alpha.ch1 SINCE 1h LIMIT 100
//	SELECT topic, title FROM app.project.* LATEST PER TOPIC WHERE workspaceId = $1
//	SELECT workspaceId, COUNT(*) AS n FROM app.project.* GROUP BY workspaceId ORDER BY n DESC
//	TOPICS app.project.*
//	PUT teams.*.ch1 VALUE $1 TTL 1h       -- a wildcard write: read by every teams.<x>.ch1
//	DELETE FROM teams.alpha.ch1 ID $1
//	DELETE FROM logs.* BEFORE 7d
//	CREATE INDEX by_ws ON app.project.* (workspaceId) LATEST
//	CREATE RANGE INDEX by_updated ON app.project.* (updatedAt) LATEST
//	EXPLAIN SELECT ...
//
// A topic is dot-separated parts: words, quoted strings or parameters. A
// part may be "*" (any one part) and a topic may end in "..." (any parts
// after). FROM one topic reads it as unitdb does, with the entries put to
// wildcard topics that match it; FROM a pattern reads each matching topic's
// own entries, newest first.
//
// Values are always parameters ($1, $2, ...), never spliced into the text,
// and a parameter in a topic is exactly one part.
//
// Fields are topic, id and time, which every entry has, and payload fields
// of JSON payloads: title, data.workspaceId, tags[0].name, or payload.id for
// a field a built-in name hides.
//
// UQL watches the database's writes to name topics (the engine keeps only
// hashes) and keep indexes, in topics starting with "$uql.". Open it with
// New in every process that writes, and Close it.
//
// Keywords are not case sensitive. "--" starts a comment. The tests show each
// level at work.
package uql
