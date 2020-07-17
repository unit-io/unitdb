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

package unitdb

import (
	"os"
	"strings"

	"github.com/rs/zerolog"
)

// Logger is logger to use in application.
var logger = zerolog.New(os.Stderr).With().Timestamp().Logger()

// Info logs the action with a tag.
func Info(context, action string) {
	logger.Info().Str("context", context).Msg(action)
}

// Fatal logs the fatal error messages.
func Fatal(context, msg string, err error) {
	logger.Fatal().
		Err(err).
		Str("context", context).Msg(msg)
}

// Debug logs the debug message with tag if it is turned on.
func Debug(context, msg string) {
	logger.Debug().Str("context", context).Msg(msg)
}

// ParseLevel parses a string which represents a log level and returns
// a zerolog.Level.
func ParseLevel(level string, defaultLevel zerolog.Level) zerolog.Level {
	l := defaultLevel
	switch strings.ToLower(level) {
	case "0", "debug":
		l = zerolog.DebugLevel
	case "1", "info":
		l = zerolog.InfoLevel
	case "2", "warn":
		l = zerolog.WarnLevel
	case "3", "error":
		l = zerolog.ErrorLevel
	case "4", "fatal":
		l = zerolog.FatalLevel
	}
	return l
}
