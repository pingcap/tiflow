// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redact

import (
	"fmt"

	"github.com/pingcap/errors"
	tidbredact "github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// ensure the lazy types implement the zap interfaces at compile time
	_ fmt.Stringer           = lazyString("")
	_ fmt.Stringer           = lazyAny{}
	_ zapcore.ArrayMarshaler = lazyStrings(nil)
)

// lazyString defers marker redaction of a string until the log entry is
// actually encoded. Building the marked copy eagerly is wasted work when the
// entry is filtered out by the log level, which is the common case for
// debug logs on hot paths.
type lazyString string

func (s lazyString) String() string {
	return markString(string(s))
}

// lazyAny defers both formatting (fmt.Sprint) and marker redaction of an
// arbitrary value until the log entry is actually encoded.
type lazyAny struct {
	value interface{}
}

func (a lazyAny) String() string {
	return markString(fmt.Sprint(a.value))
}

// lazyStrings defers marker redaction of a string slice until the log entry
// is actually encoded.
type lazyStrings []string

// MarshalLogArray implements zapcore.ArrayMarshaler.
func (s lazyStrings) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, v := range s {
		enc.AppendString(markString(v))
	}
	return nil
}

// RedactionMode represents the type of log redaction
type RedactionMode int32

const (
	// RedactionModeOff disables redaction, shows all content
	RedactionModeOff RedactionMode = iota
	// RedactionModeOn enables redaction, replaces sensitive content with "?"
	RedactionModeOn
	// RedactionModeMarker enables redaction with markers, surrounds content with ‹›
	RedactionModeMarker
)

// String returns the string representation of RedactionMode
func (mode RedactionMode) String() string {
	switch mode {
	case RedactionModeOff:
		return "OFF"
	case RedactionModeOn:
		return "ON"
	case RedactionModeMarker:
		return "MARKER"
	default:
		return "UNKNOWN"
	}
}

// SetRedactionMode sets the global redaction mode shared with TiDB errors and
// TiKV client logs. Invalid modes enable full redaction.
func SetRedactionMode(mode RedactionMode) {
	switch mode {
	case RedactionModeOff:
		errors.RedactLogEnabled.Store(errors.RedactLogDisable)
	case RedactionModeMarker:
		errors.RedactLogEnabled.Store(errors.RedactLogMarker)
	default:
		errors.RedactLogEnabled.Store(errors.RedactLogEnable)
	}
}

// GetRedactionMode returns the current global redaction mode. The empty default
// disables redaction, while unknown modes enable full redaction.
func GetRedactionMode() RedactionMode {
	switch errors.RedactLogEnabled.Load() {
	case "", errors.RedactLogDisable:
		return RedactionModeOff
	case errors.RedactLogMarker:
		return RedactionModeMarker
	default:
		return RedactionModeOn
	}
}

// String redacts a string according to the current global redaction mode
func String(input string) string {
	mode := GetRedactionMode()
	switch mode {
	case RedactionModeMarker:
		return markString(input)
	case RedactionModeOff:
		return input
	case RedactionModeOn:
		return "?"
	default:
		return "?"
	}
}

// markString escapes existing markers before surrounding the input. It does
// not consult the global mode, so lazy fields keep the mode selected when
// they were constructed even if the configuration changes before encoding.
func markString(input string) string {
	return tidbredact.String(errors.RedactLogMarker, input)
}

// ZapString returns a zap field with redacted string content.
//
// In marker mode the redacted copy is built lazily, when the entry is encoded,
// so callers can pass large strings to log.Debug without paying for them when
// debug logging is disabled.
func ZapString(key string, value string) zap.Field {
	mode := GetRedactionMode()
	switch mode {
	case RedactionModeOff:
		return zap.String(key, value)
	case RedactionModeOn:
		return zap.String(key, "?")
	case RedactionModeMarker:
		return zap.Stringer(key, lazyString(value))
	default:
		return zap.String(key, "?")
	}
}

// ZapAny returns a zap field with redacted content.
//
// In marker mode the value is formatted with fmt.Sprint and redacted lazily,
// when the entry is encoded. Filtered log entries skip formatting and escaping.
func ZapAny(key string, value interface{}) zap.Field {
	mode := GetRedactionMode()
	switch mode {
	case RedactionModeOff:
		return zap.Any(key, value)
	case RedactionModeOn:
		return zap.String(key, "?")
	case RedactionModeMarker:
		// use fmt.Sprint instead of zap.Any("", value).String as it is not materialized string
		return zap.Stringer(key, lazyAny{value: value})
	default:
		return zap.String(key, "?")
	}
}

// ZapStringer returns a zap field with redacted stringer content. String is
// called lazily in marker mode and is never called when redaction is on.
func ZapStringer(key string, value fmt.Stringer) zap.Field {
	switch GetRedactionMode() {
	case RedactionModeOff:
		return zap.Stringer(key, value)
	case RedactionModeMarker:
		return zap.Stringer(key, lazyAny{value: value})
	default:
		return zap.String(key, "?")
	}
}

// ZapStrings returns a zap field with redacted string slice content.
//
// In marker mode the redacted copies are built lazily, when the entry is encoded.
func ZapStrings(key string, value []string) zap.Field {
	mode := GetRedactionMode()
	switch mode {
	case RedactionModeOff:
		return zap.Strings(key, value)
	case RedactionModeOn:
		return zap.String(key, "?")
	case RedactionModeMarker:
		return zap.Array(key, lazyStrings(value))
	default:
		return zap.String(key, "?")
	}
}
