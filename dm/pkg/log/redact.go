// Copyright 2026 PingCAP, Inc.
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

package log

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// IsSensitiveExpressionField identifies configuration fields that can contain
// SQL literals. Configuration serialization for persistence must not use it.
func IsSensitiveExpressionField(key string) bool {
	switch strings.ToLower(key) {
	case "where", "extra-args", "insert-value-expr", "update-old-value-expr",
		"update-new-value-expr", "delete-value-expr", "sql-pattern":
		return true
	}
	return false
}

// RedactConfigJSON masks SQL-bearing fields in a log-only JSON representation.
// Other fields remain available for diagnosis, including unknown fields.
func RedactConfigJSON(input string) string {
	if !IsRedactLogEnabled() {
		return input
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(input))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "(invalid config; redacted)"
	}
	var redact func(any)
	redact = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			for key, child := range value {
				if IsSensitiveExpressionField(key) {
					value[key] = "?"
				} else {
					redact(child)
				}
			}
		case []any:
			for _, child := range value {
				redact(child)
			}
		}
	}
	redact(value)
	data, err := json.Marshal(value)
	if err != nil {
		return "(invalid config; redacted)"
	}
	return string(data)
}

// ConfigField formats DM configuration for logging only. JSON tags still hide
// credentials; optional expression redaction must not affect config comparisons.
func ConfigField(key string, config any) zap.Field {
	data, err := json.Marshal(config)
	if err != nil {
		return zap.String(key, "(invalid config; redacted)")
	}
	return zap.String(key, RedactConfigJSON(string(data)))
}

// RedactField hides a sensitive field without evaluating its Stringer or
// marshaler when redaction is enabled. Otherwise its original type is retained.
func RedactField(field zap.Field) zap.Field {
	if IsRedactLogEnabled() {
		return zap.String(field.Key, "?")
	}
	return field
}

// RedactError retains error identifiers but hides free-form text, which can
// contain SQL values or raw binlog bytes. The original error is never changed,
// so callers can still classify it and make retry decisions.
func RedactError(err error) string {
	if err == nil {
		return ""
	}
	if !IsRedactLogEnabled() {
		return err.Error()
	}
	if e, ok := err.(*terror.Error); ok {
		return fmt.Sprintf("[code=%d:class=%s:scope=%s:level=%s] ?", e.Code(), e.Class(), e.Scope(), e.Level())
	}
	if e, ok := errors.Cause(err).(*mysql.MySQLError); ok {
		return fmt.Sprintf("MySQL error %d: ?", e.Number)
	}
	return "?"
}

// WithRedaction protects error fields in DM and embedded component loggers.
// Sensitive non-error fields must be explicitly annotated with RedactField.
func WithRedaction(logger *zap.Logger) *zap.Logger {
	return logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return &redactingCore{Core: core}
	}))
}

type redactingCore struct {
	zapcore.Core
}

func (c *redactingCore) With(fields []zapcore.Field) zapcore.Core {
	return &redactingCore{Core: c.Core.With(redactErrorFields(fields))}
}

func (c *redactingCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Core.Check(entry, nil) != nil {
		return checked.AddCore(entry, c)
	}
	return checked
}

func (c *redactingCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	return c.Core.Write(entry, redactErrorFields(fields))
}

func redactErrorFields(fields []zapcore.Field) []zapcore.Field {
	if !IsRedactLogEnabled() {
		return fields
	}
	result := append([]zapcore.Field(nil), fields...)
	for i, field := range result {
		// Embedded Dumpling/Lightning log JSON configs and SQL themselves.
		// Their storage credentials are already masked by Config.String().
		switch strings.ToLower(field.Key) {
		case "conf", "config", "cfg":
			switch field.Type {
			case zapcore.StringType:
				result[i] = zap.String(field.Key, RedactConfigJSON(field.String))
			case zapcore.StringerType:
				if value, ok := field.Interface.(fmt.Stringer); ok {
					result[i] = zap.String(field.Key, RedactConfigJSON(value.String()))
				}
			}
		case "sql", "query", "queries", "statement", "statements", "args", "arguments", "row", "row data":
			result[i] = zap.String(field.Key, "?")
		}
		if field.Type == zapcore.ErrorType {
			if err, ok := field.Interface.(error); ok {
				result[i] = zap.String(field.Key, RedactError(err))
			}
		} else if field.Key == "error" || field.Key == "err" || field.Key == "warning messages" {
			// Some components format errors before passing them to zap.
			result[i] = zap.String(field.Key, "?")
		}
	}
	return result
}
