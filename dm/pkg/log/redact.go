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
	stderrors "errors"
	"fmt"
	"regexp"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	parsermysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// mysqlFormatArgument matches placeholders in trusted MySQL message templates,
// never in server-provided error text. All dynamic arguments are omitted because
// an error packet does not expose their boundaries or sensitivity separately.
var mysqlFormatArgument = regexp.MustCompile(`%%|%[-+# 0-9.*\[\]]*[a-zA-Z]`)

// RedactDBError hides values in known MySQL data errors, including wrapped
// errors, while retaining error codes and standard reasons. Other errors,
// including syntax and unsupported-DDL errors, retain their original text.
// The original error is never changed; execution and retry use that original.
func RedactDBError(err error) string {
	if err == nil {
		return ""
	}
	if text, ok := redactDBError(err); ok {
		return text
	}
	return err.Error()
}

func redactDBError(err error) (string, bool) {
	if !IsRedactLogEnabled() || err == nil {
		return "", false
	}
	var dbErr *mysql.MySQLError
	var dmErr *terror.Error
	// DM errors expose Cause; standard-library wrappers expose Unwrap.
	for current := err; current != nil; {
		if typed, ok := current.(*terror.Error); ok && dmErr == nil {
			dmErr = typed
		}
		if typed, ok := current.(*mysql.MySQLError); ok {
			dbErr = typed
			break
		}
		next := errors.Unwrap(current)
		if next == nil {
			next = stderrors.Unwrap(current)
		}
		current = next
	}
	if dbErr == nil {
		return "", false
	}
	// Limit this change to data errors whose messages can contain row values.
	// SQL/DDL syntax and other free-form diagnostics need a separate policy.
	switch dbErr.Number {
	case parsermysql.ErrDupEntry, parsermysql.ErrDupEntryWithKeyName,
		parsermysql.ErrDupEntryAutoincrementCase, parsermysql.ErrTruncatedWrongValue,
		parsermysql.ErrTruncatedWrongValueForField, parsermysql.ErrIllegalValueForType,
		parsermysql.ErrWrongValueForType, parsermysql.ErrWrongValue,
		parsermysql.ErrNoPartitionForGivenValue, parsermysql.ErrDataOutOfRange,
		parsermysql.ErrForeignDuplicateKeyOldUnused, parsermysql.ErrForeignDuplicateKeyWithChildInfo,
		parsermysql.ErrForeignDuplicateKeyWithoutChildInfo:
	default:
		return "", false
	}
	reason := "details redacted"
	if template := parsermysql.MySQLErrName[dbErr.Number]; template != nil {
		reason = mysqlFormatArgument.ReplaceAllStringFunc(template.Raw, func(argument string) string {
			if argument == "%%" {
				return "%"
			}
			return "?"
		})
	}
	text := fmt.Sprintf("MySQL error %d: %s", dbErr.Number, reason)
	if dmErr != nil {
		text = fmt.Sprintf("[code=%d:class=%s:scope=%s:level=%s] %s",
			dmErr.Code(), dmErr.Class(), dmErr.Scope(), dmErr.Level(), text)
	}
	return text, true
}

// WithDBErrorRedaction protects typed MySQL data-error fields, including named
// and logger-bound errors. SQL fields and other errors retain their original
// representation. ShortError handles errors formatted before reaching zap.
func WithDBErrorRedaction(logger *zap.Logger) *zap.Logger {
	return logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return &dbErrorRedactingCore{Core: core}
	}))
}

type dbErrorRedactingCore struct {
	zapcore.Core
}

func (c *dbErrorRedactingCore) With(fields []zapcore.Field) zapcore.Core {
	return &dbErrorRedactingCore{Core: c.Core.With(redactDBErrorFields(fields))}
}

func (c *dbErrorRedactingCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Core.Check(entry, nil) != nil {
		return checked.AddCore(entry, c)
	}
	return checked
}

func (c *dbErrorRedactingCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	return c.Core.Write(entry, redactDBErrorFields(fields))
}

func redactDBErrorFields(fields []zapcore.Field) []zapcore.Field {
	if !IsRedactLogEnabled() {
		return fields
	}
	var result []zapcore.Field
	for i, field := range fields {
		if field.Type != zapcore.ErrorType {
			continue
		}
		if err, ok := field.Interface.(error); ok {
			if text, redacted := redactDBError(err); redacted {
				if result == nil {
					result = append([]zapcore.Field(nil), fields...)
				}
				result[i] = zap.String(field.Key, text)
			}
		}
	}
	if result == nil {
		return fields
	}
	return result
}
