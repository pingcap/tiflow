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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	parsermysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
)

var (
	mysqlDiagnosticCode      = regexp.MustCompile(`^Error ([0-9]+)(?: \([A-Z0-9]+\))?:`)
	parserDiagnosticPosition = regexp.MustCompile(`(?i)^line ([0-9]+) column ([0-9]+)`)
)

// RedactSQLDiagnostic sanitizes known SQL-bearing DM errors on diagnostic copies.
// It retains the DM reason and sanitized SQL, and summarizes the cause using
// trusted error templates or a parser position. Unknown error classes are not
// interpreted as SQL. Original errors and status used for retry remain intact.
func RedactSQLDiagnostic(code int32, message, cause string) (string, string) {
	if !IsRedactLogEnabled() {
		return message, cause
	}
	if !sqlDiagnosticCode(terror.ErrCode(code)) {
		return message, cause
	}
	if terror.ErrCode(code) == terror.ErrSyncerUnitParseStmt.Code() {
		if cause == "" {
			cause = message
		}
		return "SQL parsing failed", redactSQLCause(cause)
	}
	base, ok := terror.ErrorFromCode(terror.ErrCode(code))
	if !ok {
		return message, cause
	}
	prefix, suffix, hasSQL := strings.Cut(base.Message(), "%s")
	if !hasSQL {
		return message, cause
	}
	if strings.HasPrefix(message, prefix) && strings.HasSuffix(message, suffix) &&
		len(message) >= len(prefix)+len(suffix) {
		sql := message[len(prefix) : len(message)-len(suffix)]
		message = prefix + RedactSQL(sql) + suffix
	} else {
		message = prefix + "(SQL details redacted)" + suffix
	}
	return message, redactSQLCause(cause)
}

func redactSQLCause(cause string) string {
	if cause == "" {
		return ""
	}
	if match := mysqlDiagnosticCode.FindStringSubmatch(cause); match != nil {
		code, _ := strconv.ParseUint(match[1], 10, 16)
		reason := mysqlDiagnosticReason(uint16(code))
		if code == uint64(parsermysql.ErrParse) {
			reason = "SQL syntax error (details redacted)"
		} else if strings.Contains(strings.ToLower(cause), "unsupported") ||
			strings.Contains(strings.ToLower(cause), "not supported") ||
			strings.Contains(strings.ToLower(cause), "doesn't yet support") {
			reason = "unsupported SQL/DDL (details redacted)"
		}
		return fmt.Sprintf("MySQL error %d: %s", code, reason)
	}
	if match := parserDiagnosticPosition.FindStringSubmatch(cause); match != nil {
		return fmt.Sprintf("SQL parse error at line %s column %s (details redacted)", match[1], match[2])
	}
	switch cause {
	case "context canceled", "context deadline exceeded", "driver: bad connection",
		"invalid connection", "EOF", "unexpected EOF":
		return cause
	}
	return "(unstructured SQL error cause; details redacted)"
}

func mysqlDiagnosticReason(code uint16) string {
	if template := parsermysql.MySQLErrName[code]; template != nil {
		return mysqlFormatArgument.ReplaceAllStringFunc(template.Raw, func(argument string) string {
			if argument == "%%" {
				return "%"
			}
			return "?"
		})
	}
	return "details redacted"
}

// SQLWarningField retains available parser warning codes and standard reasons
// without logging free-form warning text that can quote SQL literals.
func SQLWarningField(key string, warnings []error) zap.Field {
	if !IsRedactLogEnabled() {
		return zap.Errors(key, warnings)
	}
	output := make([]string, len(warnings))
	for i, warning := range warnings {
		if normalized, ok := errors.Cause(warning).(*errors.Error); ok &&
			normalized.Code() >= 0 && normalized.Code() <= 65535 {
			code := uint16(normalized.Code())
			output[i] = fmt.Sprintf("SQL warning %d: %s", code, mysqlDiagnosticReason(code))
		} else {
			output[i] = "SQL warning (details redacted)"
		}
	}
	return zap.Strings(key, output)
}

func sqlDiagnosticCode(code terror.ErrCode) bool {
	switch code {
	case terror.ErrDBExecuteFailed.Code(), terror.ErrDBQueryFailed.Code(),
		terror.ErrParseSQL.Code(), terror.ErrSyncerUnitParseStmt.Code(),
		terror.ErrSyncerUnitDDLOnMultipleTable.Code(), terror.ErrSyncerUnitInjectDDLOnly.Code(),
		terror.ErrSyncerUnitInjectDDLWithoutSchema.Code(), terror.ErrSyncerUnitOnlineDDLOnMultipleTable.Code():
		return true
	default:
		return false
	}
}
