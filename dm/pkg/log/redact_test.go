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
	"context"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	parsermysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRedactDBError(t *testing.T) {
	previous := IsRedactLogEnabled()
	t.Cleanup(func() { SetRedactLog(previous) })
	cases := []struct {
		code    uint16
		message string
		reason  string
	}{
		{1062, "Duplicate entry 'private-value' for key 'uk_email'", "Duplicate entry '?' for key '?'"},
		{1292, "Truncated incorrect DECIMAL value: 'private-value'", "Truncated incorrect ? value: '?'"},
		{
			parsermysql.ErrForeignDuplicateKeyWithChildInfo,
			"Foreign key constraint for table 't', record 'private-value' would lead to a duplicate entry in table 'c', key 'k'",
			"Foreign key constraint for table '?', record '?' would lead to a duplicate entry in table '?', key '?'",
		},
		{1366, "Incorrect integer value: 'private-value' for column 'uid' at row 1", "Incorrect ? value: '?' for column '?' at row ?"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.code), func(t *testing.T) {
			dbErr := &mysql.MySQLError{Number: tc.code, Message: tc.message}
			before := dbErr.Error()
			wrappers := []error{
				dbErr, errors.Annotate(dbErr, "SQL private-value"), fmt.Errorf("query private-value: %w", dbErr),
				terror.ErrDBExecuteFailed.Delegate(dbErr, "SQL private-value"),
				fmt.Errorf("context private-value: %w", terror.ErrDBExecuteFailed.Delegate(dbErr, "SQL private-value")),
				terror.ErrDBExecuteFailed.Delegate(fmt.Errorf("query private-value: %w", dbErr), "SQL private-value"),
			}
			for _, err := range wrappers {
				SetRedactLog(true)
				originalCause := errors.Cause(err)
				got := RedactDBError(err)
				require.Contains(t, got, fmt.Sprintf("MySQL error %d: %s", tc.code, tc.reason))
				require.NotContains(t, got, "private-value")
				require.Equal(t, before, dbErr.Error())
				require.Same(t, originalCause, errors.Cause(err))
				SetRedactLog(false)
				require.Equal(t, err.Error(), RedactDBError(err))
			}
		})
	}
	SetRedactLog(true)
	require.Empty(t, RedactDBError(nil))
	for _, err := range []error{
		context.Canceled, errors.New("non-database reason"),
		&mysql.MySQLError{Number: 1406, Message: "Data too long for column 'email' at row 123"},
		&mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock; try restarting transaction"},
		&mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded; try restarting transaction"},
		&mysql.MySQLError{Number: 1064, Message: "syntax error near a SQL fragment"},
		&mysql.MySQLError{Number: 1235, Message: "This version doesn't yet support a DDL feature"},
		&mysql.MySQLError{Number: 65000, Message: "unknown server error"},
		terror.ErrDBExecuteFailed.Delegate(errors.New("network failure"), "SQL text"),
	} {
		require.Equal(t, err.Error(), RedactDBError(err))
	}
}

func TestDBErrorRedactionCore(t *testing.T) {
	previous := IsRedactLogEnabled()
	t.Cleanup(func() { SetRedactLog(previous) })
	dbErr := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'private-value' for key 'uk_email'"}
	for _, enabled := range []bool{false, true} {
		SetRedactLog(enabled)
		core, logs := observer.New(zap.DebugLevel)
		logger := WithDBErrorRedaction(zap.New(core))
		fields := []zap.Field{zap.Error(dbErr), zap.String("query", "INSERT INTO t(email) VALUES (?)")}
		logger.With(fields...).Error("bound")
		logger.Error("named", zap.NamedError("cause", errors.Annotate(dbErr, "query private-value")))
		logger.Error("short", ShortError(dbErr))
		logger.Error("plain", zap.Error(context.Canceled), zap.String("error text", "ordinary error"))
		logger.Info("ddl", zap.String("query", "ALTER TABLE t ADD COLUMN v INT DEFAULT 7"))
		require.Equal(t, zap.Error(dbErr), fields[0], "logging must not mutate caller fields")
		require.Equal(t, fields[1].String, logs.All()[0].ContextMap()["query"])
		require.Equal(t, "ALTER TABLE t ADD COLUMN v INT DEFAULT 7", logs.All()[4].ContextMap()["query"])
		require.Equal(t, context.Canceled.Error(), logs.All()[3].ContextMap()["error"])
		require.Equal(t, "ordinary error", logs.All()[3].ContextMap()["error text"])
		for i, key := range []string{"error", "cause", "error"} {
			text := fmt.Sprint(logs.All()[i].ContextMap()[key])
			if enabled {
				require.NotContains(t, text, "private-value")
				require.Contains(t, text, "1062")
				require.Contains(t, text, "Duplicate entry")
			} else {
				require.Contains(t, text, "private-value")
			}
		}
	}
}
