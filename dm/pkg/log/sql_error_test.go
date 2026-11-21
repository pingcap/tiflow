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
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRedactSQLDiagnostic(t *testing.T) {
	previous := IsRedactLogEnabled()
	t.Cleanup(func() { SetRedactLog(previous) })
	message := "execute statement failed: ALTER TABLE t ADD v VARCHAR(100) DEFAULT 'private-value'"
	for _, cause := range []string{
		"Error 1064 (42000): syntax error near 'private-value'",
		"Error 1105: unsupported add column DEFAULT 'private-value'",
		"line 1 column 42 near private-value",
		"unstructured parser diagnostic private-value",
	} {
		SetRedactLog(true)
		gotMessage, gotCause := RedactSQLDiagnostic(int32(terror.ErrDBExecuteFailed.Code()), message, cause)
		require.NotContains(t, gotMessage, "private-value")
		require.NotContains(t, gotCause, "private-value")
		require.Contains(t, gotMessage, "execute statement failed:")
		require.Contains(t, gotMessage, "ALTER TABLE")
		require.Contains(t, gotMessage, "VARCHAR(100)")
		if cause[0:5] == "Error" {
			require.Contains(t, gotCause, "MySQL error")
		}
		SetRedactLog(false)
		gotMessage, gotCause = RedactSQLDiagnostic(int32(terror.ErrDBExecuteFailed.Code()), message, cause)
		require.Equal(t, message, gotMessage)
		require.Equal(t, cause, gotCause)
	}
	SetRedactLog(true)
	gotMessage, gotCause := RedactSQLDiagnostic(0, "ordinary error", "connection failed")
	require.Equal(t, "ordinary error", gotMessage)
	require.Equal(t, "connection failed", gotCause)
}

func TestSQLWarningField(t *testing.T) {
	previous := IsRedactLogEnabled()
	t.Cleanup(func() { SetRedactLog(previous) })
	warning := errors.Normalize("Truncated incorrect DOUBLE value: '%s'", errors.MySQLErrorCode(1292)).GenWithStackByArgs("private-value")
	for _, enabled := range []bool{false, true} {
		SetRedactLog(enabled)
		core, logs := observer.New(zap.WarnLevel)
		zap.New(core).Warn("parse statement", SQLWarningField("warnings", []error{warning}))
		output := fmt.Sprint(logs.All()[0].ContextMap()["warnings"])
		if enabled {
			require.NotContains(t, output, "private-value")
			require.Contains(t, output, "1292")
			require.Contains(t, output, "Truncated incorrect")
		} else {
			require.Contains(t, output, "private-value")
		}
	}
}

func TestWrappedSQLParserError(t *testing.T) {
	previous := IsRedactLogEnabled()
	SetRedactLog(true)
	t.Cleanup(func() { SetRedactLog(previous) })
	cause := errors.New("line 1 column 19 near private-value")
	inner := terror.ErrParseSQL.Delegate(cause, "ALTER TABLE t ADD v TEXT DEFAULT 'private-value'")
	outer := terror.ErrNotSet.Delegate(errors.Annotate(inner, "outer context"))
	got := RedactDBError(outer)
	require.NotContains(t, got, "private-value")
	require.Contains(t, got, "ALTER TABLE")
	require.Contains(t, got, "line 1 column 19")
	err := terror.ErrSyncerUnitParseStmt.New("line 1 column 19 near private-value")
	got = RedactDBError(err)
	require.NotContains(t, got, "private-value")
	require.Contains(t, got, "SQL parsing failed")
	require.Contains(t, got, "line 1 column 19")
	require.Contains(t, err.Error(), "private-value", "the original error stays available for internal decisions")
}

func TestRawDriverSQLError(t *testing.T) {
	previous := IsRedactLogEnabled()
	t.Cleanup(func() { SetRedactLog(previous) })
	for _, code := range []uint16{1064, 1149, 1235, 1105} {
		err := &mysql.MySQLError{Number: code, Message: "unsupported SQL near 'private-value'"}
		SetRedactLog(true)
		got := RedactDBError(err)
		require.NotContains(t, got, "private-value")
		require.Contains(t, got, fmt.Sprint(code))
		require.Contains(t, err.Error(), "private-value")
		SetRedactLog(false)
		require.Equal(t, err.Error(), RedactDBError(err))
	}
}
