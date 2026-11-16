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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactSQL(t *testing.T) {
	previous := IsRedactLogEnabled()
	t.Cleanup(func() { SetRedactLog(previous) })
	cases := []struct {
		sql  string
		keep []string
	}{
		{"UPDATE `exchange`.`t` SET uid = ? WHERE id = ? LIMIT 1", []string{"UPDATE", "`exchange`.`t`", "uid", "id", "LIMIT 1"}},
		{"ALTER TABLE t ADD COLUMN email VARCHAR(100) DEFAULT 'private-value'", []string{"ALTER TABLE", "`t`", "VARCHAR(100)", "DEFAULT ?"}},
		{"CREATE TABLE t(id BIGINT, n DECIMAL(10,2) DEFAULT 87654321, email VARCHAR(100) COMMENT 'private-value') COMMENT='private-value'", []string{"BIGINT", "DECIMAL(10,2)", "VARCHAR(100)", "COMMENT"}},
		{"CREATE TABLE t(v ENUM('private-value', 'private-value2'), INDEX k(v) COMMENT 'private-value')", []string{"ENUM", "INDEX", "`k`"}},
		{"INSERT INTO t(email,n) VALUES ('private-value',87654321),('private-value2',87654321)", []string{"INSERT INTO", "VALUES"}},
		{"SELECT * FROM t WHERE email='private-value' AND n=87654321", []string{"SELECT", "FROM `t`", "WHERE"}},
		{"ALTER TABLE t ADD v VARBINARY(100) DEFAULT X'ABCDEF12'", []string{"VARBINARY(100)", "DEFAULT ?"}},
		{"SELECT 'private-value'; UPDATE t SET n=87654321", []string{"SELECT ?", "; UPDATE"}},
		{"SELECT /* private-value */ v FROM t", []string{"SELECT", "FROM `t`"}},
		{"SELECT /*+ SET_VAR(tidb_opt_enable_topn_to_cop='private-value') */ v FROM t", []string{"SELECT"}},
		{"CREATE VIEW v AS SELECT 'private-value'", []string{"create", "view"}},
		{"LOAD DATA INFILE '/private-value' INTO TABLE t", []string{"load", "data"}},
		{"ALTER TABLE t SET TIFLASH REPLICA 1 LOCATION LABELS 'private-value'", []string{"tiflash", "replica"}},
		{"ALTER DATABASE test SET TIFLASH REPLICA 1 LOCATION LABELS 'private-value'", []string{"tiflash", "replica"}},
		{"/*!50100 ALTER TABLE t ADD v VARCHAR(100) DEFAULT 'private-value' */", []string{"ALTER TABLE", "VARCHAR(100)"}},
		{"ALTER TABLE t UNKNOWN_SYNTAX 'private-value'", []string{"alter", "unknown_syntax"}},
		{"ALTER TABLE t ADD v TEXT DEFAULT 'private-value", []string{"alter", "table"}},
		{"SELECT 'private-value\\'private-value2'", []string{"SQL redacted"}},
	}
	for _, tc := range cases {
		SetRedactLog(true)
		got := RedactSQL(tc.sql)
		require.NotContains(t, got, "private-value")
		require.NotContains(t, got, "87654321")
		require.NotContains(t, got, "ABCDEF12")
		for _, keep := range tc.keep {
			require.Contains(t, strings.ToLower(got), strings.ToLower(keep), tc.sql)
		}
		SetRedactLog(false)
		require.Equal(t, tc.sql, RedactSQL(tc.sql))
	}
	require.Empty(t, RedactSQL(""))
}

func TestRedactSQLsDoesNotMutateInput(t *testing.T) {
	previous := IsRedactLogEnabled()
	t.Cleanup(func() { SetRedactLog(previous) })
	SetRedactLog(true)
	statements := []string{"INSERT INTO t(v) VALUES ('private-value')", "UPDATE t SET v='private-value2'"}
	before := append([]string(nil), statements...)
	redacted := RedactSQLs(statements)
	require.Equal(t, before, statements)
	require.NotContains(t, redacted[0], "private-value")
	require.NotContains(t, redacted[1], "private-value")
	SetRedactLog(false)
	require.Equal(t, statements, RedactSQLs(statements))
}

func TestRedactSQLKeepsParameterizedTemplate(t *testing.T) {
	previous := IsRedactLogEnabled()
	SetRedactLog(true)
	t.Cleanup(func() { SetRedactLog(previous) })
	sql := "UPDATE `exchange`.`t` SET `uid` = ? WHERE `id` = ? LIMIT 1"
	require.Equal(t, sql, RedactSQL(sql))
	require.Equal(t, "ALTER TABLE t ADD v VARCHAR(100)", RedactSQL("ALTER TABLE t ADD v VARCHAR(100)"))
}
