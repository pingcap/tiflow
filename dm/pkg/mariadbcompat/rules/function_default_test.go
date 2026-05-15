// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package rules

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

// FunctionDefaultRule decision table:
//
//	+---------------------------------+--------------------+--------+
//	| DEFAULT expression              | Column type        | Action |
//	+---------------------------------+--------------------+--------+
//	| literal                         | any                | keep   |
//	| whitelisted time func           | time (TS/DT/DATE)  | keep   |
//	| whitelisted time func           | non-time           | strip  |
//	| (parenthesised) any expression  | any                | strip  |
//	| non-whitelisted func            | any                | strip  |
//	+---------------------------------+--------------------+--------+

func applyFunctionDefault(t *testing.T, sql string) *ast.ColumnDef {
	t.Helper()
	stmt := parseCreateTable(t, sql)
	col := stmt.Cols[0]
	rule := &FunctionDefaultRule{}
	if !rule.ShouldApply(col) {
		return col
	}
	out, err := rule.Apply(col)
	require.NoError(t, err)
	return out.(*ast.ColumnDef)
}

func TestFunctionDefault_LiteralKept(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (c VARCHAR(10) DEFAULT 'foo');`)
	require.True(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
}

func TestFunctionDefault_NullLiteralKept(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (c INT DEFAULT NULL);`)
	require.True(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
}

func TestFunctionDefault_TimeFuncOnTimeColumnKept(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`)
	require.True(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
}

func TestFunctionDefault_NowOnDatetimeKept(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (ts DATETIME DEFAULT NOW());`)
	require.True(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
}

func TestFunctionDefault_LocaltimestampOnTimestampKept(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (ts TIMESTAMP DEFAULT LOCALTIMESTAMP);`)
	require.True(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
}

func TestFunctionDefault_TimeFuncOnVarcharStripped(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (c VARCHAR(100) DEFAULT CURRENT_TIMESTAMP);`)
	require.False(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
}

func TestFunctionDefault_TimeFuncCallOnVarcharStripped(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (c VARCHAR(100) DEFAULT CURRENT_TIMESTAMP());`)
	require.False(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
}

func TestFunctionDefault_ParenthesisedExprStripped(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (c VARCHAR(100) DEFAULT (CURRENT_TIMESTAMP()));`)
	require.False(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
}

func TestFunctionDefault_UUIDFuncStripped(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (id CHAR(36) DEFAULT (UUID()));`)
	require.False(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
}

func TestFunctionDefault_RandFuncStripped(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (n DOUBLE DEFAULT (RAND()));`)
	require.False(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
}

func TestFunctionDefault_NonWhitelistedFuncOnTimeColumnStripped(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (ts TIMESTAMP DEFAULT FROM_UNIXTIME(0));`)
	require.False(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
}

func TestFunctionDefault_PreservesOtherOptions(t *testing.T) {
	col := applyFunctionDefault(t, `CREATE TABLE t (c VARCHAR(100) NOT NULL DEFAULT (CURRENT_TIMESTAMP()) COMMENT 'created');`)
	require.False(t, hasColOpt(col, ast.ColumnOptionDefaultValue))
	require.True(t, hasColOpt(col, ast.ColumnOptionNotNull))
	require.True(t, hasColOpt(col, ast.ColumnOptionComment))
}
