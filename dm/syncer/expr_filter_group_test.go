// Copyright 2021 PingCAP, Inc.
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

package syncer

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	ddl2 "github.com/pingcap/tidb/pkg/ddl"
	context2 "github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/config"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestSkipDMLByExpression(t *testing.T) {
	cases := []struct {
		exprStr    string
		tableStr   string
		skippedRow []any
		passedRow  []any
	}{
		{
			"state != 1",
			`
create table t (
	primary_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
	id bigint(20) unsigned NOT NULL,
	state tinyint(3) unsigned NOT NULL,
	PRIMARY KEY (primary_id),
	UNIQUE KEY uniq_id (id),
	KEY idx_state (state)
);`,
			[]any{100, 100, 3},
			[]any{100, 100, 1},
		},
		{
			"f > 1.23",
			`
create table t (
	f float
);`,
			[]any{float32(2.0)},
			[]any{float32(1.0)},
		},
		{
			"f > a + b",
			`
create table t (
	f float,
	a int,
	b int
);`,
			[]any{float32(123.45), 1, 2},
			[]any{float32(0.01), 23, 45},
		},
		{
			"id = 30",
			`
create table t (
	id int(11) NOT NULL AUTO_INCREMENT,
	name varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
	dt datetime DEFAULT NULL,
	ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (id)
);`,
			[]any{int32(30), "30", nil, "2021-06-17 10:13:05"},
			[]any{int32(20), "20", nil, "2021-06-17 10:13:05"},
		},
	}

	var (
		ctx     = context.Background()
		dbName  = "test"
		tblName = "t"
		table   = &filter.Table{
			Schema: dbName,
			Name:   tblName,
		}
	)
	require.NoError(t, log.InitLogger(&log.Config{Level: "debug"}))

	for _, ca := range cases {
		schemaTracker, err := schema.NewTestTracker(ctx, "unit-test", nil, log.L())
		require.NoError(t, err)
		require.NoError(t, schemaTracker.CreateSchemaIfNotExists(dbName))
		stmt, err := parseSQL(ca.tableStr)
		require.NoError(t, err)
		require.NoError(t, schemaTracker.Exec(ctx, dbName, stmt))

		ti, err := schemaTracker.GetTableInfo(table)
		require.NoError(t, err)

		exprConfig := []*config.ExpressionFilter{
			{
				Schema:          dbName,
				Table:           tblName,
				InsertValueExpr: ca.exprStr,
			},
		}
		sessCtx := utils.NewSessionCtx(map[string]string{"time_zone": "UTC"})
		g := NewExprFilterGroup(tcontext.Background(), sessCtx, exprConfig)
		exprs, err := g.GetInsertExprs(table, ti)
		require.NoError(t, err)
		require.Len(t, exprs, 1)
		expr := exprs[0]

		ca.skippedRow = util.Must(adjustValueFromBinlogData(ca.skippedRow, ti))
		ca.passedRow = util.Must(adjustValueFromBinlogData(ca.passedRow, ti))

		skip, err := SkipDMLByExpression(sessCtx, ca.skippedRow, expr, ti.Columns)
		require.NoError(t, err)
		require.True(t, skip)

		skip, err = SkipDMLByExpression(sessCtx, ca.passedRow, expr, ti.Columns)
		require.NoError(t, err)
		require.False(t, skip)

		schemaTracker.Close()
	}
}

func TestAllBinaryProtocolTypes(t *testing.T) {
	cases := []struct {
		exprStr    string
		tableStr   string
		skippedRow []any
		passedRow  []any
	}{
		// MYSQL_TYPE_NULL
		{
			"c IS NULL",
			`
create table t (
	c int
);`,
			[]any{nil},
			[]any{100},
		},
		// MYSQL_TYPE_LONG
		{
			"c = 1",
			`
create table t (
	c int
);`,
			[]any{int32(1)},
			[]any{int32(100)},
		},
		// MYSQL_TYPE_TINY
		{
			"c = 2",
			`
create table t (
	c tinyint
);`,
			[]any{int8(2)},
			[]any{int8(-1)},
		},
		// MYSQL_TYPE_SHORT
		{
			"c < 10",
			`
create table t (
	c smallint
);`,
			[]any{int16(8)},
			[]any{int16(18)},
		},
		// MYSQL_TYPE_INT24
		{
			"c < 0",
			`
create table t (
	c mediumint
);`,
			[]any{int32(-8)},
			[]any{int32(1)},
		},
		// MYSQL_TYPE_LONGLONG
		{
			"c = 100000000",
			`
create table t (
	c bigint
);`,
			[]any{int64(100000000)},
			[]any{int64(200000000)},
		},
		// MYSQL_TYPE_NEWDECIMAL
		{
			"c = 10.1",
			`
create table t (
	c decimal(5,2)
);`,
			[]any{"10.10"},
			[]any{"10.11"},
		},
		// MYSQL_TYPE_FLOAT
		{
			"c < 0.1",
			`
create table t (
	c float
);`,
			[]any{float32(0.08)},
			[]any{float32(0.18)},
		},
		// MYSQL_TYPE_DOUBLE
		{
			"c < 0.1",
			`
create table t (
	c double
);`,
			[]any{float64(0.08)},
			[]any{float64(0.18)},
		},
		// MYSQL_TYPE_BIT
		{
			"c = b'1'",
			`
create table t (
	c bit(4)
);`,
			[]any{int64(1)},
			[]any{int64(2)},
		},
		// MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_TIMESTAMP2
		// DM does not set ParseTime
		// TODO: use upstream timezone later
		{
			"c = '2021-06-21 12:34:56'",
			`
create table t (
	c timestamp
);`,
			[]any{"2021-06-21 12:34:56"},
			[]any{"1970-01-01 00:00:01"},
		},
		// MYSQL_TYPE_DATETIME, MYSQL_TYPE_DATETIME2
		{
			"c = '2021-06-21 00:00:12'",
			`
create table t (
	c datetime
);`,
			[]any{"2021-06-21 00:00:12"},
			[]any{"1970-01-01 00:00:01"},
		},
		// MYSQL_TYPE_TIME, MYSQL_TYPE_TIME2
		{
			"c = '00:00:12'",
			`
create table t (
	c time(6)
);`,
			[]any{"00:00:12"},
			[]any{"00:00:01"},
		},
		// MYSQL_TYPE_DATE
		{
			"c = '2021-06-21'",
			`
create table t (
	c date
);`,
			[]any{"2021-06-21"},
			[]any{"1970-01-01"},
		},
		// MYSQL_TYPE_YEAR
		{
			"c = '2021'",
			`
create table t (
	c year
);`,
			[]any{int(2021)},
			[]any{int(2020)},
		},
		// MYSQL_TYPE_ENUM
		{
			"c = 'x-small'",
			`
create table t (
	c ENUM('x-small', 'small', 'medium', 'large', 'x-large')
);`,
			[]any{int64(1)}, // 1-indexed
			[]any{int64(2)},
		},
		// MYSQL_TYPE_SET
		{
			"find_in_set('c', c) > 0",
			`
create table t (
	c SET('a', 'b', 'c', 'd')
);`,
			[]any{int64(0b1100)}, // c,d
			[]any{int64(0b1000)}, // d
		},
		// MYSQL_TYPE_BLOB
		{
			"c = x'1234'",
			`
create table t (
	c blob
);`,
			[]any{[]byte("\x124")}, // x'1234'
			[]any{[]byte("Vx")},    // x'5678'
		},
		// MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING
		{
			"c = 'abc'",
			`
create table t (
	c varchar(20)
);`,
			[]any{"abc"},
			[]any{"def"},
		},
		// MYSQL_TYPE_JSON
		{
			`c->"$.id" = 1`,
			`
create table t (
	c json
);`,
			[]any{[]byte(`{"id": 1}`)},
			[]any{[]byte(`{"id": 2}`)},
		},
		// MYSQL_TYPE_GEOMETRY, parser not supported
	}

	var (
		ctx     = context.Background()
		dbName  = "test"
		tblName = "t"
		table   = &filter.Table{
			Schema: dbName,
			Name:   tblName,
		}
	)
	require.NoError(t, log.InitLogger(&log.Config{Level: "debug"}))

	for _, ca := range cases {
		t.Log(ca.tableStr)
		schemaTracker, err := schema.NewTestTracker(ctx, "unit-test", nil, log.L())
		require.NoError(t, err)
		require.NoError(t, schemaTracker.CreateSchemaIfNotExists(dbName))
		stmt, err := parseSQL(ca.tableStr)
		require.NoError(t, err)
		require.NoError(t, schemaTracker.Exec(ctx, dbName, stmt))

		ti, err := schemaTracker.GetTableInfo(table)
		require.NoError(t, err)

		exprConfig := []*config.ExpressionFilter{
			{
				Schema:          dbName,
				Table:           tblName,
				InsertValueExpr: ca.exprStr,
			},
		}
		sessCtx := utils.NewSessionCtx(map[string]string{"time_zone": "UTC"})
		g := NewExprFilterGroup(tcontext.Background(), sessCtx, exprConfig)
		exprs, err := g.GetInsertExprs(table, ti)
		require.NoError(t, err)
		require.Len(t, exprs, 1)
		expr := exprs[0]

		ca.skippedRow = util.Must(adjustValueFromBinlogData(ca.skippedRow, ti))
		ca.passedRow = util.Must(adjustValueFromBinlogData(ca.passedRow, ti))

		skip, err := SkipDMLByExpression(sessCtx, ca.skippedRow, expr, ti.Columns)
		require.NoError(t, err)
		require.True(t, skip)

		skip, err = SkipDMLByExpression(sessCtx, ca.passedRow, expr, ti.Columns)
		require.NoError(t, err)
		require.False(t, skip)

		schemaTracker.Close()
	}
}

func TestExpressionContainsNonExistColumn(t *testing.T) {
	var (
		ctx     = context.Background()
		dbName  = "test"
		tblName = "t"
		table   = &filter.Table{
			Schema: dbName,
			Name:   tblName,
		}
		tableStr = `
create table t (
	c varchar(20)
);`
		exprStr = "d > 1"
	)

	schemaTracker, err := schema.NewTestTracker(ctx, "unit-test", nil, log.L())
	require.NoError(t, err)
	require.NoError(t, schemaTracker.CreateSchemaIfNotExists(dbName))
	stmt, err := parseSQL(tableStr)
	require.NoError(t, err)
	require.NoError(t, schemaTracker.Exec(ctx, dbName, stmt))

	ti, err := schemaTracker.GetTableInfo(table)
	require.NoError(t, err)

	exprConfig := []*config.ExpressionFilter{
		{
			Schema:          dbName,
			Table:           tblName,
			InsertValueExpr: exprStr,
		},
	}
	sessCtx := utils.NewSessionCtx(map[string]string{"time_zone": "UTC"})
	g := NewExprFilterGroup(tcontext.Background(), sessCtx, exprConfig)
	exprs, err := g.GetInsertExprs(table, ti)
	require.NoError(t, err)
	require.Len(t, exprs, 1)
	expr := exprs[0]
	require.Equal(t, "0", expr.StringWithCtx(context2.EmptyParamValues, errors.RedactLogDisable))

	// skip nothing
	skip, err := SkipDMLByExpression(sessCtx, []any{0}, expr, ti.Columns)
	require.NoError(t, err)
	require.False(t, skip)
	skip, err = SkipDMLByExpression(sessCtx, []any{2}, expr, ti.Columns)
	require.NoError(t, err)
	require.False(t, skip)
}

func TestGetUpdateExprsSameLength(t *testing.T) {
	var (
		dbName  = "test"
		tblName = "t"
		table   = &filter.Table{
			Schema: dbName,
			Name:   tblName,
		}
		tableStr = `
create table t (
	c varchar(20)
);`
		exprStr = "c > 1"
		sessCtx = utils.NewSessionCtx(map[string]string{"time_zone": "UTC"})
	)

	cases := []*config.ExpressionFilter{
		{
			Schema:          dbName,
			Table:           tblName,
			InsertValueExpr: exprStr,
		},
		{
			Schema:             dbName,
			Table:              tblName,
			UpdateOldValueExpr: exprStr,
		},
		{
			Schema:             dbName,
			Table:              tblName,
			UpdateNewValueExpr: exprStr,
		},
		{
			Schema:             dbName,
			Table:              tblName,
			UpdateOldValueExpr: exprStr,
			UpdateNewValueExpr: exprStr,
		},
	}

	stmt, err := parseSQL(tableStr)
	require.NoError(t, err)
	tableInfo, err := ddl2.BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)

	for i, c := range cases {
		t.Logf("case #%d", i)
		g := NewExprFilterGroup(tcontext.Background(), sessCtx, []*config.ExpressionFilter{c})
		oldExprs, newExprs, err2 := g.GetUpdateExprs(table, tableInfo)
		require.NoError(t, err2)
		require.Equal(t, len(oldExprs), len(newExprs))
	}
	g := NewExprFilterGroup(tcontext.Background(), sessCtx, cases)
	oldExprs, newExprs, err := g.GetUpdateExprs(table, tableInfo)
	require.NoError(t, err)
	require.Equal(t, len(oldExprs), len(newExprs))
	require.Len(t, oldExprs, 3)
}
