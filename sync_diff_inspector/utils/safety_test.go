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

package utils

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/stretchr/testify/require"
)

func TestFixSQLAnnotationCannotEndComment(t *testing.T) {
	table, err := GetTableInfoBySQL("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(1000))", parser.New())
	require.NoError(t, err)
	for _, payload := range []string{"x/*/; SELECT 42; */*", "/*/", "*/*", "/*/*/", "*/*/*", "x*/; DROP DATABASE prod; /*", "x\n*/; DROP DATABASE prod; /*", "/*! DROP DATABASE prod */", `x\'; DROP DATABASE prod; -- `} {
		t.Run(payload, func(t *testing.T) {
			source := map[string]*dbutil.ColumnData{"id": {Data: []byte("1")}, "v": {Data: []byte(payload)}}
			target := map[string]*dbutil.ColumnData{"id": {Data: []byte("1")}, "v": {Data: []byte("old")}}
			for _, reverse := range []bool{false, true} {
				if reverse {
					source, target = target, source
				}
				sql := GenerateReplaceDMLWithAnnotation(source, target, table, "test")
				stmts, _, err := parser.New().ParseSQL(sql)
				require.NoError(t, err, sql)
				require.Len(t, stmts, 1, sql)
				insert, ok := stmts[0].(*ast.InsertStmt)
				require.True(t, ok, sql)
				require.True(t, insert.IsReplace)
				value, ok := insert.Lists[0][1].(ast.ValueExpr)
				require.True(t, ok)
				require.Equal(t, string(source["v"].Data), value.GetString())
			}
		})
	}
}
