// Copyright 2020 PingCAP, Inc.
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

package main

import (
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
)

const (
	tableType = "BASE TABLE"
)

// createTableToSmithSchema converts a `CREATE TABLE` statement to a schema representation needed by go-sqlsmith.
// for one column in `columns`:
//   - columns[0]: DB name
//   - columns[1]: table name
//   - columns[2]: table type (only use `BASE TABLE` now)
//   - columns[3]: column name
//   - columns[4]: column type
//
// for `indexes`:
//   - list of index name
func createTableToSmithSchema(dbName, sql string) (columns [][5]string, indexes []string, err error) {
	parser2 := parser.New() // we should have clear `SQL_MODE.
	stmt, err := parser2.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, nil, err
	}

	createStmt := stmt.(*ast.CreateTableStmt)
	for _, col := range createStmt.Cols {
		columns = append(columns, [5]string{
			dbName,
			createStmt.Table.Name.O,
			tableType,
			col.Name.Name.O,
			col.Tp.String(),
		})
	}

	for _, cons := range createStmt.Constraints {
		switch cons.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			indexes = append(indexes, cons.Name)
		default:
			continue
		}
	}
	return
}
