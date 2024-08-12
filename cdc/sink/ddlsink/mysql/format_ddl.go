// Copyright 2022 PingCAP, Inc.
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

package mysql

import (
	"bytes"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
)

type fingerprintVisitor struct{}

func (f *fingerprintVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	if v, ok := n.(*ast.ColumnNameExpr); ok {
		if v.Name.OrigColName() == "VECTOR" {
			v.SetType(types.NewFieldType(mysql.TypeVarString))
		}
	}
	return n, false
}

func (f *fingerprintVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

func formatQuery(sql string) string {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {

	}
	stmt.Accept(&fingerprintVisitor{})

	buf := new(bytes.Buffer)
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
	err = stmt.Restore(restoreCtx)
	if nil != err {
	}
	return buf.String()
}
