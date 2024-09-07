// Copyright 2024 PingCAP, Inc.
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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"go.uber.org/zap"
)

type visiter struct{}

func (f *visiter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch v := n.(type) {
	case *ast.ColumnDef:
		if v.Tp != nil {
			switch v.Tp.GetType() {
			case mysql.TypeTiDBVectorFloat32:
				v.Tp.SetType(mysql.TypeLongBlob)
				v.Tp.SetCharset("")
				v.Tp.SetCollate("")
				v.Tp.SetFlen(-1)
				v.Options = []*ast.ColumnOption{} // clear COMMENT
			}
		}
	}
	return n, false
}

func (f *visiter) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

func formatQuery(sql string) string {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		log.Error("format query parse one stmt failed", zap.Error(err))
	}
	stmt.Accept(&visiter{})

	buf := new(bytes.Buffer)
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
	if err = stmt.Restore(restoreCtx); err != nil {
		log.Error("format query restore failed", zap.Error(err))
	}
	return buf.String()
}
