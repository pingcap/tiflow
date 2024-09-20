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

package schema

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
)

type currentDBSetter struct {
	currentDB string
}

func (c currentDBSetter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	// mimic preprocessor in TiDB
	switch v := n.(type) {
	case *ast.CreateTableStmt:
		for _, val := range v.Constraints {
			if val.Refer != nil && val.Refer.Table.Schema.String() == "" {
				val.Refer.Table.Schema = v.Table.Schema
			}
		}
	case *ast.AlterTableStmt:
		for _, spec := range v.Specs {
			if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint.Refer != nil {
				table := spec.Constraint.Refer.Table
				if table.Schema.L == "" && v.Table.Schema.L != "" {
					table.Schema = pmodel.NewCIStr(v.Table.Schema.L)
				}
			}
		}
	}
	return n, false
}

func (c currentDBSetter) Leave(n ast.Node) (node ast.Node, ok bool) {
	v, ok := n.(*ast.TableName)
	if !ok {
		return n, true
	}
	if v.Schema.O == "" {
		v.Schema = pmodel.NewCIStr(c.currentDB)
	}
	return n, true
}
