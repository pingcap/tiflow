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

package sqlgen

import (
	"strings"

	"github.com/chaos-mesh/go-sqlsmith/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // import this to make the parser work
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/simulator/config"
	"github.com/pingcap/tiflow/dm/simulator/mcp"
	"go.uber.org/zap"
)

type sqlGeneratorImpl struct {
	tableConfig *config.TableConfig
	columnMap   map[string]*config.ColumnDefinition
	ukMap       map[string]struct{}
}

// NewSQLGeneratorImpl generates a new implementation object for SQL generator.
func NewSQLGeneratorImpl(tableConfig *config.TableConfig) *sqlGeneratorImpl {
	colDefMap := make(map[string]*config.ColumnDefinition)
	for _, colDef := range tableConfig.Columns {
		colDefMap[colDef.ColumnName] = colDef
	}
	ukMap := make(map[string]struct{})
	for _, ukColName := range tableConfig.UniqueKeyColumnNames {
		if _, ok := colDefMap[ukColName]; ok {
			ukMap[ukColName] = struct{}{}
		}
	}
	return &sqlGeneratorImpl{
		tableConfig: tableConfig,
		columnMap:   colDefMap,
		ukMap:       ukMap,
	}
}

// outputString parses an ast node to SQL string.
func outputString(node ast.Node) (string, error) {
	var sb strings.Builder
	err := node.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	if err != nil {
		return "", errors.Annotate(err, "restore AST into SQL string error")
	}
	return sb.String(), nil
}

// GenTruncateTable generates a TRUNCATE TABLE SQL.
// It implements the SQLGenerator interface.
func (g *sqlGeneratorImpl) GenTruncateTable() (string, error) {
	truncateTree := &ast.TruncateTableStmt{
		Table: &ast.TableName{
			Schema: pmodel.NewCIStr(g.tableConfig.DatabaseName),
			Name:   pmodel.NewCIStr(g.tableConfig.TableName),
		},
	}
	return outputString(truncateTree)
}

func (g *sqlGeneratorImpl) generateWhereClause(theUK map[string]interface{}) (ast.ExprNode, error) {
	compareExprs := make([]ast.ExprNode, 0)
	// iterate the existing UKs, to make sure all the uk columns has values
	for ukColName := range g.ukMap {
		val, ok := theUK[ukColName]
		if !ok {
			log.L().Error(ErrUKColValueNotProvided.Error(), zap.String("column_name", ukColName))
			return nil, errors.Trace(ErrUKColValueNotProvided)
		}
		var compareExpr ast.ExprNode
		if val == nil {
			compareExpr = &ast.IsNullExpr{
				Expr: &ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Name: pmodel.NewCIStr(ukColName),
					},
				},
			}
		} else {
			compareExpr = &ast.BinaryOperationExpr{
				Op: opcode.EQ,
				L: &ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Name: pmodel.NewCIStr(ukColName),
					},
				},
				R: ast.NewValueExpr(val, "", ""),
			}
		}
		compareExprs = append(compareExprs, compareExpr)
	}
	resultExpr := generateCompoundBinaryOpExpr(compareExprs)
	if resultExpr == nil {
		return nil, ErrWhereFiltersEmpty
	}
	return resultExpr, nil
}

func generateCompoundBinaryOpExpr(compExprs []ast.ExprNode) ast.ExprNode {
	switch len(compExprs) {
	case 0:
		return nil
	case 1:
		return compExprs[0]
	default:
		return &ast.BinaryOperationExpr{
			Op: opcode.LogicAnd,
			L:  compExprs[0],
			R:  generateCompoundBinaryOpExpr(compExprs[1:]),
		}
	}
}

// GenUpdateRow generates an UPDATE SQL for the given unique key.
// It implements the SQLGenerator interface.
func (g *sqlGeneratorImpl) GenUpdateRow(theUK *mcp.UniqueKey) (string, error) {
	if theUK == nil {
		return "", errors.Trace(ErrMissingUKValue)
	}
	assignments := make([]*ast.Assignment, 0)
	for _, colInfo := range g.columnMap {
		if _, ok := g.ukMap[colInfo.ColumnName]; ok {
			// this is a UK column, skip from modifying it
			// TODO: support UK modification in the future
			continue
		}
		assignments = append(assignments, &ast.Assignment{
			Column: &ast.ColumnName{
				Name: pmodel.NewCIStr(colInfo.ColumnName),
			},
			Expr: ast.NewValueExpr(util.GenerateDataItem(colInfo.DataType), "", ""),
		})
	}
	whereClause, err := g.generateWhereClause(theUK.GetValue())
	if err != nil {
		return "", errors.Annotate(err, "generate where clause error")
	}
	updateTree := &ast.UpdateStmt{
		List: assignments,
		TableRefs: &ast.TableRefsClause{
			TableRefs: &ast.Join{
				Left: &ast.TableName{
					Schema: pmodel.NewCIStr(g.tableConfig.DatabaseName),
					Name:   pmodel.NewCIStr(g.tableConfig.TableName),
				},
			},
		},
		Where: whereClause,
	}
	return outputString(updateTree)
}

// GenInsertRow generates an INSERT SQL.
// It implements the SQLGenerator interface.
// The new row's unique key is also provided,
// so that it can be further added into an MCP.
func (g *sqlGeneratorImpl) GenInsertRow() (string, *mcp.UniqueKey, error) {
	ukValues := make(map[string]interface{})
	columnNames := []*ast.ColumnName{}
	values := []ast.ExprNode{}
	for _, col := range g.columnMap {
		columnNames = append(columnNames, &ast.ColumnName{
			Name: pmodel.NewCIStr(col.ColumnName),
		})
		newValue := util.GenerateDataItem(col.DataType)
		values = append(values, ast.NewValueExpr(newValue, "", ""))
		if _, ok := g.ukMap[col.ColumnName]; ok {
			// add UK value
			ukValues[col.ColumnName] = newValue
		}
	}
	insertTree := &ast.InsertStmt{
		Table: &ast.TableRefsClause{
			TableRefs: &ast.Join{
				Left: &ast.TableName{
					Schema: pmodel.NewCIStr(g.tableConfig.DatabaseName),
					Name:   pmodel.NewCIStr(g.tableConfig.TableName),
				},
			},
		},
		Lists:   [][]ast.ExprNode{values},
		Columns: columnNames,
	}
	sql, err := outputString(insertTree)
	if err != nil {
		return "", nil, errors.Annotate(err, "output INSERT AST into SQL string error")
	}
	return sql, mcp.NewUniqueKey(-1, ukValues), nil
}

// GenDeleteRow generates a DELETE SQL for the given unique key.
// It implements the SQLGenerator interface.
func (g *sqlGeneratorImpl) GenDeleteRow(theUK *mcp.UniqueKey) (string, error) {
	if theUK == nil {
		return "", errors.Trace(ErrMissingUKValue)
	}
	whereClause, err := g.generateWhereClause(theUK.GetValue())
	if err != nil {
		return "", errors.Annotate(err, "generate where clause error")
	}
	updateTree := &ast.DeleteStmt{
		TableRefs: &ast.TableRefsClause{
			TableRefs: &ast.Join{
				Left: &ast.TableName{
					Schema: pmodel.NewCIStr(g.tableConfig.DatabaseName),
					Name:   pmodel.NewCIStr(g.tableConfig.TableName),
				},
			},
		},
		Where: whereClause,
	}
	return outputString(updateTree)
}

// GenLoadUniqueKeySQL generates a SELECT SQL fetching all the uniques of a table.
// It implements the SQLGenerator interface.
// The column definitions of the returned data is also provided,
// so that the values can be stored to different variables.
func (g *sqlGeneratorImpl) GenLoadUniqueKeySQL() (string, []*config.ColumnDefinition, error) {
	selectFields := make([]*ast.SelectField, 0)
	cols := make([]*config.ColumnDefinition, 0)
	for ukColName := range g.ukMap {
		selectFields = append(selectFields, &ast.SelectField{
			Expr: &ast.ColumnNameExpr{
				Name: &ast.ColumnName{
					Name: pmodel.NewCIStr(ukColName),
				},
			},
		})
		cols = append(cols, g.columnMap[ukColName])
	}
	selectTree := &ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{
			Fields: selectFields,
		},
		From: &ast.TableRefsClause{
			TableRefs: &ast.Join{
				Left: &ast.TableName{
					Schema: pmodel.NewCIStr(g.tableConfig.DatabaseName),
					Name:   pmodel.NewCIStr(g.tableConfig.TableName),
				},
			},
		},
	}
	sql, err := outputString(selectTree)
	if err != nil {
		return "", nil, errors.Annotate(err, "output SELECT AST into SQL string error")
	}
	return sql, cols, nil
}

func (g *sqlGeneratorImpl) GenCreateTable() string {
	return g.tableConfig.GenCreateTable()
}
