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

package sqlmodel

import (
	"strings"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/opcode"
	driver "github.com/pingcap/tidb/types/parser_driver"

	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/quotes"
)

// SameTypeTargetAndColumns check whether two row changes have same type, target
// and columns, so they can be merged to a multi-value DML.
func SameTypeTargetAndColumns(lhs *RowChange, rhs *RowChange) bool {
	if lhs.tp != rhs.tp {
		return false
	}
	if lhs.sourceTable.Schema == rhs.sourceTable.Schema &&
		lhs.sourceTable.Table == rhs.sourceTable.Table {
		return true
	}
	if lhs.targetTable.Schema != rhs.targetTable.Schema ||
		lhs.targetTable.Table != rhs.targetTable.Table {
		return false
	}

	// when the targets are the same and the sources are not the same (same
	// group of shard tables), this piece of code is run.
	var lhsCols, rhsCols []string
	switch lhs.tp {
	case RowChangeDelete:
		lhsCols, _ = lhs.whereColumnsAndValues()
		rhsCols, _ = rhs.whereColumnsAndValues()
	case RowChangeUpdate:
		// not supported yet
		return false
	case RowChangeInsert:
		for _, col := range lhs.sourceTableInfo.Columns {
			lhsCols = append(lhsCols, col.Name.L)
		}
		for _, col := range rhs.sourceTableInfo.Columns {
			rhsCols = append(rhsCols, col.Name.L)
		}
	}

	if len(lhsCols) != len(rhsCols) {
		return false
	}
	for i := 0; i < len(lhsCols); i++ {
		if lhsCols[i] != rhsCols[i] {
			return false
		}
	}
	return true
}

// GenDeleteSQL generates the DELETE SQL and its arguments.
// Input `changes` should have same target table and same columns for WHERE
// (typically same PK/NOT NULL UK), otherwise the behaviour is undefined.
func GenDeleteSQL(changes ...*RowChange) (string, []interface{}) {
	if len(changes) == 0 {
		log.L().DPanic("row changes is empty")
		return "", nil
	}

	first := changes[0]

	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(first.targetTable.QuoteString())
	buf.WriteString(" WHERE (")

	whereColumns, _ := first.whereColumnsAndValues()
	for i, column := range whereColumns {
		if i != len(whereColumns)-1 {
			buf.WriteString(quotes.QuoteName(column) + ",")
		} else {
			buf.WriteString(quotes.QuoteName(column) + ")")
		}
	}
	buf.WriteString(" IN (")
	// TODO: can't handle NULL by IS NULL, should use WHERE OR
	args := make([]interface{}, 0, len(changes)*len(whereColumns))
	holder := valuesHolder(len(whereColumns))
	for i, change := range changes {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(holder)
		_, whereValues := change.whereColumnsAndValues()
		// a simple check about different number of WHERE values, not trying to
		// cover all cases
		if len(whereValues) != len(whereColumns) {
			log.L().DPanic("len(whereValues) != len(whereColumns)",
				zap.Int("len(whereValues)", len(whereValues)),
				zap.Int("len(whereColumns)", len(whereColumns)),
				zap.Any("whereValues", whereValues),
				zap.Stringer("sourceTable", change.sourceTable))
			return "", nil
		}
		args = append(args, whereValues...)
	}
	buf.WriteString(")")
	return buf.String(), args
}

// TODO: support GenUpdateSQL(changes ...*RowChange) using UPDATE SET CASE WHEN

// GenInsertSQL generates the INSERT SQL and its arguments.
// Input `changes` should have same target table and same modifiable columns,
// otherwise the behaviour is undefined.
func GenInsertSQL(tp DMLType, changes ...*RowChange) (string, []interface{}) {
	if len(changes) == 0 {
		log.L().DPanic("row changes is empty")
		return "", nil
	}

	first := changes[0]

	var buf strings.Builder
	buf.Grow(1024)
	if tp == DMLReplace {
		buf.WriteString("REPLACE INTO ")
	} else {
		buf.WriteString("INSERT INTO ")
	}
	buf.WriteString(first.targetTable.QuoteString())
	buf.WriteString(" (")
	columnNum := 0
	var skipColIdx []int
	for i, col := range first.sourceTableInfo.Columns {
		if isGenerated(first.targetTableInfo.Columns, col.Name) {
			skipColIdx = append(skipColIdx, i)
			continue
		}

		if columnNum != 0 {
			buf.WriteByte(',')
		}
		columnNum++
		buf.WriteString(quotes.QuoteName(col.Name.O))
	}
	buf.WriteString(") VALUES ")
	holder := valuesHolder(columnNum)
	for i := range changes {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(holder)
	}
	if tp == DMLInsertOnDuplicateUpdate {
		buf.WriteString(" ON DUPLICATE KEY UPDATE ")
		i := 0 // used as index of skipColIdx
		writtenFirstCol := false

		for j, col := range first.sourceTableInfo.Columns {
			if i < len(skipColIdx) && skipColIdx[i] == j {
				i++
				continue
			}

			if writtenFirstCol {
				buf.WriteByte(',')
			}
			writtenFirstCol = true

			colName := quotes.QuoteName(col.Name.O)
			buf.WriteString(colName + "=VALUES(" + colName + ")")
		}
	}

	args := make([]interface{}, 0, len(changes)*(len(first.sourceTableInfo.Columns)-len(skipColIdx)))
	for _, change := range changes {
		i := 0 // used as index of skipColIdx
		for j, val := range change.postValues {
			if i >= len(skipColIdx) {
				args = append(args, change.postValues[j:]...)
				break
			}
			if skipColIdx[i] == j {
				i++
				continue
			}
			args = append(args, val)
		}
	}
	return buf.String(), args
}

// GenUpdateSQL generates the UPDATE SQL and its arguments.
// Input `changes` should have same target table and same columns for WHERE
// (typically same PK/NOT NULL UK), otherwise the behaviour is undefined.
// Compared to GenInsertSQL with DMLInsertOnDuplicateUpdate, this function is
// slower and more complex, we should only use it when PK/UK is updated.
func GenUpdateSQL(changes ...*RowChange) (string, []interface{}) {
	if len(changes) == 0 {
		log.L().DPanic("row changes is empty")
		return "", nil
	}

	stmt := &ast.UpdateStmt{}
	first := changes[0]

	// handle UPDATE db.tbl ...

	t := &ast.TableName{
		Schema: model.NewCIStr(first.targetTable.Schema),
		Name:   model.NewCIStr(first.targetTable.Table),
	}
	stmt.TableRefs = &ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{Source: t}}}

	// handle ... SET col... , col2... , ...

	stmt.List = make([]*ast.Assignment, 0, len(first.sourceTableInfo.Columns))
	var skipColIdx []int

	whereColumns, _ := first.whereColumnsAndValues()
	var (
		whereColumnsExpr ast.ExprNode
		whereValuesExpr  ast.ExprNode
	)
	// row constructor does not support only one value.
	if len(whereColumns) == 1 {
		whereColumnsExpr = &ast.ColumnNameExpr{
			Name: &ast.ColumnName{Name: model.NewCIStr(whereColumns[0])},
		}
		whereValuesExpr = &driver.ParamMarkerExpr{}
	} else {
		e := &ast.RowExpr{Values: make([]ast.ExprNode, 0, len(whereColumns))}
		for _, col := range whereColumns {
			e.Values = append(e.Values, &ast.ColumnNameExpr{
				Name: &ast.ColumnName{Name: model.NewCIStr(col)},
			})
		}
		whereColumnsExpr = e

		e2 := &ast.RowExpr{Values: make([]ast.ExprNode, 0, len(whereColumns))}
		for range whereColumns {
			e2.Values = append(e2.Values, &driver.ParamMarkerExpr{})
		}
		whereValuesExpr = e2
	}

	// WHEN (c1, c2) = (?, ?) THEN ?
	whenCommon := &ast.WhenClause{
		Expr: &ast.BinaryOperationExpr{
			Op: opcode.EQ,
			L:  whereColumnsExpr,
			R:  whereValuesExpr,
		},
		Result: &driver.ParamMarkerExpr{},
	}
	// each row change should generate one WHEN case, identified by PK/UK
	allWhenCases := make([]*ast.WhenClause, len(changes))
	for i := range allWhenCases {
		allWhenCases[i] = whenCommon
	}
	for i, col := range first.sourceTableInfo.Columns {
		if isGenerated(first.targetTableInfo.Columns, col.Name) {
			skipColIdx = append(skipColIdx, i)
			continue
		}

		assign := &ast.Assignment{Column: &ast.ColumnName{Name: col.Name}}
		assign.Expr = &ast.CaseExpr{WhenClauses: allWhenCases}
		stmt.List = append(stmt.List, assign)
	}

	// handle ... WHERE IN ...

	where := &ast.PatternInExpr{Expr: whereColumnsExpr}
	stmt.Where = where
	// every row change has a where case
	where.List = make([]ast.ExprNode, len(changes))
	for i := range where.List {
		where.List[i] = whereValuesExpr
	}

	// now build args of the UPDATE SQL

	args := make([]interface{}, 0, len(stmt.List)*len(changes)*(len(whereColumns)+1)+len(changes)*len(whereColumns))
	argsPerCol := make([][]interface{}, len(stmt.List))
	for i := range stmt.List {
		argsPerCol[i] = make([]interface{}, 0, len(changes)*(len(whereColumns)+1))
	}
	whereValuesAtTheEnd := make([]interface{}, 0, len(changes)*len(whereColumns))
	for _, change := range changes {
		_, whereValues := change.whereColumnsAndValues()
		// a simple check about different number of WHERE values, not trying to
		// cover all cases
		if len(whereValues) != len(whereColumns) {
			log.L().DPanic("len(whereValues) != len(whereColumns)",
				zap.Int("len(whereValues)", len(whereValues)),
				zap.Int("len(whereColumns)", len(whereColumns)),
				zap.Any("whereValues", whereValues),
				zap.Stringer("sourceTable", change.sourceTable))
			return "", nil
		}

		whereValuesAtTheEnd = append(whereValuesAtTheEnd, whereValues...)

		i := 0 // used as index of skipColIdx
		writeableCol := 0
		for j, val := range change.postValues {
			if i < len(skipColIdx) && skipColIdx[i] == j {
				i++
				continue
			}
			argsPerCol[writeableCol] = append(argsPerCol[writeableCol], whereValues...)
			argsPerCol[writeableCol] = append(argsPerCol[writeableCol], val)
			writeableCol++
		}
	}
	for _, a := range argsPerCol {
		args = append(args, a...)
	}
	args = append(args, whereValuesAtTheEnd...)

	var buf strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
	if err := stmt.Restore(restoreCtx); err != nil {
		log.L().DPanic("failed to generate multi-row UPDATE",
			zap.Int("numberOfChanges", len(changes)),
			zap.Error(err))
		return "", nil
	}
	return buf.String(), args
}
