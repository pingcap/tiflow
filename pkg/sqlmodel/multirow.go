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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/quotes"
	"go.uber.org/zap"
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
	allArgs := make([]interface{}, 0, len(changes)*len(whereColumns))

	for i, c := range changes {
		if i > 0 {
			buf.WriteString(") OR (")
		}
		args := c.genWhere(&buf)
		allArgs = append(allArgs, args...)
	}
	buf.WriteString(")")
	return buf.String(), allArgs
}

// GenUpdateSQL generates the UPDATE SQL and its arguments.
// Input `changes` should have same target table and same columns for WHERE
// (typically same PK/NOT NULL UK), otherwise the behaviour is undefined.
func GenUpdateSQL(changes ...*RowChange) (string, []any) {
	if len(changes) == 0 {
		log.L().DPanic("row changes is empty")
		return "", nil
	}
	var buf strings.Builder
	buf.Grow(1024)

	// Generate UPDATE `db`.`table` SET
	first := changes[0]
	buf.WriteString("UPDATE ")
	buf.WriteString(first.targetTable.QuoteString())
	buf.WriteString(" SET ")

	// Pre-generate essential sub statements used after WHEN, WHERE.
	var (
		whenCaseStmts = make([]string, len(changes))
		whenCaseArgs  = make([][]interface{}, len(changes))
	)
	whereColumns, _ := first.whereColumnsAndValues()

	var whereBuf strings.Builder
	for i, c := range changes {
		whereBuf.Reset()
		whereBuf.Grow(128)
		whenCaseArgs[i] = c.genWhere(&whereBuf)
		whenCaseStmts[i] = whereBuf.String()
	}

	// Generate `ColumnName`=CASE WHEN .. THEN .. END
	// Use this value in order to identify which is the first CaseWhenThen line,
	// because generated column can happen any where and it will be skipped.
	isFirstCaseWhenThenLine := true
	for _, column := range first.targetTableInfo.Columns {
		if isGenerated(first.targetTableInfo.Columns, column.Name) {
			continue
		}
		if !isFirstCaseWhenThenLine {
			// insert ", " after END of each lines except for the first line.
			buf.WriteString(", ")
		}

		buf.WriteString(quotes.QuoteName(column.Name.String()) + "=CASE")
		for i := range changes {
			buf.WriteString(" WHEN ")
			buf.WriteString(whenCaseStmts[i])
			buf.WriteString(" THEN ?")
		}
		buf.WriteString(" END")
		isFirstCaseWhenThenLine = false
	}

	// Generate WHERE (...) OR (...)
	buf.WriteString(" WHERE (")
	for i, s := range whenCaseStmts {
		if i > 0 {
			buf.WriteString(") OR (")
		}
		buf.WriteString(s)
	}
	buf.WriteString(")")

	// Build args of the UPDATE SQL
	var assignValueColumnCount int
	var skipColIdx []int
	for i, col := range first.sourceTableInfo.Columns {
		if isGenerated(first.targetTableInfo.Columns, col.Name) {
			skipColIdx = append(skipColIdx, i)
			continue
		}
		assignValueColumnCount++
	}
	whereValuesAtTheEnd := make([]any, 0, len(changes)*len(whereColumns))
	args := make([]any, 0,
		assignValueColumnCount*len(changes)*(len(whereColumns)+1)+len(whereValuesAtTheEnd))
	argsPerCol := make([][]any, assignValueColumnCount)
	for i := 0; i < assignValueColumnCount; i++ {
		argsPerCol[i] = make([]any, 0, len(changes)*(len(whereColumns)+1))
	}
	for i, change := range changes {
		whereValues := whenCaseArgs[i]
		// a simple check about different number of WHERE values, not trying to
		// cover all cases
		if len(whereValues) != len(whereColumns) {
			log.Panic("len(whereValues) != len(whereColumns)",
				zap.Int("len(whereValues)", len(whereValues)),
				zap.Int("len(whereColumns)", len(whereColumns)),
				zap.Any("whereValues", whereValues),
				zap.Stringer("sourceTable", change.sourceTable))
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

	return buf.String(), args
}

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
