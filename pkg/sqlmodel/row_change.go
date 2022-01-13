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
	"fmt"
	"strings"

	"github.com/pingcap/failpoint"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"go.uber.org/zap"

	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/quotes"
)

type RowChangeType int

// these consts represent types of row change.
const (
	RowChangeNull RowChangeType = iota
	RowChangeInsert
	RowChangeUpdate
	RowChangeDelete
)

func (t RowChangeType) String() string {
	switch t {
	case RowChangeInsert:
		return "ChangeInsert"
	case RowChangeUpdate:
		return "ChangeUpdate"
	case RowChangeDelete:
		return "ChangeDelete"
	}

	return ""
}

// RowChange represents a row change, it can be further converted into DML SQL.
type RowChange struct {
	sourceTable *cdcmodel.TableName
	targetTable *cdcmodel.TableName

	preValues  []interface{} // values to be updated for UPDATE and values to be deleted for DELETE
	postValues []interface{} // values to be inserted for INSERT and values updated for UPDATE

	sourceTableInfo *timodel.TableInfo
	targetTableInfo *timodel.TableInfo

	tiSessionCtx sessionctx.Context

	tp           RowChangeType
	identityInfo *schema.DownstreamTableInfo // this field can be set from outside or lazy initialized
}

// NewRowChange creates a new RowChange.
// These parameters can be nil:
// - targetTable: when same as sourceTable or not applicable
// - preValues: when INSERT
// - postValues: when DELETE
// - targetTableInfo: when same as sourceTableInfo or not applicable
// - tiSessionCtx: use default sessionCtx which is UTC timezone
// The arguments must not be changed after assigned to RowChange, any modification (like convert []byte to string)
// should be done before NewRowChange.
func NewRowChange(
	sourceTable *cdcmodel.TableName,
	targetTable *cdcmodel.TableName,
	preValues []interface{},
	postValues []interface{},
	sourceTableInfo *timodel.TableInfo,
	downstreamTableInfo *timodel.TableInfo,
	tiCtx sessionctx.Context,
) *RowChange {
	ret := &RowChange{
		sourceTable:     sourceTable,
		preValues:       preValues,
		postValues:      postValues,
		sourceTableInfo: sourceTableInfo,
	}

	if targetTable != nil {
		ret.targetTable = targetTable
	} else {
		ret.targetTable = sourceTable
	}

	if downstreamTableInfo != nil {
		ret.targetTableInfo = downstreamTableInfo
	} else {
		ret.targetTableInfo = sourceTableInfo
	}

	if tiCtx != nil {
		ret.tiSessionCtx = tiCtx
	} else {
		ret.tiSessionCtx = utils.NewSessionCtx(nil)
	}

	ret.calculateType()

	return ret
}

func (r *RowChange) calculateType() {
	switch {
	case r.preValues == nil && r.postValues != nil:
		r.tp = RowChangeInsert
	case r.preValues != nil && r.postValues != nil:
		r.tp = RowChangeUpdate
	case r.preValues != nil && r.postValues == nil:
		r.tp = RowChangeDelete
	default:
		log.L().DPanic("preValues and postValues can't both be nil", zap.Stringer("sourceTable", r.sourceTable))
	}
}

func (r *RowChange) Type() RowChangeType {
	return r.tp
}

// String implements Stringer interface.
func (r *RowChange) String() string {
	return fmt.Sprintf("type: %s, source table: %s, target table: %s, preValues: %v, postValues: %v",
		r.tp, r.sourceTable, r.targetTable, r.preValues, r.postValues)
}

// TargetTableID returns a ID string for target table.
func (r *RowChange) TargetTableID() string {
	return r.targetTable.QuoteString()
}

// SetIdentifyInfo can be used to calculate and cache identityInfo in caller, to avoid every RowChange lazy initialize
// it.
func (r *RowChange) SetIdentifyInfo(info *schema.DownstreamTableInfo) {
	r.identityInfo = info
}

func (r *RowChange) lazyInitIdentityInfo() {
	if r.identityInfo != nil {
		return
	}

	r.identityInfo = schema.GetDownStreamTI(r.targetTableInfo, r.sourceTableInfo)
}

func getColsAndValuesOfIdx(columns []*timodel.ColumnInfo, indexColumns *timodel.IndexInfo, data []interface{}) ([]*timodel.ColumnInfo, []interface{}) {
	cols := make([]*timodel.ColumnInfo, 0, len(indexColumns.Columns))
	values := make([]interface{}, 0, len(indexColumns.Columns))
	for _, column := range indexColumns.Columns {
		cols = append(cols, columns[column.Offset])
		values = append(values, data[column.Offset])
	}

	return cols, values
}

// whereColumnsAndValues returns columns and values to identify the row.
func (r *RowChange) whereColumnsAndValues() ([]string, []interface{}) {
	r.lazyInitIdentityInfo()

	uniqueIndex := r.identityInfo.AbsoluteUKIndexInfo
	if uniqueIndex == nil {
		uniqueIndex = schema.GetIdentityUKByData(r.identityInfo, r.preValues)
	}

	columns, values := r.sourceTableInfo.Columns, r.preValues
	if uniqueIndex != nil {
		columns, values = getColsAndValuesOfIdx(r.sourceTableInfo.Columns, uniqueIndex, values)
	}

	columnNames := make([]string, 0, len(columns))
	for _, column := range columns {
		columnNames = append(columnNames, column.Name.O)
	}

	failpoint.Inject("DownstreamTrackerWhereCheck", func() {
		if r.tp == RowChangeUpdate {
			log.L().Info("UpdateWhereColumnsCheck", zap.String("Columns", fmt.Sprintf("%v", columnNames)))
		} else if r.tp == RowChangeDelete {
			log.L().Info("DeleteWhereColumnsCheck", zap.String("Columns", fmt.Sprintf("%v", columnNames)))
		}
	})

	return columnNames, values
}

// genWhere generates where clause for UPDATE and DELETE to identify the row.
// the SQL part is written to `buf` and the args part is returned.
func (r *RowChange) genWhere(buf *strings.Builder) []interface{} {
	whereColumns, whereValues := r.whereColumnsAndValues()

	for i, col := range whereColumns {
		if i != 0 {
			buf.WriteString(" AND ")
		}
		buf.WriteString(quotes.QuoteName(col))
		if whereValues[i] == nil {
			buf.WriteString(" IS ?")
		} else {
			buf.WriteString(" = ?")
		}
	}
	return whereValues
}

func GenDeleteSQL(changes ...*RowChange) (string, []interface{}) {
	if len(changes) == 0 {
		return "", nil
	}

	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(changes[0].targetTable.QuoteString())
	buf.WriteString(" WHERE (")

	whereColumns, _ := changes[0].whereColumnsAndValues()
	for i, column := range whereColumns {
		if i != len(whereColumns)-1 {
			buf.WriteString(quotes.QuoteName(column) + ",")
		} else {
			buf.WriteString(quotes.QuoteName(column) + ")")
		}
	}
	buf.WriteString(" IN (")
	// can't handle iS NULL???
	args := make([]interface{}, 0, len(changes)*len(whereColumns))
	for i, change := range changes {
		if i > 0 {
			buf.WriteString(",")
		}
		writeValuesHolder(&buf, len(whereColumns))
		_, whereValues := change.whereColumnsAndValues()
		// whereValues will have same length because we have checked it in genDMLsWithSameCols.
		args = append(args, whereValues...)
	}
	buf.WriteString(")")
	return buf.String(), args
}

func (r *RowChange) genDeleteSQL() (string, []interface{}) {
	if r.tp != RowChangeDelete && r.tp != RowChangeUpdate {
		log.L().DPanic("illegal type for genDeleteSQL",
			zap.String("source table", r.sourceTable.String()),
			zap.Stringer("change type", r.tp))
		return "", nil
	}

	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(r.targetTable.QuoteString())
	buf.WriteString(" WHERE ")
	whereArgs := r.genWhere(&buf)
	buf.WriteString(" LIMIT 1")

	return buf.String(), whereArgs
}

func isGenerated(columns []*timodel.ColumnInfo, name timodel.CIStr) bool {
	for _, col := range columns {
		if col.Name.L == name.L {
			return col.IsGenerated()
		}
	}
	return false
}

func (r *RowChange) genUpdateSQL() (string, []interface{}) {
	if r.tp != RowChangeUpdate {
		log.L().DPanic("illegal type for genUpdateSQL",
			zap.String("source table", r.sourceTable.String()),
			zap.Stringer("change type", r.tp))
		return "", nil
	}

	var buf strings.Builder
	buf.Grow(2048)
	buf.WriteString("UPDATE ")
	buf.WriteString(r.targetTable.QuoteString())
	buf.WriteString(" SET ")

	args := make([]interface{}, 0, len(r.preValues)+len(r.postValues))
	writtenFirstCol := false
	for i, col := range r.sourceTableInfo.Columns {
		if isGenerated(r.targetTableInfo.Columns, col.Name) {
			continue
		}

		if writtenFirstCol {
			buf.WriteString(", ")
		}
		writtenFirstCol = true
		fmt.Fprintf(&buf, "%s = ?", quotes.QuoteName(col.Name.O))
		args = append(args, r.postValues[i])
	}

	buf.WriteString(" WHERE ")
	whereArgs := r.genWhere(&buf)
	buf.WriteString(" LIMIT 1")

	args = append(args, whereArgs...)
	return buf.String(), args
}

// writeValuesHolder gens values holder like (?,?,?).
func writeValuesHolder(buf *strings.Builder, n int) {
	buf.WriteByte('(')
	for i := 0; i < n; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("?")
	}
	buf.WriteByte(')')
}

func GenInsertSQL(tp DMLType, changes ...*RowChange) (string, []interface{}) {
	if len(changes) == 0 {
		return "", nil
	}

	var buf strings.Builder
	buf.Grow(1024)
	if tp == DMLReplace {
		buf.WriteString("REPLACE INTO ")
	} else {
		buf.WriteString("INSERT INTO ")
	}
	buf.WriteString(changes[0].targetTable.QuoteString())
	buf.WriteString(" (")
	columnNum := 0
	var skipColIdx []int
	for i, col := range changes[0].sourceTableInfo.Columns {
		if isGenerated(changes[0].targetTableInfo.Columns, col.Name) {
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
	for i := range changes {
		if i > 0 {
			buf.WriteString(",")
		}
		writeValuesHolder(&buf, columnNum)
	}
	if tp == DMLInsertOnDuplicateUpdate {
		buf.WriteString(" ON DUPLICATE KEY UPDATE ")
		for j, col := range changes[0].sourceTableInfo.Columns {
			colName := quotes.QuoteName(col.Name.O)
			buf.WriteString(colName + "=VALUES(" + colName + ")")
			if j != len(changes[0].sourceTableInfo.Columns)-1 {
				buf.WriteByte(',')
			}
		}
	}

	// TODO: move the check earlier
	args := make([]interface{}, 0, len(changes)*len(changes[0].sourceTableInfo.Columns))
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

func (r *RowChange) genInsertSQL(tp DMLType) (string, []interface{}) {
	return GenInsertSQL(tp, r)
}

type DMLType int

// these consts represent types of row change.
const (
	DMLNull DMLType = iota
	DMLInsert
	DMLReplace
	DMLInsertOnDuplicateUpdate
	DMLUpdate
	DMLDelete
)

func (t DMLType) String() string {
	switch t {
	case DMLInsert:
		return "DMLInsert"
	case DMLReplace:
		return "DMLReplace"
	case DMLUpdate:
		return "DMLUpdate"
	case DMLInsertOnDuplicateUpdate:
		return "DMLInsertOnDuplicateUpdate"
	case DMLDelete:
		return "DMLDelete"
	}

	return ""
}

func (r *RowChange) GenSQL(tp DMLType) (string, []interface{}) {
	switch tp {
	case DMLInsert, DMLReplace, DMLInsertOnDuplicateUpdate:
		return r.genInsertSQL(tp)
	case DMLUpdate:
		return r.genUpdateSQL()
	case DMLDelete:
		return r.genDeleteSQL()
	}
	log.L().DPanic("illegal type for GenSQL",
		zap.String("source table", r.sourceTable.String()),
		zap.Stringer("DML type", tp))
	return "", nil
}
