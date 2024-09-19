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
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/quotes"
	"go.uber.org/zap"
)

// RowChangeType is the type of row change.
type RowChangeType int

// these constants represent types of row change.
const (
	RowChangeNull RowChangeType = iota
	RowChangeInsert
	RowChangeUpdate
	RowChangeDelete
)

// String implements fmt.Stringer interface.
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
// It also provides some utility functions about calculating causality of two
// row changes, merging successive row changes into one row change, etc.
type RowChange struct {
	sourceTable *cdcmodel.TableName
	targetTable *cdcmodel.TableName

	preValues  []interface{}
	postValues []interface{}

	sourceTableInfo *timodel.TableInfo
	targetTableInfo *timodel.TableInfo

	tiSessionCtx sessionctx.Context

	tp          RowChangeType
	whereHandle *WhereHandle

	approximateDataSize int64
}

// NewRowChange creates a new RowChange.
// preValues stands for values exists before this change, postValues stands for
// values exists after this change.
// These parameters can be nil:
// - targetTable: when same as sourceTable or not applicable
// - preValues: when INSERT
// - postValues: when DELETE
// - targetTableInfo: when same as sourceTableInfo or not applicable
// - tiSessionCtx: will use default sessionCtx which is UTC timezone
// All arguments must not be changed after assigned to RowChange, any
// modification (like convert []byte to string) should be done before
// NewRowChange.
func NewRowChange(
	sourceTable *cdcmodel.TableName,
	targetTable *cdcmodel.TableName,
	preValues []interface{},
	postValues []interface{},
	sourceTableInfo *timodel.TableInfo,
	downstreamTableInfo *timodel.TableInfo,
	tiCtx sessionctx.Context,
) *RowChange {
	if sourceTable == nil {
		log.L().DPanic("sourceTable is nil")
	}
	if sourceTableInfo == nil {
		log.L().DPanic("sourceTableInfo is nil")
	}

	ret := &RowChange{
		sourceTable:     sourceTable,
		preValues:       preValues,
		postValues:      postValues,
		sourceTableInfo: sourceTableInfo,
	}

	colCount := ret.ColumnCount()
	if preValues != nil && len(preValues) != colCount {
		log.L().DPanic("preValues length not equal to sourceTableInfo columns",
			zap.Int("preValues", len(preValues)),
			zap.Int("sourceTableInfo", colCount),
			zap.Stringer("sourceTable", sourceTable))
	}
	if postValues != nil && len(postValues) != colCount {
		log.L().DPanic("postValues length not equal to sourceTableInfo columns",
			zap.Int("postValues", len(postValues)),
			zap.Int("sourceTableInfo", colCount),
			zap.Stringer("sourceTable", sourceTable))
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
		ret.tiSessionCtx = utils.ZeroSessionCtx
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
		log.L().DPanic("preValues and postValues can't both be nil",
			zap.Stringer("sourceTable", r.sourceTable))
	}
}

// Type returns the RowChangeType of this RowChange. Caller can future decide
// the DMLType when generate DML from it.
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

// ColumnCount returns the number of columns of this RowChange.
// TiDB TableInfo contains some internal columns like expression index, they
// are not included in this count.
func (r *RowChange) ColumnCount() int {
	c := 0
	for _, col := range r.sourceTableInfo.Columns {
		if !col.Hidden {
			c++
		}
	}
	return c
}

// SourceTableInfo returns the TableInfo of source table.
func (r *RowChange) SourceTableInfo() *timodel.TableInfo {
	return r.sourceTableInfo
}

// UniqueNotNullIdx returns the unique and not null index.
func (r *RowChange) UniqueNotNullIdx() *timodel.IndexInfo {
	r.lazyInitWhereHandle()
	return r.whereHandle.UniqueNotNullIdx
}

// SetWhereHandle can be used when caller has cached whereHandle, to avoid every
// RowChange lazily initialize it.
func (r *RowChange) SetWhereHandle(whereHandle *WhereHandle) {
	r.whereHandle = whereHandle
}

// GetApproximateDataSize returns internal approximateDataSize, it could be zero
// if this value is not set.
func (r *RowChange) GetApproximateDataSize() int64 {
	return r.approximateDataSize
}

// SetApproximateDataSize sets the approximate size of row change.
func (r *RowChange) SetApproximateDataSize(approximateDataSize int64) {
	r.approximateDataSize = approximateDataSize
}

func (r *RowChange) lazyInitWhereHandle() {
	if r.whereHandle != nil {
		return
	}

	r.whereHandle = GetWhereHandle(r.sourceTableInfo, r.targetTableInfo)
}

// whereColumnsAndValues returns columns and values to identify the row, to form
// the WHERE clause.
func (r *RowChange) whereColumnsAndValues() ([]string, []interface{}) {
	r.lazyInitWhereHandle()

	columns, values := r.sourceTableInfo.Columns, r.preValues

	uniqueIndex := r.whereHandle.getWhereIdxByData(r.preValues)
	if uniqueIndex != nil {
		columns, values = getColsAndValuesOfIdx(r.sourceTableInfo.Columns, uniqueIndex, values)
	}

	columnNames := make([]string, 0, len(columns))
	for _, column := range columns {
		columnNames = append(columnNames, column.Name.O)
	}

	failpoint.Inject("DownstreamTrackerWhereCheck", func() {
		if r.tp == RowChangeUpdate {
			log.L().Info("UpdateWhereColumnsCheck",
				zap.String("Columns", fmt.Sprintf("%v", columnNames)))
		} else if r.tp == RowChangeDelete {
			log.L().Info("DeleteWhereColumnsCheck",
				zap.String("Columns", fmt.Sprintf("%v", columnNames)))
		}
	})

	return columnNames, values
}

// genWhere generates WHERE clause for UPDATE and DELETE to identify the row.
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

func (r *RowChange) genDeleteSQL() (string, []interface{}) {
	if r.tp != RowChangeDelete && r.tp != RowChangeUpdate {
		log.L().DPanic("illegal type for genDeleteSQL",
			zap.String("sourceTable", r.sourceTable.String()),
			zap.Stringer("changeType", r.tp))
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

func (r *RowChange) genUpdateSQL() (string, []interface{}) {
	if r.tp != RowChangeUpdate {
		log.L().DPanic("illegal type for genUpdateSQL",
			zap.String("sourceTable", r.sourceTable.String()),
			zap.Stringer("changeType", r.tp))
		return "", nil
	}

	var buf strings.Builder
	buf.Grow(2048)
	buf.WriteString("UPDATE ")
	buf.WriteString(r.targetTable.QuoteString())
	buf.WriteString(" SET ")

	// Build target generated columns lower names set to accelerate following check
	generatedColumns := generatedColumnsNameSet(r.targetTableInfo.Columns)
	args := make([]interface{}, 0, len(r.preValues)+len(r.postValues))
	writtenFirstCol := false
	for i, col := range r.sourceTableInfo.Columns {
		if _, ok := generatedColumns[col.Name.L]; ok {
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

func (r *RowChange) genInsertSQL(tp DMLType) (string, []interface{}) {
	return GenInsertSQL(tp, r)
}

// DMLType indicates the type of DML.
type DMLType int

// these constants represent types of row change.
const (
	DMLNull DMLType = iota
	DMLInsert
	DMLReplace
	DMLInsertOnDuplicateUpdate
	DMLUpdate
	DMLDelete
)

// String implements fmt.Stringer interface.
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

// GenSQL generated a DML SQL for this RowChange.
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
		zap.String("sourceTable", r.sourceTable.String()),
		zap.Stringer("DMLType", tp))
	return "", nil
}

// GetPreValues is only used in tests.
func (r *RowChange) GetPreValues() []interface{} {
	return r.preValues
}

// GetPostValues is only used in tests.
func (r *RowChange) GetPostValues() []interface{} {
	return r.postValues
}

// RowValues returns the values of this row change
// for INSERT and UPDATE, it is the post values.
// for DELETE, it is the pre values.
func (r *RowChange) RowValues() []interface{} {
	switch r.tp {
	case RowChangeInsert, RowChangeUpdate:
		return r.postValues
	default:
		return r.preValues
	}
}

// GetSourceTable returns TableName of the source table.
func (r *RowChange) GetSourceTable() *cdcmodel.TableName {
	return r.sourceTable
}

// GetTargetTable returns TableName of the target table.
func (r *RowChange) GetTargetTable() *cdcmodel.TableName {
	return r.targetTable
}
