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
type RowChange struct {
	sourceTable *cdcmodel.TableName
	targetTable *cdcmodel.TableName

	preValues  []interface{}
	postValues []interface{}

	sourceTableInfo *timodel.TableInfo
	targetTableInfo *timodel.TableInfo

	tiSessionCtx sessionctx.Context

	tp           RowChangeType
	identityInfo *schema.DownstreamTableInfo
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
	ret := &RowChange{
		sourceTable:     sourceTable,
		preValues:       preValues,
		postValues:      postValues,
		sourceTableInfo: sourceTableInfo,
	}

	if preValues != nil && len(preValues) != len(sourceTableInfo.Columns) {
		log.L().DPanic("preValues length not equal to sourceTableInfo columns",
			zap.Int("preValues", len(preValues)),
			zap.Int("sourceTableInfo", len(sourceTableInfo.Columns)),
			zap.Stringer("sourceTable", sourceTable))
	}
	if postValues != nil && len(postValues) != len(sourceTableInfo.Columns) {
		log.L().DPanic("postValues length not equal to sourceTableInfo columns",
			zap.Int("postValues", len(postValues)),
			zap.Int("sourceTableInfo", len(sourceTableInfo.Columns)),
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

// SetIdentifyInfo can be used when caller has calculated and cached
// identityInfo, to avoid every RowChange lazily initialize it.
func (r *RowChange) SetIdentifyInfo(info *schema.DownstreamTableInfo) {
	r.identityInfo = info
}

func (r *RowChange) lazyInitIdentityInfo() {
	if r.identityInfo != nil {
		return
	}

	r.identityInfo = schema.GetDownStreamTI(r.targetTableInfo, r.sourceTableInfo)
}

func getColsAndValuesOfIdx(
	columns []*timodel.ColumnInfo,
	indexColumns *timodel.IndexInfo,
	data []interface{},
) ([]*timodel.ColumnInfo, []interface{}) {
	cols := make([]*timodel.ColumnInfo, 0, len(indexColumns.Columns))
	values := make([]interface{}, 0, len(indexColumns.Columns))
	for _, col := range indexColumns.Columns {
		cols = append(cols, columns[col.Offset])
		values = append(values, data[col.Offset])
	}

	return cols, values
}

// whereColumnsAndValues returns columns and values to identify the row, to form
// the WHERE clause.
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

// valuesHolder gens values holder like (?,?,?).
func valuesHolder(n int) string {
	var builder strings.Builder
	builder.Grow((n-1)*2 + 3)
	builder.WriteByte('(')
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	builder.WriteByte(')')
	return builder.String()
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
			zap.String("sourceTable", r.sourceTable.String()),
			zap.Stringer("changeType", r.tp))
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

// GetSourceTable returns TableName of the source table.
func (r *RowChange) GetSourceTable() *cdcmodel.TableName {
	return r.sourceTable
}

// GetTargetTable returns TableName of the target table.
func (r *RowChange) GetTargetTable() *cdcmodel.TableName {
	return r.targetTable
}
