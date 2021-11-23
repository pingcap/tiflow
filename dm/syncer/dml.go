// Copyright 2019 PingCAP, Inc.
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

package syncer

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/tablecodec"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/ticdc/dm/pkg/context"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/pkg/schema"
	"github.com/pingcap/ticdc/dm/pkg/terror"
	"github.com/pingcap/ticdc/dm/pkg/utils"
)

// this type is used to generate DML SQL, opType is used to mark type in binlog.
type dmlOpType int64

const (
	insertDML                      = dmlOpType(insert)
	updateDML                      = dmlOpType(update)
	deleteDML                      = dmlOpType(del)
	insertOnDuplicateDML dmlOpType = iota + 1
)

func (op dmlOpType) String() (str string) {
	switch op {
	case insertDML:
		return "insert"
	case updateDML:
		return "update"
	case deleteDML:
		return "delete"
	case insertOnDuplicateDML:
		return "insert on duplicate update"
	}
	return
}

// genDMLParam stores pruned columns, data as well as the original columns, data, index.
type genDMLParam struct {
	targetTableID   string              // as a key in map like `schema`.`table`
	sourceTable     *filter.Table       // origin table
	safeMode        bool                // only used in update
	data            [][]interface{}     // pruned data
	originalData    [][]interface{}     // all data
	columns         []*model.ColumnInfo // pruned columns
	sourceTableInfo *model.TableInfo    // all table info
}

func extractValueFromData(data []interface{}, columns []*model.ColumnInfo) []interface{} {
	value := make([]interface{}, 0, len(data))
	for i := range data {
		value = append(value, castUnsigned(data[i], &columns[i].FieldType))
	}
	return value
}

func (s *Syncer) genAndFilterInsertDMLs(tctx *tcontext.Context, param *genDMLParam, filterExprs []expression.Expression) ([]*DML, error) {
	var (
		tableID         = param.targetTableID
		dataSeq         = param.data
		originalDataSeq = param.originalData
		columns         = param.columns
		ti              = param.sourceTableInfo
		dmls            = make([]*DML, 0, len(dataSeq))
	)

	// if downstream pk/uk(not null) exits, then use downstream pk/uk(not null)
	downstreamTableInfo, err := s.schemaTracker.GetDownStreamTableInfo(tctx, tableID, ti)
	if err != nil {
		return nil, err
	}
	downstreamIndexColumns := downstreamTableInfo.AbsoluteUKIndexInfo

RowLoop:
	for dataIdx, data := range dataSeq {
		if len(data) != len(columns) {
			return nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(columns), len(data))
		}

		value := extractValueFromData(data, columns)
		originalValue := value
		if len(columns) != len(ti.Columns) {
			originalValue = extractValueFromData(originalDataSeq[dataIdx], ti.Columns)
		}

		for _, expr := range filterExprs {
			skip, err := SkipDMLByExpression(s.sessCtx, originalValue, expr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip {
				s.filteredInsert.Add(1)
				continue RowLoop
			}
		}

		if downstreamIndexColumns == nil {
			downstreamIndexColumns = s.schemaTracker.GetAvailableDownStreamUKIndexInfo(tableID, value)
		}

		dmls = append(dmls, newDML(insert, param.safeMode, tableID, param.sourceTable, nil, value, nil, originalValue, columns, ti, downstreamIndexColumns, downstreamTableInfo))
	}

	return dmls, nil
}

func (s *Syncer) genAndFilterUpdateDMLs(
	tctx *tcontext.Context,
	param *genDMLParam,
	oldValueFilters []expression.Expression,
	newValueFilters []expression.Expression,
) ([]*DML, error) {
	var (
		tableID      = param.targetTableID
		data         = param.data
		originalData = param.originalData
		columns      = param.columns
		ti           = param.sourceTableInfo
		dmls         = make([]*DML, 0, len(data)/2)
	)

	// if downstream pk/uk(not null) exits, then use downstream pk/uk(not null)
	downstreamTableInfo, err := s.schemaTracker.GetDownStreamTableInfo(tctx, tableID, ti)
	if err != nil {
		return nil, err
	}
	downstreamIndexColumns := downstreamTableInfo.AbsoluteUKIndexInfo

RowLoop:
	for i := 0; i < len(data); i += 2 {
		oldData := data[i]
		changedData := data[i+1]
		oriOldData := originalData[i]
		oriChangedData := originalData[i+1]

		if len(oldData) != len(changedData) {
			return nil, terror.ErrSyncerUnitDMLOldNewValueMismatch.Generate(len(oldData), len(changedData))
		}

		if len(oldData) != len(columns) {
			return nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(columns), len(oldData))
		}

		oldValues := extractValueFromData(oldData, columns)
		changedValues := extractValueFromData(changedData, columns)

		var oriOldValues, oriChangedValues []interface{}
		if len(columns) == len(ti.Columns) {
			oriOldValues = oldValues
			oriChangedValues = changedValues
		} else {
			oriOldValues = extractValueFromData(oriOldData, ti.Columns)
			oriChangedValues = extractValueFromData(oriChangedData, ti.Columns)
		}

		for j := range oldValueFilters {
			// AND logic
			oldExpr, newExpr := oldValueFilters[j], newValueFilters[j]
			skip1, err := SkipDMLByExpression(s.sessCtx, oriOldValues, oldExpr, ti.Columns)
			if err != nil {
				return nil, err
			}
			skip2, err := SkipDMLByExpression(s.sessCtx, oriChangedValues, newExpr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip1 && skip2 {
				s.filteredUpdate.Add(1)
				// TODO: we skip generating the UPDATE SQL, so we left the old value here. Is this expected?
				continue RowLoop
			}
		}

		if downstreamIndexColumns == nil {
			downstreamIndexColumns = s.schemaTracker.GetAvailableDownStreamUKIndexInfo(tableID, oriOldValues)
		}

		dmls = append(dmls, newDML(update, param.safeMode, param.targetTableID, param.sourceTable, oldValues, changedValues, oriOldValues, oriChangedValues, columns, ti, downstreamIndexColumns, downstreamTableInfo))
	}

	return dmls, nil
}

func (s *Syncer) genAndFilterDeleteDMLs(tctx *tcontext.Context, param *genDMLParam, filterExprs []expression.Expression) ([]*DML, error) {
	var (
		tableID = param.targetTableID
		dataSeq = param.originalData
		ti      = param.sourceTableInfo
		dmls    = make([]*DML, 0, len(dataSeq))
	)

	// if downstream pk/uk(not null) exits, then use downstream pk/uk(not null)
	downstreamTableInfo, err := s.schemaTracker.GetDownStreamTableInfo(tctx, tableID, ti)
	if err != nil {
		return nil, err
	}
	downstreamIndexColumns := downstreamTableInfo.AbsoluteUKIndexInfo

RowLoop:
	for _, data := range dataSeq {
		if len(data) != len(ti.Columns) {
			return nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(ti.Columns), len(data))
		}

		value := extractValueFromData(data, ti.Columns)

		for _, expr := range filterExprs {
			skip, err := SkipDMLByExpression(s.sessCtx, value, expr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip {
				s.filteredDelete.Add(1)
				continue RowLoop
			}
		}

		if downstreamIndexColumns == nil {
			downstreamIndexColumns = s.schemaTracker.GetAvailableDownStreamUKIndexInfo(tableID, value)
		}

		dmls = append(dmls, newDML(del, false, param.targetTableID, param.sourceTable, nil, value, nil, value, ti.Columns, ti, downstreamIndexColumns, downstreamTableInfo))
	}

	return dmls, nil
}

func castUnsigned(data interface{}, ft *types.FieldType) interface{} {
	if !mysql.HasUnsignedFlag(ft.Flag) {
		return data
	}

	switch v := data.(type) {
	case int:
		return uint(v)
	case int8:
		return uint8(v)
	case int16:
		return uint16(v)
	case int32:
		if ft.Tp == mysql.TypeInt24 {
			// we use int32 to store MEDIUMINT, if the value is signed, it's fine
			// but if the value is un-signed, simply convert it use `uint32` may out of the range
			// like -4692783 converted to 4290274513 (2^32 - 4692783), but we expect 12084433 (2^24 - 4692783)
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, uint32(v))
			return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
		}
		return uint32(v)
	case int64:
		return uint64(v)
	}

	return data
}

func columnValue(value interface{}, ft *types.FieldType) string {
	castValue := castUnsigned(value, ft)

	var data string
	switch v := castValue.(type) {
	case nil:
		data = "null"
	case bool:
		if v {
			data = "1"
		} else {
			data = "0"
		}
	case int:
		data = strconv.FormatInt(int64(v), 10)
	case int8:
		data = strconv.FormatInt(int64(v), 10)
	case int16:
		data = strconv.FormatInt(int64(v), 10)
	case int32:
		data = strconv.FormatInt(int64(v), 10)
	case int64:
		data = strconv.FormatInt(v, 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(v, 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}

	return data
}

func findFitIndex(ti *model.TableInfo) *model.IndexInfo {
	for _, idx := range ti.Indices {
		if idx.Primary {
			return idx
		}
	}

	if pk := ti.GetPkColInfo(); pk != nil {
		return &model.IndexInfo{
			Table:   ti.Name,
			Unique:  true,
			Primary: true,
			State:   model.StatePublic,
			Tp:      model.IndexTypeBtree,
			Columns: []*model.IndexColumn{{
				Name:   pk.Name,
				Offset: pk.Offset,
				Length: types.UnspecifiedLength,
			}},
		}
	}

	// second find not null unique key
	fn := func(i int) bool {
		return !mysql.HasNotNullFlag(ti.Columns[i].Flag)
	}

	return getSpecifiedIndexColumn(ti, fn)
}

func getSpecifiedIndexColumn(ti *model.TableInfo, fn func(i int) bool) *model.IndexInfo {
	for _, indexCols := range ti.Indices {
		if !indexCols.Unique {
			continue
		}

		findFitIndex := true
		for _, col := range indexCols.Columns {
			if fn(col.Offset) {
				findFitIndex = false
				break
			}
		}

		if findFitIndex {
			return indexCols
		}
	}

	return nil
}

func getColumnData(columns []*model.ColumnInfo, indexColumns *model.IndexInfo, data []interface{}) ([]*model.ColumnInfo, []interface{}) {
	cols := make([]*model.ColumnInfo, 0, len(indexColumns.Columns))
	values := make([]interface{}, 0, len(indexColumns.Columns))
	for _, column := range indexColumns.Columns {
		cols = append(cols, columns[column.Offset])
		values = append(values, data[column.Offset])
	}

	return cols, values
}

func (s *Syncer) mappingDML(table *filter.Table, ti *model.TableInfo, data [][]interface{}) ([][]interface{}, error) {
	if s.columnMapping == nil {
		return data, nil
	}

	columns := make([]string, 0, len(ti.Columns))
	for _, col := range ti.Columns {
		columns = append(columns, col.Name.O)
	}

	var (
		err  error
		rows = make([][]interface{}, len(data))
	)
	for i := range data {
		rows[i], _, err = s.columnMapping.HandleRowValue(table.Schema, table.Name, columns, data[i])
		if err != nil {
			return nil, terror.ErrSyncerUnitDoColumnMapping.Delegate(err, data[i], table)
		}
	}
	return rows, nil
}

// pruneGeneratedColumnDML filters columns list, data and index removing all
// generated column. because generated column is not support setting value
// directly in DML, we must remove generated column from DML, including column
// list and data list including generated columns.
func pruneGeneratedColumnDML(ti *model.TableInfo, data [][]interface{}) ([]*model.ColumnInfo, [][]interface{}, error) {
	// search for generated columns. if none found, return everything as-is.
	firstGeneratedColumnIndex := -1
	for i, c := range ti.Columns {
		if c.IsGenerated() {
			firstGeneratedColumnIndex = i
			break
		}
	}
	if firstGeneratedColumnIndex < 0 {
		return ti.Columns, data, nil
	}

	// remove generated columns from the list of columns
	cols := make([]*model.ColumnInfo, 0, len(ti.Columns))
	cols = append(cols, ti.Columns[:firstGeneratedColumnIndex]...)
	for _, c := range ti.Columns[(firstGeneratedColumnIndex + 1):] {
		if !c.IsGenerated() {
			cols = append(cols, c)
		}
	}

	// remove generated columns from the list of data.
	rows := make([][]interface{}, 0, len(data))
	for _, row := range data {
		if len(row) != len(ti.Columns) {
			return nil, nil, terror.ErrSyncerUnitDMLPruneColumnMismatch.Generate(len(ti.Columns), len(data))
		}
		value := make([]interface{}, 0, len(cols))
		for i := range row {
			if !ti.Columns[i].IsGenerated() {
				value = append(value, row[i])
			}
		}
		rows = append(rows, value)
	}
	return cols, rows, nil
}

// checkLogColumns returns error when not all rows in skipped is empty, which means the binlog doesn't contain all
// columns.
// TODO: don't return error when all skipped columns is non-PK.
func checkLogColumns(skipped [][]int) error {
	for _, row := range skipped {
		if len(row) > 0 {
			return terror.ErrBinlogNotLogColumn
		}
	}
	return nil
}

// DML stores param for DML.
type DML struct {
	targetTableID             string
	sourceTable               *filter.Table
	op                        opType
	oldValues                 []interface{} // only for update SQL
	values                    []interface{}
	columns                   []*model.ColumnInfo
	sourceTableInfo           *model.TableInfo
	originOldValues           []interface{} // only for update SQL
	originValues              []interface{} // use to gen key and `WHERE`
	safeMode                  bool
	key                       string                      // use to detect causality
	pickedDownstreamIndexInfo *model.IndexInfo            // pick an index from downstream which comes from pk/uk not null/uk value not null and is only used in genWhere
	downstreamTableInfo       *schema.DownstreamTableInfo // downstream table info
}

// newDML creates DML.
func newDML(op opType, safeMode bool, targetTableID string, sourceTable *filter.Table, oldValues, values, originOldValues, originValues []interface{}, columns []*model.ColumnInfo, sourceTableInfo *model.TableInfo, pickedDownstreamIndexInfo *model.IndexInfo, downstreamTableInfo *schema.DownstreamTableInfo) *DML {
	return &DML{
		op:                        op,
		safeMode:                  safeMode,
		targetTableID:             targetTableID,
		sourceTable:               sourceTable,
		oldValues:                 oldValues,
		values:                    values,
		columns:                   columns,
		sourceTableInfo:           sourceTableInfo,
		originOldValues:           originOldValues,
		originValues:              originValues,
		pickedDownstreamIndexInfo: pickedDownstreamIndexInfo,
		downstreamTableInfo:       downstreamTableInfo,
	}
}

// String returns the DML's string.
func (dml *DML) String() string {
	return fmt.Sprintf("[safemode: %t, targetTableID: %s, op: %s, columns: %v, oldValues: %v, values: %v]", dml.safeMode, dml.targetTableID, dml.op.String(), dml.columnNames(), dml.originOldValues, dml.originValues)
}

// updateToDelAndInsert turns updateDML to delDML and insertDML.
func updateToDelAndInsert(updateDML *DML) (*DML, *DML) {
	delDML := &DML{}
	*delDML = *updateDML
	delDML.op = del
	// use oldValues of update as values of delete and reset oldValues
	delDML.values = updateDML.oldValues
	delDML.originValues = updateDML.originOldValues
	delDML.oldValues = nil
	delDML.originOldValues = nil

	insertDML := &DML{}
	*insertDML = *updateDML
	insertDML.op = insert
	// reset oldValues
	insertDML.oldValues = nil
	insertDML.originOldValues = nil

	return delDML, insertDML
}

// identifyColumns gets columns of unique not null index.
// This is used for compact.
func (dml *DML) identifyColumns() []string {
	if defaultIndexColumns := dml.downstreamTableInfo.AbsoluteUKIndexInfo; defaultIndexColumns != nil {
		columns := make([]string, 0, len(defaultIndexColumns.Columns))
		for _, column := range defaultIndexColumns.Columns {
			columns = append(columns, column.Name.O)
		}
		return columns
	}
	return nil
}

// identifyValues gets values of unique not null index.
// This is used for compact.
func (dml *DML) identifyValues() []interface{} {
	if defaultIndexColumns := dml.downstreamTableInfo.AbsoluteUKIndexInfo; defaultIndexColumns != nil {
		values := make([]interface{}, 0, len(defaultIndexColumns.Columns))
		for _, column := range defaultIndexColumns.Columns {
			values = append(values, dml.values[column.Offset])
		}
		return values
	}
	return nil
}

// oldIdentifyValues gets old values of unique not null index.
// only for update SQL.
func (dml *DML) oldIdentifyValues() []interface{} {
	if defaultIndexColumns := dml.downstreamTableInfo.AbsoluteUKIndexInfo; defaultIndexColumns != nil {
		values := make([]interface{}, 0, len(defaultIndexColumns.Columns))
		for _, column := range defaultIndexColumns.Columns {
			values = append(values, dml.oldValues[column.Offset])
		}
		return values
	}
	return nil
}

// identifyKey use identifyValues to gen key.
// This is used for compact.
// PK or (UK + NOT NULL).
func (dml *DML) identifyKey() string {
	return genKey(dml.identifyValues())
}

// updateIdentify check whether a update sql update its identify values.
func (dml *DML) updateIdentify() bool {
	if len(dml.oldValues) == 0 {
		return false
	}

	values := dml.identifyValues()
	oldValues := dml.oldIdentifyValues()

	if len(values) != len(oldValues) {
		return true
	}

	for i := 0; i < len(values); i++ {
		if values[i] != oldValues[i] {
			return true
		}
	}

	return false
}

// identifyKeys gens keys by unique not null value.
// This is used for causality.
// PK or (UK + NOT NULL) or (UK + NULL + NOT NULL VALUE).
func (dml *DML) identifyKeys(ctx sessionctx.Context) []string {
	var keys []string
	// for UPDATE statement
	if dml.originOldValues != nil {
		keys = append(keys, genMultipleKeys(ctx, dml.downstreamTableInfo, dml.sourceTableInfo, dml.originOldValues, dml.targetTableID)...)
	}

	if dml.originValues != nil {
		keys = append(keys, genMultipleKeys(ctx, dml.downstreamTableInfo, dml.sourceTableInfo, dml.originValues, dml.targetTableID)...)
	}
	return keys
}

// columnNames return column names of DML.
func (dml *DML) columnNames() []string {
	columnNames := make([]string, 0, len(dml.columns))
	for _, column := range dml.columns {
		columnNames = append(columnNames, column.Name.O)
	}
	return columnNames
}

// whereColumnsAndValues gets columns and values of unique column with not null value.
// This is used to generete where condition.
func (dml *DML) whereColumnsAndValues() ([]string, []interface{}) {
	columns, values := dml.sourceTableInfo.Columns, dml.originValues

	if dml.op == update {
		values = dml.originOldValues
	}

	if dml.pickedDownstreamIndexInfo != nil {
		columns, values = getColumnData(dml.sourceTableInfo.Columns, dml.pickedDownstreamIndexInfo, values)
	}

	columnNames := make([]string, 0, len(columns))
	for _, column := range columns {
		columnNames = append(columnNames, column.Name.O)
	}

	failpoint.Inject("DownstreamTrackerWhereCheck", func() {
		if dml.op == update {
			log.L().Info("UpdateWhereColumnsCheck", zap.String("Columns", fmt.Sprintf("%v", columnNames)))
		} else if dml.op == del {
			log.L().Info("DeleteWhereColumnsCheck", zap.String("Columns", fmt.Sprintf("%v", columnNames)))
		}
	})

	return columnNames, values
}

// genKey gens key by values e.g. "a.1.b".
// This is used for compact.
func genKey(values []interface{}) string {
	builder := new(strings.Builder)
	for i, v := range values {
		if i != 0 {
			builder.WriteString(".")
		}
		fmt.Fprintf(builder, "%v", v)
	}

	return builder.String()
}

// genKeyList format keys.
func genKeyList(table string, columns []*model.ColumnInfo, dataSeq []interface{}) string {
	var buf strings.Builder
	for i, data := range dataSeq {
		if data == nil {
			log.L().Debug("ignore null value", zap.String("column", columns[i].Name.O), zap.String("table", table))
			continue // ignore `null` value.
		}
		// one column key looks like:`column_val.column_name.`
		buf.WriteString(columnValue(data, &columns[i].FieldType))
		buf.WriteString(".")
		buf.WriteString(columns[i].Name.O)
		buf.WriteString(".")
	}
	if buf.Len() == 0 {
		log.L().Debug("all value are nil, no key generated", zap.String("table", table))
		return "" // all values are `null`.
	}
	buf.WriteString(table)
	return buf.String()
}

// truncateIndexValues truncate prefix index from data.
func truncateIndexValues(ctx sessionctx.Context, ti *model.TableInfo, indexColumns *model.IndexInfo, tiColumns []*model.ColumnInfo, data []interface{}) []interface{} {
	values := make([]interface{}, 0, len(indexColumns.Columns))
	datums, err := utils.AdjustBinaryProtocolForDatum(ctx, data, tiColumns)
	if err != nil {
		log.L().Warn("adjust binary protocol for datum error", zap.Error(err))
		return data
	}
	tablecodec.TruncateIndexValues(ti, indexColumns, datums)
	for _, datum := range datums {
		values = append(values, datum.GetValue())
	}
	return values
}

// genMultipleKeys gens keys with UNIQUE NOT NULL value.
// if not UNIQUE NOT NULL value, use table name instead.
func genMultipleKeys(ctx sessionctx.Context, downstreamTableInfo *schema.DownstreamTableInfo, ti *model.TableInfo, value []interface{}, table string) []string {
	multipleKeys := make([]string, 0, len(downstreamTableInfo.AvailableUKIndexList))

	for _, indexCols := range downstreamTableInfo.AvailableUKIndexList {
		cols, vals := getColumnData(ti.Columns, indexCols, value)
		// handle prefix index
		truncVals := truncateIndexValues(ctx, ti, indexCols, cols, vals)
		key := genKeyList(table, cols, truncVals)
		if len(key) > 0 { // ignore `null` value.
			multipleKeys = append(multipleKeys, key)
		} else {
			log.L().Debug("ignore empty key", zap.String("table", table))
		}
	}

	if len(multipleKeys) == 0 {
		// use table name as key if no key generated (no PK/UK),
		// no concurrence for rows in the same table.
		log.L().Debug("use table name as the key", zap.String("table", table))
		multipleKeys = append(multipleKeys, table)
	}

	return multipleKeys
}

// genWhere generates where condition.
func (dml *DML) genWhere(buf *strings.Builder) []interface{} {
	whereColumns, whereValues := dml.whereColumnsAndValues()

	for i, col := range whereColumns {
		if i != 0 {
			buf.WriteString(" AND ")
		}
		buf.WriteString(dbutil.ColumnName(col))
		if whereValues[i] == nil {
			buf.WriteString(" IS ?")
		} else {
			buf.WriteString(" = ?")
		}
	}
	return whereValues
}

// genSQL generates SQL for a DML.
func (dml *DML) genSQL() (sql []string, arg [][]interface{}) {
	switch dml.op {
	case insert:
		return dml.genInsertSQL()
	case del:
		return dml.genDeleteSQL()
	case update:
		return dml.genUpdateSQL()
	}
	return
}

// genUpdateSQL generates a `UPDATE` SQL with `WHERE`.
func (dml *DML) genUpdateSQL() ([]string, [][]interface{}) {
	if dml.safeMode {
		sqls, args := dml.genDeleteSQL()
		insertSQLs, insertArgs := dml.genInsertSQL()
		sqls = append(sqls, insertSQLs...)
		args = append(args, insertArgs...)
		return sqls, args
	}
	var buf strings.Builder
	buf.Grow(2048)
	buf.WriteString("UPDATE ")
	buf.WriteString(dml.targetTableID)
	buf.WriteString(" SET ")

	for i, column := range dml.columns {
		if i == len(dml.columns)-1 {
			fmt.Fprintf(&buf, "%s = ?", dbutil.ColumnName(column.Name.O))
		} else {
			fmt.Fprintf(&buf, "%s = ?, ", dbutil.ColumnName(column.Name.O))
		}
	}

	buf.WriteString(" WHERE ")
	whereArgs := dml.genWhere(&buf)
	buf.WriteString(" LIMIT 1")

	args := dml.values
	args = append(args, whereArgs...)
	return []string{buf.String()}, [][]interface{}{args}
}

// genDeleteSQL generates a `DELETE FROM` SQL with `WHERE`.
func (dml *DML) genDeleteSQL() ([]string, [][]interface{}) {
	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(dml.targetTableID)
	buf.WriteString(" WHERE ")
	whereArgs := dml.genWhere(&buf)
	buf.WriteString(" LIMIT 1")

	return []string{buf.String()}, [][]interface{}{whereArgs}
}

// genInsertSQL generates a `INSERT`.
// if in safemode, generates a `INSERT ON DUPLICATE UPDATE` statement.
func (dml *DML) genInsertSQL() ([]string, [][]interface{}) {
	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("INSERT INTO ")
	buf.WriteString(dml.targetTableID)
	buf.WriteString(" (")
	for i, column := range dml.columns {
		buf.WriteString(dbutil.ColumnName(column.Name.O))
		if i != len(dml.columns)-1 {
			buf.WriteByte(',')
		} else {
			buf.WriteByte(')')
		}
	}
	buf.WriteString(" VALUES (")

	// placeholders
	for i := range dml.columns {
		if i != len(dml.columns)-1 {
			buf.WriteString("?,")
		} else {
			buf.WriteString("?)")
		}
	}
	if dml.safeMode {
		buf.WriteString(" ON DUPLICATE KEY UPDATE ")
		for i, column := range dml.columns {
			col := dbutil.ColumnName(column.Name.O)
			buf.WriteString(col + "=VALUES(" + col + ")")
			if i != len(dml.columns)-1 {
				buf.WriteByte(',')
			}
		}
	}
	return []string{buf.String()}, [][]interface{}{dml.values}
}

// valuesHolder gens values holder like (?,?,?).
func valuesHolder(n int) string {
	builder := new(strings.Builder)
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

// genInsertOnDuplicateSQLMultipleRows generates a `INSERT` with multiple rows like 'INSERT INTO tb(a,b) VALUES (1,1),(2,2)'
// if onDuplicate, generates a `INSERT ON DUPLICATE KEY UPDATE` statement like 'INSERT INTO tb(a,b) VALUES (1,1),(2,2) ON DUPLICATE KEY UPDATE a=VALUES(a),b=VALUES(b)'.
func genInsertOnDuplicateSQLMultipleRows(onDuplicate bool, dmls []*DML) ([]string, [][]interface{}) {
	if len(dmls) == 0 {
		return nil, nil
	}

	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("INSERT INTO")
	buf.WriteString(" " + dmls[0].targetTableID + " (")
	for i, column := range dmls[0].columns {
		buf.WriteString(dbutil.ColumnName(column.Name.O))
		if i != len(dmls[0].columns)-1 {
			buf.WriteByte(',')
		} else {
			buf.WriteByte(')')
		}
	}
	buf.WriteString(" VALUES ")

	holder := valuesHolder(len(dmls[0].columns))
	for i := range dmls {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(holder)
	}

	if onDuplicate {
		buf.WriteString(" ON DUPLICATE KEY UPDATE ")
		for i, column := range dmls[0].columns {
			col := dbutil.ColumnName(column.Name.O)
			buf.WriteString(col + "=VALUES(" + col + ")")
			if i != len(dmls[0].columns)-1 {
				buf.WriteByte(',')
			}
		}
	}

	args := make([]interface{}, 0, len(dmls)*len(dmls[0].columns))
	for _, dml := range dmls {
		args = append(args, dml.values...)
	}
	return []string{buf.String()}, [][]interface{}{args}
}

// genDeleteSQLMultipleRows generates delete statement with multiple rows like 'DELETE FROM tb WHERE (a,b) IN (1,1),(2,2)'.
func genDeleteSQLMultipleRows(dmls []*DML) ([]string, [][]interface{}) {
	if len(dmls) == 0 {
		return nil, nil
	}

	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(dmls[0].targetTableID)
	buf.WriteString(" WHERE (")

	whereColumns, _ := dmls[0].whereColumnsAndValues()
	for i, column := range whereColumns {
		if i != len(whereColumns)-1 {
			buf.WriteString(dbutil.ColumnName(column) + ",")
		} else {
			buf.WriteString(dbutil.ColumnName(column) + ")")
		}
	}
	buf.WriteString(" IN (")

	holder := valuesHolder(len(whereColumns))
	args := make([]interface{}, 0, len(dmls)*len(dmls[0].columns))
	for i, dml := range dmls {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(holder)
		_, whereValues := dml.whereColumnsAndValues()
		// whereValues will have same length because we have checked it in genDMLsWithSameCols.
		args = append(args, whereValues...)
	}
	buf.WriteString(")")
	return []string{buf.String()}, [][]interface{}{args}
}

// genSQLMultipleRows generates multiple rows SQL with different dmlOpType.
func genSQLMultipleRows(op dmlOpType, dmls []*DML) (queries []string, args [][]interface{}) {
	if len(dmls) > 1 {
		log.L().Debug("generate DMLs with multiple rows", zap.Stringer("op", op), zap.Stringer("original op", dmls[0].op), zap.Int("rows", len(dmls)))
	}
	switch op {
	case insertDML:
		return genInsertOnDuplicateSQLMultipleRows(false, dmls)
	case insertOnDuplicateDML:
		return genInsertOnDuplicateSQLMultipleRows(true, dmls)
	case deleteDML:
		return genDeleteSQLMultipleRows(dmls)
	}
	return
}

// sameColumns check whether two DMLs have same columns.
func sameColumns(lhs *DML, rhs *DML) bool {
	var lhsCols, rhsCols []string
	if lhs.op == del {
		lhsCols, _ = lhs.whereColumnsAndValues()
		rhsCols, _ = rhs.whereColumnsAndValues()
	} else {
		// if source table is same, columns will be same.
		if lhs.sourceTable.Schema == rhs.sourceTable.Schema && lhs.sourceTable.Name == rhs.sourceTable.Name {
			return true
		}
		lhsCols = lhs.columnNames()
		rhsCols = rhs.columnNames()
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

// genDMLsWithSameCols group and gen dmls by same columns.
// in optimistic shard mode, different upstream tables may have different columns.
// e.g.
// insert into tb(a,b,c) values(1,1,1)
// insert into tb(a,b,d) values(2,2,2)
// we can only combine DMLs with same column names.
// all dmls should have same dmlOpType and same tableName.
func genDMLsWithSameCols(op dmlOpType, dmls []*DML) ([]string, [][]interface{}) {
	queries := make([]string, 0, len(dmls))
	args := make([][]interface{}, 0, len(dmls))
	var lastDML *DML
	var query []string
	var arg [][]interface{}
	groupDMLs := make([]*DML, 0, len(dmls))

	// group dmls by same columns
	for i, dml := range dmls {
		if i == 0 {
			lastDML = dml
		}
		if !sameColumns(lastDML, dml) {
			query, arg = genSQLMultipleRows(op, groupDMLs)
			queries = append(queries, query...)
			args = append(args, arg...)

			groupDMLs = groupDMLs[0:0]
			lastDML = dml
		}
		groupDMLs = append(groupDMLs, dml)
	}
	if len(groupDMLs) > 0 {
		query, arg = genSQLMultipleRows(op, groupDMLs)
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	return queries, args
}

// genDMLsWithSameTable groups and generates dmls with same table.
// all the dmls should have same dmlOpType.
func genDMLsWithSameTable(op dmlOpType, dmls []*DML) ([]string, [][]interface{}) {
	queries := make([]string, 0, len(dmls))
	args := make([][]interface{}, 0, len(dmls))
	var lastTable string
	groupDMLs := make([]*DML, 0, len(dmls))

	// for updateDML, generate SQLs one by one
	if op == updateDML {
		for _, dml := range dmls {
			query, arg := dml.genUpdateSQL()
			queries = append(queries, query...)
			args = append(args, arg...)
		}
		return queries, args
	}

	// group dmls with same table
	for i, dml := range dmls {
		if i == 0 {
			lastTable = dml.targetTableID
		}
		if lastTable != dml.targetTableID {
			query, arg := genDMLsWithSameCols(op, groupDMLs)
			queries = append(queries, query...)
			args = append(args, arg...)

			groupDMLs = groupDMLs[0:0]
			lastTable = dml.targetTableID
		}
		groupDMLs = append(groupDMLs, dml)
	}
	if len(groupDMLs) > 0 {
		query, arg := genDMLsWithSameCols(op, groupDMLs)
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	return queries, args
}

// genDMLsWithSameOp groups and generates dmls by dmlOpType.
// TODO: implement a volcano iterator interface for genDMLsWithSameXXX.
func genDMLsWithSameOp(dmls []*DML) ([]string, [][]interface{}) {
	queries := make([]string, 0, len(dmls))
	args := make([][]interface{}, 0, len(dmls))
	var lastOp dmlOpType
	groupDMLs := make([]*DML, 0, len(dmls))

	// group dmls with same dmlOp
	for i, dml := range dmls {
		curOp := dmlOpType(dml.op)
		// if update statement didn't update identify values, regard it as insert on duplicate.
		// if insert with safemode, regard it as insert on duplicate.
		if (curOp == updateDML && !dml.updateIdentify()) || (curOp == insertDML && dml.safeMode) {
			curOp = insertOnDuplicateDML
		}

		if i == 0 {
			lastOp = curOp
		}

		// now there are 4 situations: [insert, insert on duplicate(insert with safemode/update without identify keys), update(update identify keys), delete]
		if lastOp != curOp {
			query, arg := genDMLsWithSameTable(lastOp, groupDMLs)
			queries = append(queries, query...)
			args = append(args, arg...)

			groupDMLs = groupDMLs[0:0]
			lastOp = curOp
		}
		groupDMLs = append(groupDMLs, dml)
	}
	if len(groupDMLs) > 0 {
		query, arg := genDMLsWithSameTable(lastOp, groupDMLs)
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	return queries, args
}
