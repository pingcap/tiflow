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

	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/pkg/terror"
)

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

func (s *Syncer) genAndFilterInsertDMLs(param *genDMLParam, filterExprs []expression.Expression) ([]*DML, error) {
	var (
		dataSeq         = param.data
		originalDataSeq = param.originalData
		columns         = param.columns
		ti              = param.sourceTableInfo
		dmls            = make([]*DML, 0, len(dataSeq))
	)

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
			skip, err := SkipDMLByExpression(originalValue, expr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip {
				s.filteredInsert.Add(1)
				continue RowLoop
			}
		}

		dmls = append(dmls, newDML(insert, param.safeMode, param.targetTableID, param.sourceTable, nil, value, nil, originalValue, columns, ti))
	}

	return dmls, nil
}

func (s *Syncer) genAndFilterUpdateDMLs(
	param *genDMLParam,
	oldValueFilters []expression.Expression,
	newValueFilters []expression.Expression,
) ([]*DML, error) {
	var (
		data         = param.data
		originalData = param.originalData
		columns      = param.columns
		ti           = param.sourceTableInfo
		dmls         = make([]*DML, 0, len(data)/2)
	)

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
			skip1, err := SkipDMLByExpression(oriOldValues, oldExpr, ti.Columns)
			if err != nil {
				return nil, err
			}
			skip2, err := SkipDMLByExpression(oriChangedValues, newExpr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip1 && skip2 {
				s.filteredUpdate.Add(1)
				// TODO: we skip generating the UPDATE SQL, so we left the old value here. Is this expected?
				continue RowLoop
			}
		}

		dmls = append(dmls, newDML(update, param.safeMode, param.targetTableID, param.sourceTable, oldValues, changedValues, oriOldValues, oriChangedValues, columns, ti))
	}

	return dmls, nil
}

func (s *Syncer) genAndFilterDeleteDMLs(param *genDMLParam, filterExprs []expression.Expression) ([]*DML, error) {
	var (
		dataSeq = param.originalData
		ti      = param.sourceTableInfo
		dmls    = make([]*DML, 0, len(dataSeq))
	)

RowLoop:
	for _, data := range dataSeq {
		if len(data) != len(ti.Columns) {
			return nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(ti.Columns), len(data))
		}

		value := extractValueFromData(data, ti.Columns)

		for _, expr := range filterExprs {
			skip, err := SkipDMLByExpression(value, expr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip {
				s.filteredDelete.Add(1)
				continue RowLoop
			}
		}
		dmls = append(dmls, newDML(del, false, param.targetTableID, param.sourceTable, nil, value, nil, value, ti.Columns, ti))
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

func getAvailableIndexColumn(ti *model.TableInfo, data []interface{}) *model.IndexInfo {
	fn := func(i int) bool {
		return data[i] == nil
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
	targetTableID   string
	sourceTable     *filter.Table
	op              opType
	oldValues       []interface{} // only for update SQL
	values          []interface{}
	columns         []*model.ColumnInfo
	sourceTableInfo *model.TableInfo
	originOldValues []interface{} // only for update SQL
	originValues    []interface{} // use to gen key and `WHERE`
	safeMode        bool
	key             string // use to detect causality
}

// newDML creates DML.
func newDML(op opType, safeMode bool, targetTableID string, sourceTable *filter.Table, oldValues, values, originOldValues, originValues []interface{}, columns []*model.ColumnInfo, sourceTableInfo *model.TableInfo) *DML {
	return &DML{
		op:              op,
		safeMode:        safeMode,
		targetTableID:   targetTableID,
		sourceTable:     sourceTable,
		oldValues:       oldValues,
		values:          values,
		columns:         columns,
		sourceTableInfo: sourceTableInfo,
		originOldValues: originOldValues,
		originValues:    originValues,
	}
}

// String returns the DML's string.
func (dml *DML) String() string {
	return fmt.Sprintf("[safemode: %t, targetTableID: %s, op: %s, columns: %v, oldValues: %v, values: %v]", dml.safeMode, dml.targetTableID, dml.op.String(), dml.columnNames(), dml.originOldValues, dml.originValues)
}

// identifyKeys gens keys by unique not null value.
// This is used for causality.
// PK or (UK + NOT NULL) or (UK + NULL + NOT NULL VALUE).
func (dml *DML) identifyKeys() []string {
	var keys []string
	// for UPDATE statement
	if dml.originOldValues != nil {
		keys = append(keys, genMultipleKeys(dml.sourceTableInfo, dml.originOldValues, dml.targetTableID)...)
	}

	if dml.originValues != nil {
		keys = append(keys, genMultipleKeys(dml.sourceTableInfo, dml.originValues, dml.targetTableID)...)
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

	defaultIndexColumns := findFitIndex(dml.sourceTableInfo)

	if defaultIndexColumns == nil {
		defaultIndexColumns = getAvailableIndexColumn(dml.sourceTableInfo, values)
	}
	if defaultIndexColumns != nil {
		columns, values = getColumnData(dml.sourceTableInfo.Columns, defaultIndexColumns, values)
	}

	columnNames := make([]string, 0, len(columns))
	for _, column := range columns {
		columnNames = append(columnNames, column.Name.O)
	}
	return columnNames, values
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

// genMultipleKeys gens keys with UNIQUE NOT NULL value.
// if not UNIQUE NOT NULL value, use table name instead.
func genMultipleKeys(ti *model.TableInfo, value []interface{}, table string) []string {
	multipleKeys := make([]string, 0, len(ti.Indices)+1)
	if ti.PKIsHandle {
		if pk := ti.GetPkColInfo(); pk != nil {
			cols := []*model.ColumnInfo{pk}
			vals := []interface{}{value[pk.Offset]}
			multipleKeys = append(multipleKeys, genKeyList(table, cols, vals))
		}
	}

	for _, indexCols := range ti.Indices {
		// PK also has a true Unique
		if !indexCols.Unique {
			continue
		}
		cols, vals := getColumnData(ti.Columns, indexCols, value)
		key := genKeyList(table, cols, vals)
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
		buf.WriteByte('`')
		buf.WriteString(strings.ReplaceAll(col, "`", "``"))
		if whereValues[i] == nil {
			buf.WriteString("` IS ?")
		} else {
			buf.WriteString("` = ?")
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
			fmt.Fprintf(&buf, "`%s` = ?", strings.ReplaceAll(column.Name.O, "`", "``"))
		} else {
			fmt.Fprintf(&buf, "`%s` = ?, ", strings.ReplaceAll(column.Name.O, "`", "``"))
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
	buf.Grow(256)
	buf.WriteString("INSERT INTO ")
	buf.WriteString(dml.targetTableID)
	buf.WriteString(" (")
	for i, column := range dml.columns {
		if i != len(dml.columns)-1 {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`,")
		} else {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`)")
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
			col := strings.ReplaceAll(column.Name.O, "`", "``")
			buf.WriteString("`" + col + "`=VALUES(`" + col + "`)")
			if i != len(dml.columns)-1 {
				buf.WriteByte(',')
			}
		}
	}
	return []string{buf.String()}, [][]interface{}{dml.values}
}
