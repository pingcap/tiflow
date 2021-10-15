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

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/expression"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// genDMLParam stores pruned columns, data as well as the original columns, data, index.
type genDMLParam struct {
	tableID         string              // as a key in map like `schema`.`table`
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

func (s *Syncer) genInsertSQLs(param *genDMLParam, filterExprs []expression.Expression) ([]string, [][]string, [][]interface{}, error) {
	var (
		tableID         = param.tableID
		dataSeq         = param.data
		originalDataSeq = param.originalData
		columns         = param.columns
		ti              = param.sourceTableInfo
		sqls            = make([]string, 0, len(dataSeq))
		keys            = make([][]string, 0, len(dataSeq))
		values          = make([][]interface{}, 0, len(dataSeq))
	)

	insertOrReplace := "INSERT INTO"
	if param.safeMode {
		insertOrReplace = "REPLACE INTO"
	}
	sql := genInsertReplace(insertOrReplace, tableID, columns)

RowLoop:
	for dataIdx, data := range dataSeq {
		if len(data) != len(columns) {
			return nil, nil, nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(columns), len(data))
		}

		value := extractValueFromData(data, columns)
		originalValue := value
		if len(columns) != len(ti.Columns) {
			originalValue = extractValueFromData(originalDataSeq[dataIdx], ti.Columns)
		}

		for _, expr := range filterExprs {
			skip, err := SkipDMLByExpression(originalValue, expr, ti.Columns)
			if err != nil {
				return nil, nil, nil, err
			}
			if skip {
				s.filteredInsert.Add(1)
				continue RowLoop
			}
		}

		ks := genMultipleKeys(ti, originalValue, tableID)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

func (s *Syncer) genUpdateSQLs(
	param *genDMLParam,
	oldValueFilters []expression.Expression,
	newValueFilters []expression.Expression,
) ([]string, [][]string, [][]interface{}, error) {
	var (
		tableID             = param.tableID
		data                = param.data
		originalData        = param.originalData
		columns             = param.columns
		ti                  = param.sourceTableInfo
		defaultIndexColumns = findFitIndex(ti)
		replaceSQL          string // `REPLACE INTO` SQL
		sqls                = make([]string, 0, len(data)/2)
		keys                = make([][]string, 0, len(data)/2)
		values              = make([][]interface{}, 0, len(data)/2)
	)

	if param.safeMode {
		replaceSQL = genInsertReplace("REPLACE INTO", tableID, columns)
	}

RowLoop:
	for i := 0; i < len(data); i += 2 {
		oldData := data[i]
		changedData := data[i+1]
		oriOldData := originalData[i]
		oriChangedData := originalData[i+1]

		if len(oldData) != len(changedData) {
			return nil, nil, nil, terror.ErrSyncerUnitDMLOldNewValueMismatch.Generate(len(oldData), len(changedData))
		}

		if len(oldData) != len(columns) {
			return nil, nil, nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(columns), len(oldData))
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
				return nil, nil, nil, err
			}
			skip2, err := SkipDMLByExpression(oriChangedValues, newExpr, ti.Columns)
			if err != nil {
				return nil, nil, nil, err
			}
			if skip1 && skip2 {
				s.filteredUpdate.Add(1)
				// TODO: we skip generating the UPDATE SQL, so we left the old value here. Is this expected?
				continue RowLoop
			}
		}

		if defaultIndexColumns == nil {
			defaultIndexColumns = getAvailableIndexColumn(ti, oriOldValues)
		}

		ks := genMultipleKeys(ti, oriOldValues, tableID)
		ks = append(ks, genMultipleKeys(ti, oriChangedValues, tableID)...)

		if param.safeMode {
			// generate delete sql from old data
			sql, value := genDeleteSQL(tableID, oriOldValues, ti.Columns, defaultIndexColumns)
			sqls = append(sqls, sql)
			values = append(values, value)
			keys = append(keys, ks)
			// generate replace sql from new data
			sqls = append(sqls, replaceSQL)
			values = append(values, changedValues)
			keys = append(keys, ks)
			continue
		}

		// NOTE: move these variables outer of `for` if needed (to reuse).
		updateColumns := make([]*model.ColumnInfo, 0, indexColumnsCount(defaultIndexColumns))
		updateValues := make([]interface{}, 0, indexColumnsCount(defaultIndexColumns))
		for j := range oldValues {
			updateColumns = append(updateColumns, columns[j])
			updateValues = append(updateValues, changedValues[j])
		}

		// ignore no changed sql
		if len(updateColumns) == 0 {
			continue
		}

		value := make([]interface{}, 0, len(oldData))
		value = append(value, updateValues...)

		whereColumns, whereValues := ti.Columns, oriOldValues
		if defaultIndexColumns != nil {
			whereColumns, whereValues = getColumnData(ti.Columns, defaultIndexColumns, oriOldValues)
		}

		value = append(value, whereValues...)

		sql := genUpdateSQL(tableID, updateColumns, whereColumns, whereValues)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

func (s *Syncer) genDeleteSQLs(param *genDMLParam, filterExprs []expression.Expression) ([]string, [][]string, [][]interface{}, error) {
	var (
		tableID             = param.tableID
		dataSeq             = param.originalData
		ti                  = param.sourceTableInfo
		defaultIndexColumns = findFitIndex(ti)
		sqls                = make([]string, 0, len(dataSeq))
		keys                = make([][]string, 0, len(dataSeq))
		values              = make([][]interface{}, 0, len(dataSeq))
	)

RowLoop:
	for _, data := range dataSeq {
		if len(data) != len(ti.Columns) {
			return nil, nil, nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(ti.Columns), len(data))
		}

		value := extractValueFromData(data, ti.Columns)

		for _, expr := range filterExprs {
			skip, err := SkipDMLByExpression(value, expr, ti.Columns)
			if err != nil {
				return nil, nil, nil, err
			}
			if skip {
				s.filteredDelete.Add(1)
				continue RowLoop
			}
		}

		if defaultIndexColumns == nil {
			defaultIndexColumns = getAvailableIndexColumn(ti, value)
		}
		ks := genMultipleKeys(ti, value, tableID)

		sql, value := genDeleteSQL(tableID, value, ti.Columns, defaultIndexColumns)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

// genInsertReplace generates a DML for `INSERT INTO` or `REPLCATE INTO`.
// the returned SQL with placeholders for `VALUES`.
func genInsertReplace(op, table string, columns []*model.ColumnInfo) string {
	// NOTE: use sync.Pool to hold the builder if needed later.
	var buf strings.Builder
	buf.Grow(256)
	buf.WriteString(op)
	buf.WriteString(" " + table + " (")
	for i, column := range columns {
		if i != len(columns)-1 {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`,")
		} else {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`)")
		}
	}
	buf.WriteString(" VALUES (")

	// placeholders
	for i := range columns {
		if i != len(columns)-1 {
			buf.WriteString("?,")
		} else {
			buf.WriteString("?)")
		}
	}
	return buf.String()
}

// genUpdateSQL generates a `UPDATE` SQL with `SET` and `WHERE`.
func genUpdateSQL(table string, updateColumns, whereColumns []*model.ColumnInfo, whereValues []interface{}) string {
	var buf strings.Builder
	buf.Grow(2048)
	buf.WriteString("UPDATE ")
	buf.WriteString(table)
	buf.WriteString(" SET ")

	for i, column := range updateColumns {
		if i == len(updateColumns)-1 {
			fmt.Fprintf(&buf, "`%s` = ?", strings.ReplaceAll(column.Name.O, "`", "``"))
		} else {
			fmt.Fprintf(&buf, "`%s` = ?, ", strings.ReplaceAll(column.Name.O, "`", "``"))
		}
	}

	buf.WriteString(" WHERE ")
	genWhere(&buf, whereColumns, whereValues)
	buf.WriteString(" LIMIT 1")
	return buf.String()
}

// genDeleteSQL generates a `DELETE FROM` SQL with `WHERE`.
func genDeleteSQL(table string, value []interface{}, columns []*model.ColumnInfo, indexColumns *model.IndexInfo) (string, []interface{}) {
	whereColumns, whereValues := columns, value
	if indexColumns != nil {
		whereColumns, whereValues = getColumnData(columns, indexColumns, value)
	}

	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(table)
	buf.WriteString(" WHERE ")
	genWhere(&buf, whereColumns, whereValues)
	buf.WriteString(" LIMIT 1")

	return buf.String(), whereValues
}

func indexColumnsCount(index *model.IndexInfo) int {
	if index == nil {
		return 0
	}
	return len(index.Columns)
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
		buf.WriteString(columns[i].Name.String())
		buf.WriteString(".")
	}
	if buf.Len() == 0 {
		log.L().Debug("all value are nil, no key generated", zap.String("table", table))
		return "" // all values are `null`.
	}
	buf.WriteString(table)
	return buf.String()
}

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
			// TODO: break here? one unique index is enough?
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

func genWhere(buf *strings.Builder, columns []*model.ColumnInfo, data []interface{}) {
	for i, col := range columns {
		if i != 0 {
			buf.WriteString(" AND ")
		}
		buf.WriteByte('`')
		buf.WriteString(strings.ReplaceAll(col.Name.O, "`", "``"))
		if data[i] == nil {
			buf.WriteString("` IS ?")
		} else {
			buf.WriteString("` = ?")
		}
	}
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
