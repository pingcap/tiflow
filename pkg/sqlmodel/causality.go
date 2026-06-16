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
	"strconv"
	"strings"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// CausalityKeys returns all string representation of causality keys. If two row
// changes has the same causality keys, they must be replicated sequentially.
func (r *RowChange) CausalityKeys() []string {
	r.lazyInitWhereHandle()

	ret := make([]string, 0, 1)
	if r.preValues != nil {
		ret = append(ret, r.getCausalityString(r.preValues)...)
		ret = append(ret, r.getForeignKeyCausalityString(r.preValues)...)
	}
	if r.postValues != nil {
		ret = append(ret, r.getCausalityString(r.postValues)...)
		ret = append(ret, r.getForeignKeyCausalityString(r.postValues)...)
	}
	return ret
}

func columnNeeds2LowerCase(col *timodel.ColumnInfo) bool {
	switch col.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return collationNeeds2LowerCase(col.GetCollate())
	}
	return false
}

func collationNeeds2LowerCase(collation string) bool {
	return strings.HasSuffix(collation, "_ci")
}

func columnValue2String(value interface{}) string {
	var data string
	switch v := value.(type) {
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

func genKeyString(
	table string,
	columns []*timodel.ColumnInfo,
	values []interface{},
) string {
	var buf strings.Builder
	for i, data := range values {
		if data == nil {
			log.L().Debug("ignore null value",
				zap.String("column", columns[i].Name.O),
				zap.String("table", table))
			continue // ignore `null` value.
		}
		// one column key looks like:`column_val.column_name.`

		val := columnValue2String(data)
		if columnNeeds2LowerCase(columns[i]) {
			val = strings.ToLower(val)
		}
		buf.WriteString(val)
		buf.WriteString(".")
		buf.WriteString(columns[i].Name.L)
		buf.WriteString(".")
	}
	if buf.Len() == 0 {
		log.L().Debug("all value are nil, no key generated",
			zap.String("table", table))
		return "" // all values are `null`.
	}
	buf.WriteString(table)
	return buf.String()
}

// truncateIndexValues truncate prefix index from data.
func truncateIndexValues(
	ctx sessionctx.Context,
	ti *timodel.TableInfo,
	indexColumns *timodel.IndexInfo,
	tiColumns []*timodel.ColumnInfo,
	data []interface{},
) []interface{} {
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

func (r *RowChange) getForeignKeyCausalityString(values []interface{}) []string {
	if len(r.foreignKeyRelations) == 0 {
		return nil
	}

	keys := make([]string, 0, len(r.foreignKeyRelations))

	for _, relation := range r.foreignKeyRelations {
		if len(relation.ChildColumnIdx) == 0 || len(relation.ParentColumns) == 0 {
			continue
		}

		relationValues := make([]interface{}, len(relation.ChildColumnIdx))
		skip := false
		for i, idx := range relation.ChildColumnIdx {
			if idx >= len(values) {
				log.L().Debug("skip foreign key causality relation with out-of-range child column index",
					zap.String("childTable", r.sourceTable.String()),
					zap.String("parentTable", relation.ParentTable),
					zap.Int("childColumnIndex", idx),
					zap.Int("valueCount", len(values)))
				skip = true
				break
			}

			relationValues[i] = values[idx]
			if relationValues[i] == nil {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		key := genKeyString(relation.ParentTable, relation.ParentColumns, relationValues)
		if len(key) > 0 {
			keys = append(keys, key)
		}
	}

	return keys
}

func (r *RowChange) getCausalityString(values []interface{}) []string {
	sourceTable := r.sourceTable
	if r.causalityKeySourceTable != nil {
		// Only causality keys use this table name; r.sourceTable keeps the original source table.
		sourceTable = r.causalityKeySourceTable
	}
	causalityIndexes := r.whereHandle.causalityIdxs
	if len(causalityIndexes) == 0 {
		// No causality index: use the whole row.
		columns := r.whereHandle.rowMapper.columnsForValues(r.sourceTableInfo.Columns, values)
		return []string{genKeyString(sourceTable.String(), columns, values)}
	}

	ret := make([]string, 0, len(causalityIndexes))

	values, fullValuesOK := r.fillVirtualGeneratedValues(values)

	for _, indexCols := range causalityIndexes {
		// TODO: should not support multi value index and generate the value
		// TODO: also fix https://github.com/pingcap/tiflow/issues/3286#issuecomment-971264282
		if indexCols.MVIndex {
			continue
		}

		// Only hidden-column indexes require materialized values.
		if indexHasHiddenColumn(indexCols, r.sourceTableInfo) && !fullValuesOK {
			continue
		}

		cols, vals, ok := r.whereHandle.rowMapper.columnsAndValuesByIndex(
			r.sourceTableInfo.Columns,
			indexCols,
			values,
		)
		if !ok {
			continue
		}
		// handle prefix index
		truncVals := truncateIndexValues(r.tiSessionCtx, r.sourceTableInfo, indexCols, cols, vals)
		key := genKeyString(sourceTable.String(), cols, truncVals)
		if len(key) > 0 { // ignore `null` value.
			ret = append(ret, key)
		} else {
			log.L().Debug("ignore empty key", zap.String("table", r.sourceTable.String()))
		}
	}

	if len(ret) == 0 {
		// No index key was generated; fall back to the whole row.
		columns := r.whereHandle.rowMapper.columnsForValues(r.sourceTableInfo.Columns, values)
		return []string{genKeyString(sourceTable.String(), columns, values)}
	}

	return ret
}

// fillVirtualGeneratedValues returns a copy of values extended to the full
// source column list, with hidden/virtual generated columns evaluated. The bool
// reports whether full values are available.
func (r *RowChange) fillVirtualGeneratedValues(values []any) ([]any, bool) {
	if r.whereHandle.hiddenGeneratedColumnExprCache == nil {
		return values, false
	}

	cols := r.sourceTableInfo.Columns
	if r.whereHandle.rowMapper.isFullValues(values) {
		return values, true
	}

	exprs, exprCtx, ok := r.whereHandle.hiddenGeneratedColumnExprCache.getOrBuildExprs(r.tiSessionCtx)
	if !ok {
		return values, false
	}

	visibleCols := r.whereHandle.rowMapper.visibleColumns
	if len(values) != len(visibleCols) {
		return values, false
	}

	datums, err := utils.AdjustBinaryProtocolForDatum(r.tiSessionCtx, values, visibleCols)
	if err != nil {
		log.L().Debug("cannot adjust row for generated column evaluation",
			zap.String("table", r.sourceTable.String()), zap.Error(err))
		return values, false
	}

	full := make([]any, len(cols))
	fullDatums := make([]types.Datum, len(cols))
	for i, col := range visibleCols {
		full[col.Offset] = values[i]
		fullDatums[col.Offset] = datums[i]
	}

	mutRow := chunk.MutRowFromDatums(fullDatums)
	for _, col := range r.whereHandle.hiddenGeneratedColumnExprCache.columns {
		expr, ok := exprs[col.Offset]
		if !ok {
			return values, false
		}
		d, err := expr.Eval(exprCtx.GetEvalCtx(), mutRow.ToRow())
		if err != nil {
			log.L().Debug("cannot evaluate generated column expression",
				zap.String("table", r.sourceTable.String()),
				zap.String("column", col.Name.O), zap.Error(err))
			return values, false
		}
		full[col.Offset] = datumValue(d)
		fullDatums[col.Offset] = d
		mutRow.SetDatum(col.Offset, d)
	}
	return full, true
}

func datumValue(d types.Datum) any {
	if d.IsNull() {
		return nil
	}
	switch d.Kind() {
	case types.KindMysqlDecimal,
		types.KindMysqlTime,
		types.KindMysqlDuration,
		types.KindMysqlEnum,
		types.KindMysqlSet,
		types.KindMysqlJSON,
		types.KindBinaryLiteral,
		types.KindMysqlBit,
		types.KindVectorFloat32:
		s, err := d.ToString()
		if err == nil {
			return s
		}
	}
	value := d.GetValue()
	// Keep byte values comparable for identityUpdated's != check.
	if bs, ok := value.([]byte); ok {
		return string(bs)
	}
	return value
}
