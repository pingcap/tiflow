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

	"github.com/pingcap/tidb/pkg/expression/exprstatic"
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
	pkAndUks := r.whereHandle.causalityIdxs
	if len(pkAndUks) == 0 {
		// the table has no PK/UK, all values of the row consists the causality key
		return []string{genKeyString(sourceTable.String(), r.sourceTableInfo.Columns, values)}
	}

	ret := make([]string, 0, len(pkAndUks))

	values, fullValuesOK := r.fillVirtualGeneratedValues(values)

	for _, indexCols := range pkAndUks {
		// TODO: should not support multi value index and generate the value
		// TODO: also fix https://github.com/pingcap/tiflow/issues/3286#issuecomment-971264282
		if indexCols.MVIndex {
			continue
		}

		if indexHasHiddenColumn(indexCols, r.sourceTableInfo) && !fullValuesOK {
			continue
		}

		cols, vals := getColsAndValuesOfIdx(r.sourceTableInfo.Columns, indexCols, values)
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
		// the table has no PK/UK, or all UK are NULL. all values of the row
		// consists the causality key
		return []string{genKeyString(sourceTable.String(), r.sourceTableInfo.Columns, values)}
	}

	return ret
}

// fillVirtualGeneratedValues returns a copy of values extended to the full
// source column list, with hidden/virtual generated columns (such as the column
// backing an expression index) evaluated from their generated expression. The
// bool is false if any generated value cannot be computed, so the caller can
// skip the affected index instead of indexing the row-value slice out of range.
func (r *RowChange) fillVirtualGeneratedValues(values []any) ([]any, bool) {
	cols := r.sourceTableInfo.Columns
	if len(values) >= len(cols) {
		return values, true
	}
	if r.whereHandle.hiddenGeneratedColumnExprs == nil {
		return values, false
	}

	exprCtx := r.generatedColumnExprContext()
	// The expressions only depend on the schema, so they are built once and
	// cached on the (per-table) WhereHandle; here we just evaluate them per row.
	exprs, ok := r.whereHandle.hiddenGeneratedColumnExprs.getOrBuildExprs(exprCtx)
	if !ok {
		return values, false
	}

	// DM row values are compacted by dropping hidden columns. TiDB keeps hidden
	// expression-index columns at the end of TableInfo.Columns, so the row image
	// corresponds to the visible prefix.
	datums, err := utils.AdjustBinaryProtocolForDatum(r.tiSessionCtx, values, cols[:len(values)])
	if err != nil {
		log.L().Warn("cannot adjust row for generated column evaluation",
			zap.String("table", r.sourceTable.String()), zap.Error(err))
		return values, false
	}

	full := make([]any, len(cols))
	copy(full, values)
	fullDatums := make([]types.Datum, len(cols))
	copy(fullDatums, datums)

	// A generated column may depend on generated columns defined before it, so
	// evaluate generated columns in column order after visible values are in
	// their full TableInfo offsets.
	for _, col := range r.whereHandle.hiddenGeneratedColumnExprs.columns {
		expr, ok := exprs[col.Offset]
		if !ok {
			return values, false
		}
		d, err := expr.Eval(exprCtx.GetEvalCtx(), chunk.MutRowFromDatums(fullDatums).ToRow())
		if err != nil {
			log.L().Warn("cannot evaluate generated column expression",
				zap.String("table", r.sourceTable.String()),
				zap.String("column", col.Name.O), zap.Error(err))
			return values, false
		}
		full[col.Offset] = datumValue(d)
		fullDatums[col.Offset] = d
	}
	return full, true
}

func datumValue(d types.Datum) any {
	value := d.GetValue()
	if bs, ok := value.([]byte); ok {
		return string(bs)
	}
	return value
}

func (r *RowChange) generatedColumnExprContext() *exprstatic.ExprContext {
	vars := r.tiSessionCtx.GetSessionVars()
	charset, collation := vars.GetCharsetInfo()
	// TODO(joechenrh): Fetch downstream apply session charset/collation from
	// the downstream DB when they are not explicitly configured, so
	// generated-column evaluation fully matches downstream semantics.
	return exprstatic.NewExprContext(
		exprstatic.WithCharset(charset, collation),
		exprstatic.WithEvalCtx(exprstatic.NewEvalContext(
			exprstatic.WithLocation(vars.Location()),
			exprstatic.WithSQLMode(vars.SQLMode),
		)),
	)
}
