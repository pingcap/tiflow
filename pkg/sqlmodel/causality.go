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

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/tablecodec"
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
	}
	if r.postValues != nil {
		ret = append(ret, r.getCausalityString(r.postValues)...)
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

func (r *RowChange) getCausalityString(values []interface{}) []string {
	pkAndUks := r.whereHandle.UniqueIdxs
	if len(pkAndUks) == 0 {
		// the table has no PK/UK, all values of the row consists the causality key
		return []string{genKeyString(r.sourceTable.String(), r.sourceTableInfo.Columns, values)}
	}

	ret := make([]string, 0, len(pkAndUks))

	for _, indexCols := range pkAndUks {
		// TODO: should not support multi value index and generate the value
		// TODO: also fix https://github.com/pingcap/tiflow/issues/3286#issuecomment-971264282
		if indexCols.MVIndex {
			continue
		}
		cols, vals := getColsAndValuesOfIdx(r.sourceTableInfo.Columns, indexCols, values)
		// handle prefix index
		truncVals := truncateIndexValues(r.tiSessionCtx, r.sourceTableInfo, indexCols, cols, vals)
		key := genKeyString(r.sourceTable.String(), cols, truncVals)
		if len(key) > 0 { // ignore `null` value.
			ret = append(ret, key)
		} else {
			log.L().Debug("ignore empty key", zap.String("table", r.sourceTable.String()))
		}
	}

	if len(ret) == 0 {
		// the table has no PK/UK, or all UK are NULL. all values of the row
		// consists the causality key
		return []string{genKeyString(r.sourceTable.String(), r.sourceTableInfo.Columns, values)}
	}

	return ret
}
