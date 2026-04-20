// Copyright 2024 PingCAP, Inc.
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

package downstreamadapter

import (
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/pkg/kv"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// 全部 copy from mounter

var emptyBytes = make([]byte, 0)

const (
	sizeOfEmptyColumn = int(unsafe.Sizeof(model.Column{}))
	sizeOfEmptyBytes  = int(unsafe.Sizeof(emptyBytes))
	sizeOfEmptyString = int(unsafe.Sizeof(""))
)

// decodeRowV2 decodes value data using new encoding format.
// Ref: https://github.com/pingcap/tidb/pull/12634
//
//	https://github.com/pingcap/tidb/blob/master/docs/design/2018-07-19-row-format.md
func decodeRowV2(
	decoder *rowcodec.DatumMapDecoder, data []byte,
) (map[int64]types.Datum, error) {
	datums, err := decoder.DecodeToDatumMap(data, nil)
	if err != nil {
		return datums, cerror.WrapError(cerror.ErrDecodeRowToDatum, err)
	}
	return datums, nil
}

func decodeRow(
	rawValue []byte, recordID kv.Handle, tableInfo *model.TableInfo, isPreColumns bool,
) (map[int64]types.Datum, bool, error) {
	if len(rawValue) == 0 {
		return map[int64]types.Datum{}, false, nil
	}
	handleColIDs, handleColFt, reqCols := tableInfo.GetRowColInfos()
	var (
		datums map[int64]types.Datum
		err    error
	)

	decoder := rowcodec.NewDatumMapDecoder(reqCols, time.Local)
	datums, err = decodeRowV2(decoder, rawValue)

	if err != nil {
		return nil, false, errors.Trace(err)
	}

	datums, err = tablecodec.DecodeHandleToDatumMap(
		recordID, handleColIDs, handleColFt, time.Local, datums)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	return datums, true, nil
}

func sizeOfDatum(d types.Datum) int {
	array := [...]types.Datum{d}
	return int(types.EstimatedMemUsage(array[:], 1))
}

func sizeOfString(s string) int {
	// string data size + string struct size.
	return len(s) + sizeOfEmptyString
}

func sizeOfBytes(b []byte) int {
	// bytes data size + bytes struct size.
	return len(b) + sizeOfEmptyBytes
}

// formatColVal return interface{} need to meet the same requirement as getDefaultOrZeroValue
func formatColVal(datum types.Datum, col *timodel.ColumnInfo) (
	value interface{}, size int, warn string, err error,
) {
	if datum.IsNull() {
		return nil, 0, "", nil
	}
	switch col.GetType() {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		v := datum.GetMysqlTime().String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeDuration:
		v := datum.GetMysqlDuration().String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeJSON:
		v := datum.GetMysqlJSON().String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeNewDecimal:
		d := datum.GetMysqlDecimal()
		if d == nil {
			// nil takes 0 byte.
			return nil, 0, "", nil
		}
		v := d.String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeEnum:
		v := datum.GetMysqlEnum().Value
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), "", nil
	case mysql.TypeSet:
		v := datum.GetMysqlSet().Value
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), "", nil
	case mysql.TypeBit:
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		v, err := datum.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), "", err
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		b := datum.GetBytes()
		if b == nil {
			b = emptyBytes
		}
		return b, sizeOfBytes(b), "", nil
	case mysql.TypeFloat:
		v := datum.GetFloat32()
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 1) || math.IsInf(float64(v), -1) {
			warn = fmt.Sprintf("the value is invalid in column: %f", v)
			v = 0
		}
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), warn, nil
	case mysql.TypeDouble:
		v := datum.GetFloat64()
		if math.IsNaN(v) || math.IsInf(v, 1) || math.IsInf(v, -1) {
			warn = fmt.Sprintf("the value is invalid in column: %f", v)
			v = 0
		}
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), warn, nil
	default:
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		return datum.GetValue(), sizeOfDatum(datum), "", nil
	}
}

// Scenarios when call this function:
// (1) column define default null at creating + insert without explicit column
// (2) alter table add column default xxx + old existing data
// (3) amend + insert without explicit column + alter table add column default xxx
// (4) online DDL drop column + data insert at state delete-only
//
// getDefaultOrZeroValue return interface{} need to meet to require type in
// https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
// Supported type is: nil, basic type(Int, Int8,..., Float32, Float64, String), Slice(uint8), other types not support
// TODO: Check default expr support
func getDefaultOrZeroValue(
	col *timodel.ColumnInfo, tz *time.Location,
) (types.Datum, any, int, string, error) {
	var (
		d   types.Datum
		err error
	)
	// NOTICE: SHOULD use OriginDefaultValue here, more info pls ref to
	// https://github.com/pingcap/tiflow/issues/4048
	// FIXME: Too many corner cases may hit here, like type truncate, timezone
	// (1) If this column is uk(no pk), will cause data inconsistency in Scenarios(2)
	// (2) If not fix here, will cause data inconsistency in Scenarios(3) directly
	// Ref: https://github.com/pingcap/tidb/blob/d2c352980a43bb593db81fd1db996f47af596d91/table/column.go#L489
	if col.GetOriginDefaultValue() != nil {
		datum := types.NewDatum(col.GetOriginDefaultValue())
		d, err = datum.ConvertTo(types.DefaultStmtNoWarningContext, &col.FieldType)
		if err != nil {
			return d, d.GetValue(), sizeOfDatum(d), "", errors.Trace(err)
		}
		switch col.GetType() {
		case mysql.TypeTimestamp:
			t := d.GetMysqlTime()
			err = t.ConvertTimeZone(time.UTC, tz)
			if err != nil {
				return d, d.GetValue(), sizeOfDatum(d), "", errors.Trace(err)
			}
			d.SetMysqlTime(t)
		}
	} else if !mysql.HasNotNullFlag(col.GetFlag()) {
		// NOTICE: NotNullCheck need do after OriginDefaultValue check, as when TiDB meet "amend + add column default xxx",
		// ref: https://github.com/pingcap/ticdc/issues/3929
		// must use null if TiDB not write the column value when default value is null
		// and the value is null, see https://github.com/pingcap/tidb/issues/9304
		d = types.NewDatum(nil)
	} else {
		switch col.GetType() {
		case mysql.TypeEnum:
			// For enum type, if no default value and not null is set,
			// the default value is the first element of the enum list
			name := col.FieldType.GetElem(0)
			enumValue, err := types.ParseEnumName(col.FieldType.GetElems(), name, col.GetCollate())
			if err != nil {
				return d, nil, 0, "", errors.Trace(err)
			}
			d = types.NewMysqlEnumDatum(enumValue)
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
			return d, emptyBytes, sizeOfEmptyBytes, "", nil
		default:
			d = table.GetZeroValue(col)
			if d.IsNull() {
				log.Error("meet unsupported column type", zap.String("columnInfo", col.FieldType.String()))
			}
		}
	}
	v, size, warn, err := formatColVal(d, col)
	return d, v, size, warn, err
}

func datum2Column(
	tableInfo *model.TableInfo, datums map[int64]types.Datum, tz *time.Location,
) ([]*model.ColumnData, []types.Datum, []*timodel.ColumnInfo, error) {
	cols := make([]*model.ColumnData, len(tableInfo.RowColumnsOffset))
	rawCols := make([]types.Datum, len(tableInfo.RowColumnsOffset))

	// columnInfos should have the same length and order with cols
	columnInfos := make([]*timodel.ColumnInfo, len(tableInfo.RowColumnsOffset))

	for _, colInfo := range tableInfo.Columns {
		if !model.IsColCDCVisible(colInfo) {
			log.Debug("skip the column which is not visible",
				zap.String("table", tableInfo.Name.O), zap.String("column", colInfo.Name.O))
			continue
		}

		colID := colInfo.ID
		colDatum, exist := datums[colID]

		var (
			colValue interface{}
			size     int
			warn     string
			err      error
		)
		if exist {
			colValue, size, warn, err = formatColVal(colDatum, colInfo)
		} else {
			colDatum, colValue, size, warn, err = getDefaultOrZeroValue(colInfo, tz)
		}
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		if warn != "" {
			log.Warn(warn, zap.String("table", tableInfo.TableName.String()),
				zap.String("column", colInfo.Name.String()))
		}

		offset := tableInfo.RowColumnsOffset[colID]
		rawCols[offset] = colDatum
		cols[offset] = &model.ColumnData{
			ColumnID: colID,
			Value:    colValue,
			// ApproximateBytes = column data size + column struct size
			ApproximateBytes: size + sizeOfEmptyColumn,
		}
		columnInfos[offset] = colInfo
	}
	return cols, rawCols, columnInfos, nil
}

func decodeRowKVEntry(tableInfo *model.TableInfo,
	rawKey []byte,
	rawValue []byte,
	rawOldValue []byte) ([]*model.ColumnData, []*model.ColumnData, error) {
	recordID, err := tablecodec.DecodeRowKey(rawKey)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	row, rowExist, err := decodeRow(rawValue, recordID, tableInfo, false)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	preRow, preRowExist, err := decodeRow(rawOldValue, recordID, tableInfo, true)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var (
		preCols []*model.ColumnData
		cols    []*model.ColumnData
	)

	if preRowExist {
		// FIXME(leoppro): using pre table info to mounter pre column datum
		// the pre column and current column in one event may using different table info
		preCols, _, _, err = datum2Column(tableInfo, preRow, time.Local)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}

	if rowExist {
		cols, _, _, err = datum2Column(tableInfo, row, time.Local)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}

	return preCols, cols, nil
}
