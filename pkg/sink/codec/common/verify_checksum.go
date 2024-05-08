// Copyright 2023 PingCAP, Inc.
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

package common

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// VerifyChecksum calculate the checksum value, and compare it with the expected one, return error if not identical.
func VerifyChecksum(columns []*model.Column, columnInfo []*timodel.ColumnInfo, expected uint32) error {
	// if expected is 0, it means the checksum is not enabled, so we don't need to verify it.
	// the data maybe restored by br, and the checksum is not enabled, so no expected here.
	if expected == 0 {
		return nil
	}
	checksum, err := calculateChecksum(columns, columnInfo)
	if err != nil {
		return errors.Trace(err)
	}
	if checksum != expected {
		log.Error("checksum mismatch",
			zap.Uint32("expected", expected),
			zap.Uint32("actual", checksum))
		return errors.New("checksum mismatch")
	}

	return nil
}

// calculate the checksum, caller should make sure all columns is ordered by the column's id.
// by follow: https://github.com/pingcap/tidb/blob/e3417913f58cdd5a136259b902bf177eaf3aa637/util/rowcodec/common.go#L294
func calculateChecksum(columns []*model.Column, columnInfo []*timodel.ColumnInfo) (uint32, error) {
	var (
		checksum uint32
		err      error
	)
	buf := make([]byte, 0)
	for idx, col := range columns {
		if len(buf) > 0 {
			buf = buf[:0]
		}
		buf, err = buildChecksumBytes(buf, col.Value, columnInfo[idx].GetType())
		if err != nil {
			return 0, errors.Trace(err)
		}
		checksum = crc32.Update(checksum, crc32.IEEETable, buf)
	}
	return checksum, nil
}

// buildChecksumBytes append value to the buf, mysqlType is used to convert value interface to concrete type.
// by follow: https://github.com/pingcap/tidb/blob/e3417913f58cdd5a136259b902bf177eaf3aa637/util/rowcodec/common.go#L308
func buildChecksumBytes(buf []byte, value interface{}, mysqlType byte) ([]byte, error) {
	if value == nil {
		return buf, nil
	}

	switch mysqlType {
	// TypeTiny, TypeShort, TypeInt32 is encoded as int32
	// TypeLong is encoded as int32 if signed, else int64.
	// TypeLongLong is encoded as int64 if signed, else uint64,
	// if bigintUnsignedHandlingMode set as string, encode as string.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
		var (
			v   uint64
			err error
		)
		switch a := value.(type) {
		case int32:
			v = uint64(a)
		case uint32:
			v = uint64(a)
		case int64:
			v = uint64(a)
		case uint64:
			v = a
		case string:
			v, err = strconv.ParseUint(a, 10, 64)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case map[string]interface{}:
			// this may only happen for bigint larger than math.uint64
			v = uint64(a["value"].(int64))
		default:
			log.Panic("unknown golang type for the integral value",
				zap.Any("value", value), zap.Any("mysqlType", mysqlType))
		}
		buf = binary.LittleEndian.AppendUint64(buf, v)
	// TypeFloat encoded as float32, TypeDouble encoded as float64
	case mysql.TypeFloat, mysql.TypeDouble:
		var v float64
		switch a := value.(type) {
		case float32:
			v = float64(a)
		case float64:
			v = a
		}
		if math.IsInf(v, 0) || math.IsNaN(v) {
			v = 0
		}
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(v))
	// TypeEnum, TypeSet encoded as string
	// but convert to int by the getColumnValue function
	case mysql.TypeEnum, mysql.TypeSet:
		var number uint64
		switch v := value.(type) {
		case uint64:
			number = v
		case int64:
			number = uint64(v)
		}
		buf = binary.LittleEndian.AppendUint64(buf, number)
	case mysql.TypeBit:
		var number uint64
		switch v := value.(type) {
		// TypeBit encoded as bytes for the avro protocol
		case []byte:
			number = MustBinaryLiteralToInt(v)
		// TypeBit encoded as uint64 for the simple protocol
		case uint64:
			number = v
		}
		buf = binary.LittleEndian.AppendUint64(buf, number)
	// encoded as bytes if binary flag set to true, else string
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob,
		mysql.TypeLongBlob, mysql.TypeBlob:
		switch a := value.(type) {
		case string:
			buf = appendLengthValue(buf, []byte(a))
		case []byte:
			buf = appendLengthValue(buf, a)
		default:
			log.Panic("unknown golang type for the string value",
				zap.Any("value", value), zap.Any("mysqlType", mysqlType))
		}
	case mysql.TypeTimestamp:
		location := "Local"
		var ts string
		switch data := value.(type) {
		case map[string]interface{}:
			ts = data["value"].(string)
			location = data["location"].(string)
		case string:
			ts = data
		}
		ts, err := util.ConvertTimezone(ts, location)
		if err != nil {
			log.Panic("convert timestamp to timezone failed",
				zap.String("timestamp", ts), zap.String("location", location),
				zap.Error(err))
		}
		buf = appendLengthValue(buf, []byte(ts))
	// all encoded as string
	case mysql.TypeDatetime, mysql.TypeDate, mysql.TypeDuration, mysql.TypeNewDate:
		buf = appendLengthValue(buf, []byte(value.(string)))
	// encoded as string if decimalHandlingMode set to string, it's required to enable checksum.
	case mysql.TypeNewDecimal:
		buf = appendLengthValue(buf, []byte(value.(string)))
	// encoded as string
	case mysql.TypeJSON:
		buf = appendLengthValue(buf, []byte(value.(string)))
	// this should not happen, does not take into the checksum calculation.
	case mysql.TypeNull, mysql.TypeGeometry:
		// do nothing
	default:
		return buf, errors.New("invalid type for the checksum calculation")
	}
	return buf, nil
}

func appendLengthValue(buf []byte, val []byte) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(val)))
	buf = append(buf, val...)
	return buf
}
