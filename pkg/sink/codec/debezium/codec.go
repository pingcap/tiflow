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

package debezium

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type dbzCodec struct {
	config    *common.Config
	clusterID string
	nowFunc   func() time.Time
}

func (c *dbzCodec) writeDebeziumFieldValues(
	writer *util.JSONWriter,
	fieldName string,
	cols []*model.ColumnData,
	tableInfo *model.TableInfo,
) error {
	var err error
	colInfos := tableInfo.GetColInfosForRowChangedEvent()
	writer.WriteObjectField(fieldName, func() {
		for i, col := range cols {
			if col == nil {
				continue
			}
			colx := model.GetColumnDataX(col, tableInfo)
			err = c.writeDebeziumFieldValue(writer, colx, colInfos[i].Ft)
			if err != nil {
				log.Error("write Debezium field value meet error", zap.Error(err))
				break
			}
		}
	})
	return err
}

func (c *dbzCodec) writeDebeziumFieldSchema(
	writer *util.JSONWriter,
	col model.ColumnDataX,
	ft *types.FieldType,
) {
	switch col.GetType() {
	case mysql.TypeBit:
		n := ft.GetFlen()
		var v uint64
		var err error
		if col.GetDefaultValue() != nil {
			val, ok := col.GetDefaultValue().(string)
			if !ok {
				return
			}
			v, err = strconv.ParseUint(parseBit(val, n), 2, 64)
			if err != nil {
				return
			}
		}
		if n == 1 {
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "boolean")
				writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
				writer.WriteStringField("field", col.GetName())
				if col.GetDefaultValue() != nil {
					writer.WriteBoolField("default", v != 0) // bool
				}
			})
		} else {
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "bytes")
				writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
				writer.WriteStringField("name", "io.debezium.data.Bits")
				writer.WriteIntField("version", 1)
				writer.WriteObjectField("parameters", func() {
					writer.WriteStringField("length", fmt.Sprintf("%d", n))
				})
				writer.WriteStringField("field", col.GetName())
				if col.GetDefaultValue() != nil {
					c.writeBinaryField(writer, "default", getBitFromUint64(n, v)) // binary
				}
			})
		}

	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				writer.WriteAnyField("default", col.GetDefaultValue())
			}
		})

	case mysql.TypeEnum:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.data.Enum")
			writer.WriteIntField("version", 1)
			writer.WriteObjectField("parameters", func() {
				elems := ft.GetElems()
				parameters := make([]string, 0, len(elems))
				for _, ele := range elems {
					parameters = append(parameters, common.EscapeEnumAndSetOptions(ele))
				}
				writer.WriteStringField("allowed", strings.Join(parameters, ","))
			})
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				writer.WriteAnyField("default", col.GetDefaultValue())
			}
		})

	case mysql.TypeSet:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.data.EnumSet")
			writer.WriteIntField("version", 1)
			writer.WriteObjectField("parameters", func() {
				writer.WriteStringField("allowed", strings.Join(ft.GetElems(), ","))
			})
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				writer.WriteAnyField("default", col.GetDefaultValue())
			}
		})

	case mysql.TypeDate, mysql.TypeNewDate:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int32")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.Date")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				t, err := time.Parse("2006-01-02", v)
				if err != nil {
					// For example, time may be invalid like 1000-00-00
					// return nil, nil
					if mysql.HasNotNullFlag(ft.GetFlag()) {
						writer.WriteInt64Field("default", 0)
					}
					return
				}
				year := t.Year()
				if year < 70 {
					// treats "0018" as 2018
					t = t.AddDate(2000, 0, 0)
				} else if year < 100 {
					//  treats "0099" as 1999
					t = t.AddDate(1900, 0, 0)
				}
				writer.WriteInt64Field("default", t.UTC().Unix()/60/60/24)
			}
		})

	case mysql.TypeDatetime:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int64")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			if ft.GetDecimal() <= 3 {
				writer.WriteStringField("name", "io.debezium.time.Timestamp")
			} else {
				writer.WriteStringField("name", "io.debezium.time.MicroTimestamp")
			}
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				if v == "CURRENT_TIMESTAMP" {
					writer.WriteInt64Field("default", 0)
					return
				}
				t, err := types.StrToDateTime(types.DefaultStmtNoWarningContext, v, ft.GetDecimal())
				if err != nil {
					writer.WriteInt64Field("default", 0)
					return
				}
				gt, err := t.GoTime(time.UTC)
				if err != nil {
					if mysql.HasNotNullFlag(ft.GetFlag()) {
						writer.WriteInt64Field("default", 0)
					}
					return
				}
				year := gt.Year()
				if year < 70 {
					// treats "0018" as 2018
					gt = gt.AddDate(2000, 0, 0)
				} else if year < 100 {
					//  treats "0099" as 1999
					gt = gt.AddDate(1900, 0, 0)
				}
				if ft.GetDecimal() <= 3 {
					writer.WriteInt64Field("default", gt.UnixMilli())
				} else {
					writer.WriteInt64Field("default", gt.UnixMicro())
				}
			}
		})

	case mysql.TypeTimestamp:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.ZonedTimestamp")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				if v == "CURRENT_TIMESTAMP" {
					if mysql.HasNotNullFlag(ft.GetFlag()) {
						writer.WriteStringField("default", "1970-01-01T00:00:00Z")
					}
					return
				}
				t, err := types.StrToDateTime(types.DefaultStmtNoWarningContext, v, ft.GetDecimal())
				if err != nil {
					writer.WriteInt64Field("default", 0)
					return
				}
				if t.Compare(types.MinTimestamp) < 0 {
					if mysql.HasNotNullFlag(ft.GetFlag()) {
						writer.WriteStringField("default", "1970-01-01T00:00:00Z")
					}
					return
				}
				gt, err := t.GoTime(time.UTC)
				if err != nil {
					writer.WriteInt64Field("default", 0)
					return
				}
				str := gt.Format("2006-01-02T15:04:05")
				fsp := ft.GetDecimal()
				if fsp > 0 {
					tmp := fmt.Sprintf(".%06d", gt.Nanosecond()/1000)
					str = str + tmp[:1+fsp]
				}
				str += "Z"
				writer.WriteStringField("default", str)
			}
		})

	case mysql.TypeDuration:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int64")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.MicroTime")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				d, _, _, err := types.StrToDuration(types.DefaultStmtNoWarningContext.WithLocation(c.config.TimeZone), v, ft.GetDecimal())
				if err != nil {
					return
				}
				writer.WriteInt64Field("default", d.Microseconds())
			}
		})

	case mysql.TypeJSON:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.data.Json")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				writer.WriteAnyField("default", col.GetDefaultValue())
			}
		})

	case mysql.TypeTiny: // TINYINT
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int16")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		})

	case mysql.TypeShort: // SMALLINT
		writer.WriteObjectElement(func() {
			if mysql.HasUnsignedFlag(ft.GetFlag()) {
				writer.WriteStringField("type", "int32")
			} else {
				writer.WriteStringField("type", "int16")
			}
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		})

	case mysql.TypeInt24: // MEDIUMINT
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int32")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		})

	case mysql.TypeLong: // INT
		writer.WriteObjectElement(func() {
			if col.GetFlag().IsUnsigned() {
				writer.WriteStringField("type", "int64")
			} else {
				writer.WriteStringField("type", "int32")
			}
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		})

	case mysql.TypeLonglong: // BIGINT
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int64")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		})

	case mysql.TypeFloat:
		writer.WriteObjectElement(func() {
			if ft.GetDecimal() != -1 {
				writer.WriteStringField("type", "double")
			} else {
				writer.WriteStringField("type", "float")
			}
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		})

	case mysql.TypeDouble, mysql.TypeNewDecimal:
		// https://dev.mysql.com/doc/refman/8.4/en/numeric-types.html
		// MySQL also treats REAL as a synonym for DOUBLE PRECISION (a nonstandard variation), unless the REAL_AS_FLOAT SQL mode is enabled.
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "double")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		})

	case mysql.TypeYear:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int32")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.Year")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				if floatV < 70 {
					// treats "DEFAULT 1" as 2001
					floatV += 2000
				} else if floatV < 100 {
					//  treats "DEFAULT 99" as 1999
					floatV += 1900
				}
				writer.WriteFloat64Field("default", floatV)
			}
		})

	case mysql.TypeTiDBVectorFloat32:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.data.TiDBVectorFloat32")
			writer.WriteStringField("field", col.GetName())
			if col.GetDefaultValue() != nil {
				writer.WriteAnyField("default", col.GetDefaultValue())
			}
		})

	default:
		log.Warn(
			"meet unsupported field type",
			zap.Any("fieldType", col.GetType()),
			zap.Any("column", col.GetName()),
		)
	}
}

// See https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-data-types
//
//revive:disable indent-error-flow
func (c *dbzCodec) writeDebeziumFieldValue(
	writer *util.JSONWriter,
	col model.ColumnDataX,
	ft *types.FieldType,
) error {
	value := col.Value
	if value == nil {
		value = col.GetDefaultValue()
	}
	if value == nil {
		writer.WriteNullField(col.GetName())
		return nil
	}
	switch col.GetType() {
	case mysql.TypeBit:
		n := ft.GetFlen()
		var v uint64
		switch val := value.(type) {
		case uint64:
			v = val
		case string:
			hexValue, err := strconv.ParseUint(parseBit(val, n), 2, 64)
			if err != nil {
				return cerror.ErrDebeziumEncodeFailed.GenWithStack(
					"unexpected column value type string for bit column %s, error:%s",
					col.GetName(), err.Error())
			}
			v = hexValue
		default:
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for bit column %s",
				col.Value,
				col.GetName())
		}
		// Debezium behavior:
		// BIT(1) → BOOLEAN
		// BIT(>1) → BYTES		The byte[] contains the bits in little-endian form and is sized to
		//						contain the specified number of bits.
		if n == 1 {
			writer.WriteBoolField(col.GetName(), v != 0)
			return nil
		} else {
			c.writeBinaryField(writer, col.GetName(), getBitFromUint64(n, v))
			return nil
		}

	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		isBinary := col.GetFlag().IsBinary()
		switch v := value.(type) {
		case []byte:
			if !isBinary {
				writer.WriteStringField(col.GetName(), string(hack.String(v)))
			} else {
				c.writeBinaryField(writer, col.GetName(), v)
			}
		case string:
			if isBinary {
				c.writeBinaryField(writer, col.GetName(), hack.Slice(v))
			}
			writer.WriteStringField(col.GetName(), v)
		}
		return nil

	case mysql.TypeEnum:
		v, ok := value.(uint64)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for enum column %s",
				value,
				col.GetName())
		}
		enumVar, err := types.ParseEnumValue(ft.GetElems(), v)
		if err != nil {
			// Invalid enum value inserted in non-strict mode.
			writer.WriteStringField(col.GetName(), "")
			return nil
		}
		writer.WriteStringField(col.GetName(), enumVar.Name)
		return nil

	case mysql.TypeSet:
		v, ok := value.(uint64)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for set column %s",
				value,
				col.GetName())
		}
		setVar, err := types.ParseSetValue(ft.GetElems(), v)
		if err != nil {
			// Invalid enum value inserted in non-strict mode.
			writer.WriteStringField(col.GetName(), "")
			return nil
		}
		writer.WriteStringField(col.GetName(), setVar.Name)
		return nil

	case mysql.TypeNewDecimal:
		v, ok := value.(string)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for decimal column %s",
				value,
				col.GetName())
		}
		floatV, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return cerror.WrapError(
				cerror.ErrDebeziumEncodeFailed,
				err)
		}
		writer.WriteFloat64Field(col.GetName(), floatV)
		return nil

	case mysql.TypeDate, mysql.TypeNewDate:
		v, ok := value.(string)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for date column %s",
				value,
				col.GetName())
		}
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			// For example, time may be invalid like 1000-00-00
			// return nil, nil
			if mysql.HasNotNullFlag(ft.GetFlag()) {
				writer.WriteInt64Field(col.GetName(), 0)
			} else {
				writer.WriteNullField(col.GetName())
			}
			return nil
		}
		year := t.Year()
		if year < 70 {
			// treats "0018" as 2018
			t = t.AddDate(2000, 0, 0)
		} else if year < 100 {
			//  treats "0099" as 1999
			t = t.AddDate(1900, 0, 0)
		}

		writer.WriteInt64Field(col.GetName(), t.UTC().Unix()/60/60/24)
		return nil

	case mysql.TypeDatetime:
		// Debezium behavior from doc:
		// > Such columns are converted into epoch milliseconds or microseconds based on the
		// > column's precision by using UTC.
		v, ok := value.(string)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for datetime column %s",
				value,
				col.GetName())
		}
		if v == "CURRENT_TIMESTAMP" {
			writer.WriteInt64Field(col.GetName(), 0)
			return nil
		}
		t, err := types.StrToDateTime(types.DefaultStmtNoWarningContext, v, ft.GetDecimal())
		if err != nil {
			return cerror.WrapError(
				cerror.ErrDebeziumEncodeFailed,
				err)
		}
		gt, err := t.GoTime(time.UTC)
		if err != nil {
			if mysql.HasNotNullFlag(ft.GetFlag()) {
				writer.WriteInt64Field(col.GetName(), 0)
			} else {
				writer.WriteNullField(col.GetName())
			}
			return nil
		}
		year := gt.Year()
		if year < 70 {
			// treats "0018" as 2018
			gt = gt.AddDate(2000, 0, 0)
		} else if year < 100 {
			//  treats "0099" as 1999
			gt = gt.AddDate(1900, 0, 0)
		}
		if ft.GetDecimal() <= 3 {
			writer.WriteInt64Field(col.GetName(), gt.UnixMilli())
		} else {
			writer.WriteInt64Field(col.GetName(), gt.UnixMicro())
		}
		return nil

	case mysql.TypeTimestamp:
		// Debezium behavior from doc:
		// > The TIMESTAMP type represents a timestamp without time zone information.
		// > It is converted by MySQL from the server (or session's) current time zone into UTC
		// > when writing and from UTC into the server (or session's) current time zone when reading
		// > back the value.
		// > Such columns are converted into an equivalent io.debezium.time.ZonedTimestamp in UTC
		// > based on the server (or session's) current time zone. The time zone will be queried from
		// > the server by default. If this fails, it must be specified explicitly by the database
		// > connectionTimeZone MySQL configuration option.
		v, ok := value.(string)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for timestamp column %s",
				value,
				col.GetName())
		}
		if v == "CURRENT_TIMESTAMP" {
			if mysql.HasNotNullFlag(ft.GetFlag()) {
				writer.WriteStringField(col.GetName(), "1970-01-01T00:00:00Z")
			} else {
				writer.WriteNullField(col.GetName())
			}
			return nil
		}
		t, err := types.StrToDateTime(types.DefaultStmtNoWarningContext.WithLocation(c.config.TimeZone), v, ft.GetDecimal())
		if err != nil {
			return cerror.WrapError(
				cerror.ErrDebeziumEncodeFailed,
				err)
		}
		if t.Compare(types.MinTimestamp) < 0 {
			if col.Value == nil {
				writer.WriteNullField(col.GetName())
			} else {
				writer.WriteStringField(col.GetName(), "1970-01-01T00:00:00Z")
			}
			return nil
		}
		gt, err := t.GoTime(c.config.TimeZone)
		if err != nil {
			return cerror.WrapError(
				cerror.ErrDebeziumEncodeFailed,
				err)
		}
		str := gt.UTC().Format("2006-01-02T15:04:05")
		fsp := ft.GetDecimal()
		if fsp > 0 {
			tmp := fmt.Sprintf(".%06d", gt.Nanosecond()/1000)
			str = str + tmp[:1+fsp]
		}
		str += "Z"
		writer.WriteStringField(col.GetName(), str)
		return nil

	case mysql.TypeDuration:
		// Debezium behavior from doc:
		// > Represents the time value in microseconds and does not include
		// > time zone information. MySQL allows M to be in the range of 0-6.
		v, ok := value.(string)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for time column %s",
				value,
				col.GetName())
		}
		d, _, _, err := types.StrToDuration(types.DefaultStmtNoWarningContext.WithLocation(c.config.TimeZone), v, ft.GetDecimal())
		if err != nil {
			return cerror.WrapError(
				cerror.ErrDebeziumEncodeFailed,
				err)
		}
		writer.WriteInt64Field(col.GetName(), d.Microseconds())
		return nil

	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		// Note: Although Debezium's doc claims to use INT32 for INT, but it
		// actually uses INT64. Debezium also uses INT32 for SMALLINT.
		isUnsigned := col.GetFlag().IsUnsigned()
		maxValue := types.GetMaxValue(ft)
		minValue := types.GetMinValue(ft)
		switch v := value.(type) {
		case uint64:
			if !isUnsigned {
				return cerror.ErrDebeziumEncodeFailed.GenWithStack(
					"unexpected column value type %T for unsigned int column %s",
					value,
					col.GetName())
			}
			if ft.GetType() == mysql.TypeLonglong && v == maxValue.GetUint64() || v > maxValue.GetUint64() {
				writer.WriteAnyField(col.GetName(), -1)
			} else {
				writer.WriteInt64Field(col.GetName(), int64(v))
			}
		case int64:
			if isUnsigned {
				return cerror.ErrDebeziumEncodeFailed.GenWithStack(
					"unexpected column value type %T for int column %s",
					value,
					col.GetName())
			}
			if v < minValue.GetInt64() || v > maxValue.GetInt64() {
				writer.WriteAnyField(col.GetName(), -1)
			} else {
				writer.WriteInt64Field(col.GetName(), v)
			}
		case string:
			if isUnsigned {
				t, err := strconv.ParseUint(v, 10, 64)
				if err != nil {
					return cerror.ErrDebeziumEncodeFailed.GenWithStack(
						"unexpected column value type string for unsigned int column %s",
						col.GetName())
				}
				if ft.GetType() == mysql.TypeLonglong && t == maxValue.GetUint64() || t > maxValue.GetUint64() {
					writer.WriteAnyField(col.GetName(), -1)
				} else {
					writer.WriteInt64Field(col.GetName(), int64(t))
				}
			} else {
				t, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return cerror.ErrDebeziumEncodeFailed.GenWithStack(
						"unexpected column value type string for int column %s",
						col.GetName())
				}
				if t < minValue.GetInt64() || t > maxValue.GetInt64() {
					writer.WriteAnyField(col.GetName(), -1)
				} else {
					writer.WriteInt64Field(col.GetName(), t)
				}
			}
		}
		return nil

	case mysql.TypeDouble, mysql.TypeFloat:
		if v, ok := value.(string); ok {
			val, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return cerror.ErrDebeziumEncodeFailed.GenWithStack(
					"unexpected column value type string for int column %s",
					col.GetName())
			}
			writer.WriteFloat64Field(col.GetName(), val)
		} else {
			writer.WriteAnyField(col.GetName(), value)
		}
		return nil

	case mysql.TypeTiDBVectorFloat32:
		v, ok := value.(types.VectorFloat32)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for unsigned vector column %s",
				value,
				col.GetName())
		}
		writer.WriteStringField(col.GetName(), v.String())
		return nil
	}

	writer.WriteAnyField(col.GetName(), value)
	return nil
}

func (c *dbzCodec) writeBinaryField(writer *util.JSONWriter, fieldName string, value []byte) {
	// TODO: Deal with different binary output later.
	writer.WriteBase64StringField(fieldName, value)
}

func (c *dbzCodec) writeSourceSchema(writer *util.JSONWriter) {
	writer.WriteObjectElement(func() {
		writer.WriteStringField("type", "struct")
		writer.WriteArrayField("fields", func() {
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "version")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "connector")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "name")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "int64")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "ts_ms")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("name", "io.debezium.data.Enum")
				writer.WriteIntField("version", 1)
				writer.WriteObjectField("parameters", func() {
					writer.WriteStringField("allowed", "true,last,false,incremental")
				})
				writer.WriteStringField("default", "false")
				writer.WriteStringField("field", "snapshot")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "db")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("field", "sequence")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("field", "table")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "int64")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "server_id")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("field", "gtid")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "file")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "int64")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "pos")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "int32")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "row")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "int64")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("field", "thread")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("field", "query")
			})
		})
		writer.WriteBoolField("optional", false)
		writer.WriteStringField("name", "io.debezium.connector.mysql.Source")
		writer.WriteStringField("field", "source")
	})
}

// EncodeKey encode RowChangedEvent into key message
func (c *dbzCodec) EncodeKey(
	e *model.RowChangedEvent,
	dest io.Writer,
) error {
	// schema field describes the structure of the primary key, or the unique key if the table does not have a primary key, for the table that was changed.
	// see https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-events
	colDataXs, colInfos := e.HandleKeyColDataXInfos()
	jWriter := util.BorrowJSONWriter(dest)
	defer util.ReturnJSONWriter(jWriter)

	var err error
	jWriter.WriteObject(func() {
		jWriter.WriteObjectField("payload", func() {
			for i, col := range colDataXs {
				err = c.writeDebeziumFieldValue(jWriter, col, colInfos[i].Ft)
			}
		})
		if !c.config.DebeziumDisableSchema {
			jWriter.WriteObjectField("schema", func() {
				jWriter.WriteStringField("type", "struct")
				jWriter.WriteStringField("name",
					fmt.Sprintf("%s.Key", getSchemaTopicName(c.clusterID, e.TableInfo.GetSchemaName(), e.TableInfo.GetTableName())))
				jWriter.WriteBoolField("optional", false)
				jWriter.WriteArrayField("fields", func() {
					for i, col := range colDataXs {
						c.writeDebeziumFieldSchema(jWriter, col, colInfos[i].Ft)
					}
				})
			})
		}
	})
	return err
}

// EncodeValue encode RowChangedEvent into value message
func (c *dbzCodec) EncodeValue(
	e *model.RowChangedEvent,
	dest io.Writer,
) error {
	jWriter := util.BorrowJSONWriter(dest)
	defer util.ReturnJSONWriter(jWriter)

	commitTime := oracle.GetTimeFromTS(e.CommitTs)

	var err error

	jWriter.WriteObject(func() {
		jWriter.WriteObjectField("payload", func() {
			jWriter.WriteObjectField("source", func() {
				jWriter.WriteStringField("version", "2.4.0.Final")
				jWriter.WriteStringField("connector", "TiCDC")
				jWriter.WriteStringField("name", c.clusterID)
				// ts_ms: In the source object, ts_ms indicates the time that the change was made in the database.
				// https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
				jWriter.WriteInt64Field("ts_ms", commitTime.UnixMilli())
				// snapshot field is a string of true,last,false,incremental
				jWriter.WriteStringField("snapshot", "false")
				jWriter.WriteStringField("db", e.TableInfo.GetSchemaName())
				jWriter.WriteStringField("table", e.TableInfo.GetTableName())
				jWriter.WriteInt64Field("server_id", 0)
				jWriter.WriteNullField("gtid")
				jWriter.WriteStringField("file", "")
				jWriter.WriteInt64Field("pos", 0)
				jWriter.WriteInt64Field("row", 0)
				jWriter.WriteInt64Field("thread", 0)
				jWriter.WriteNullField("query")

				// The followings are TiDB extended fields
				jWriter.WriteUint64Field("commit_ts", e.CommitTs)
				jWriter.WriteStringField("cluster_id", c.clusterID)
			})

			// ts_ms: displays the time at which the connector processed the event
			// https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
			jWriter.WriteInt64Field("ts_ms", c.nowFunc().UnixMilli())
			jWriter.WriteNullField("transaction")
			if e.IsInsert() {
				// op: Mandatory string that describes the type of operation that caused the connector to generate the event.
				// Valid values are:
				// c = create
				// u = update
				// d = delete
				// r = read (applies to only snapshots)
				// https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
				jWriter.WriteStringField("op", "c")

				// before: An optional field that specifies the state of the row before the event occurred.
				// When the op field is c for create, the before field is null since this change event is for new content.
				// In a delete event value, the before field contains the values that were in the row before
				// it was deleted with the database commit.
				jWriter.WriteNullField("before")

				// after: An optional field that specifies the state of the row after the event occurred.
				// Optional field that specifies the state of the row after the event occurred.
				// In a delete event value, the after field is null, signifying that the row no longer exists.
				err = c.writeDebeziumFieldValues(jWriter, "after", e.Columns, e.TableInfo)
			} else if e.IsDelete() {
				jWriter.WriteStringField("op", "d")
				jWriter.WriteNullField("after")
				err = c.writeDebeziumFieldValues(jWriter, "before", e.PreColumns, e.TableInfo)
			} else if e.IsUpdate() {
				jWriter.WriteStringField("op", "u")
				if c.config.DebeziumOutputOldValue {
					err = c.writeDebeziumFieldValues(jWriter, "before", e.PreColumns, e.TableInfo)
				}
				if err == nil {
					err = c.writeDebeziumFieldValues(jWriter, "after", e.Columns, e.TableInfo)
				}
			}
		})

		if !c.config.DebeziumDisableSchema {
			jWriter.WriteObjectField("schema", func() {
				jWriter.WriteStringField("type", "struct")
				jWriter.WriteBoolField("optional", false)
				jWriter.WriteStringField("name",
					fmt.Sprintf("%s.Envelope", getSchemaTopicName(c.clusterID, e.TableInfo.GetSchemaName(), e.TableInfo.GetTableName())))
				jWriter.WriteIntField("version", 1)
				jWriter.WriteArrayField("fields", func() {
					// schema is the same for `before` and `after`. So we build a new buffer to
					// build the JSON, so that content can be reused.
					var fieldsJSON string
					{
						fieldsBuf := &bytes.Buffer{}
						fieldsWriter := util.BorrowJSONWriter(fieldsBuf)
						var validCols []*model.ColumnData
						if e.IsInsert() {
							validCols = e.Columns
						} else if e.IsDelete() {
							validCols = e.PreColumns
						} else if e.IsUpdate() {
							validCols = e.Columns
						}
						colInfos := e.TableInfo.GetColInfosForRowChangedEvent()
						for i, col := range validCols {
							if col == nil {
								continue
							}
							colx := model.GetColumnDataX(col, e.TableInfo)
							c.writeDebeziumFieldSchema(fieldsWriter, colx, colInfos[i].Ft)
						}
						if e.TableInfo.HasVirtualColumns() {
							for _, colInfo := range e.TableInfo.Columns {
								if model.IsColCDCVisible(colInfo) {
									continue
								}
								data := &model.ColumnData{ColumnID: colInfo.ID}
								colx := model.GetColumnDataX(data, e.TableInfo)
								c.writeDebeziumFieldSchema(fieldsWriter, colx, &colInfo.FieldType)
							}
						}
						util.ReturnJSONWriter(fieldsWriter)
						fieldsJSON = fieldsBuf.String()
					}
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "struct")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("name",
							fmt.Sprintf("%s.Value", getSchemaTopicName(c.clusterID, e.TableInfo.GetSchemaName(), e.TableInfo.GetTableName())))
						jWriter.WriteStringField("field", "before")
						jWriter.WriteArrayField("fields", func() {
							jWriter.WriteRaw(fieldsJSON)
						})
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "struct")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("name",
							fmt.Sprintf("%s.Value", getSchemaTopicName(c.clusterID, e.TableInfo.GetSchemaName(), e.TableInfo.GetTableName())))
						jWriter.WriteStringField("field", "after")
						jWriter.WriteArrayField("fields", func() {
							jWriter.WriteRaw(fieldsJSON)
						})
					})
					c.writeSourceSchema(jWriter)
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "string")
						jWriter.WriteBoolField("optional", false)
						jWriter.WriteStringField("field", "op")
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "int64")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("field", "ts_ms")
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "struct")
						jWriter.WriteArrayField("fields", func() {
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "id")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "int64")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "total_order")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "int64")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "data_collection_order")
							})
						})
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("name", "event.block")
						jWriter.WriteIntField("version", 1)
						jWriter.WriteStringField("field", "transaction")
					})
				})
			})
		}
	})
	return err
}
