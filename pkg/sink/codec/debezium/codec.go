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
	"encoding/binary"
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
	cols []*model.Column,
	tableInfo *model.TableInfo,
) error {
	var err error
	colInfos := tableInfo.GetColInfosForRowChangedEvent()
	writer.WriteObjectField(fieldName, func() {
		for i, col := range cols {
			err = c.writeDebeziumFieldValue(writer, col, colInfos[i].Ft)
			if err != nil {
				break
			}
		}
	})
	return err
}

func (c *dbzCodec) writeDebeziumFieldSchema(
	writer *util.JSONWriter,
	col *model.Column,
	ft *types.FieldType,
) {
	switch col.Type {
	case mysql.TypeBit:
		n := ft.GetFlen()
		if n == 1 {
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "boolean")
				writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
				writer.WriteStringField("field", col.Name)
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
				writer.WriteStringField("field", col.Name)
			})
		}

	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeEnum:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.data.Enum")
			writer.WriteIntField("version", 1)
			writer.WriteObjectField("parameters", func() {
				writer.WriteStringField("allowed", strings.Join(ft.GetElems(), ","))
			})
			writer.WriteStringField("field", col.Name)
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
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeNewDecimal:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "double")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeDate, mysql.TypeNewDate:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int32")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.Date")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", col.Name)
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
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeTimestamp:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.ZonedTimestamp")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeDuration:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int64")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.MicroTime")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeJSON:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.data.Json")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeTiny: // TINYINT
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int16")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeShort: // SMALLINT
		writer.WriteObjectElement(func() {
			if mysql.HasUnsignedFlag(ft.GetFlag()) {
				writer.WriteStringField("type", "int32")
			} else {
				writer.WriteStringField("type", "int16")
			}
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeInt24: // MEDIUMINT
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int32")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeLong: // INT
		writer.WriteObjectElement(func() {
			if mysql.HasUnsignedFlag(ft.GetFlag()) {
				writer.WriteStringField("type", "int64")
			} else {
				writer.WriteStringField("type", "int32")
			}
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeLonglong: // BIGINT
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int64")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeFloat:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "float")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeDouble:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "double")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", col.Name)
		})

	case mysql.TypeYear:
		writer.WriteObjectElement(func() {
			writer.WriteStringField("type", "int32")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.Year")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", col.Name)
		})

	default:
		log.Warn(
			"meet unsupported field type",
			zap.Any("fieldType", col.Type),
			zap.Any("column", col.Name),
		)
	}
}

// See https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-data-types
//
//revive:disable indent-error-flow
func (c *dbzCodec) writeDebeziumFieldValue(
	writer *util.JSONWriter,
	col *model.Column,
	ft *types.FieldType,
) error {
	if col.Value == nil {
		writer.WriteNullField(col.Name)
		return nil
	}
	switch col.Type {
	case mysql.TypeBit:
		v, ok := col.Value.(uint64)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for bit column %s",
				col.Value,
				col.Name)
		}

		// Debezium behavior:
		// BIT(1) → BOOLEAN
		// BIT(>1) → BYTES		The byte[] contains the bits in little-endian form and is sized to
		//						contain the specified number of bits.
		n := ft.GetFlen()
		if n == 1 {
			writer.WriteBoolField(col.Name, v != 0)
			return nil
		} else {
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], v)
			numBytes := n / 8
			if n%8 != 0 {
				numBytes += 1
			}
			c.writeBinaryField(writer, col.Name, buf[:numBytes])
			return nil
		}

	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		v, ok := col.Value.([]byte)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for string column %s",
				col.Value,
				col.Name)
		}

		if col.Flag.IsBinary() {
			c.writeBinaryField(writer, col.Name, v)
			return nil
		} else {
			writer.WriteStringField(col.Name, string(hack.String(v)))
			return nil
		}

	case mysql.TypeEnum:
		v, ok := col.Value.(uint64)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for enum column %s",
				col.Value,
				col.Name)
		}

		enumVar, err := types.ParseEnumValue(ft.GetElems(), v)
		if err != nil {
			// Invalid enum value inserted in non-strict mode.
			writer.WriteStringField(col.Name, "")
			return nil
		}

		writer.WriteStringField(col.Name, enumVar.Name)
		return nil

	case mysql.TypeSet:
		v, ok := col.Value.(uint64)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for set column %s",
				col.Value,
				col.Name)
		}

		setVar, err := types.ParseSetValue(ft.GetElems(), v)
		if err != nil {
			// Invalid enum value inserted in non-strict mode.
			writer.WriteStringField(col.Name, "")
			return nil
		}

		writer.WriteStringField(col.Name, setVar.Name)
		return nil

	case mysql.TypeNewDecimal:
		v, ok := col.Value.(string)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for decimal column %s",
				col.Value,
				col.Name)
		}

		floatV, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return cerror.WrapError(
				cerror.ErrDebeziumEncodeFailed,
				err)
		}

		writer.WriteFloat64Field(col.Name, floatV)
		return nil

	case mysql.TypeDate, mysql.TypeNewDate:
		v, ok := col.Value.(string)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for date column %s",
				col.Value,
				col.Name)
		}

		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			// For example, time may be invalid like 1000-00-00
			// return nil, nil
			if mysql.HasNotNullFlag(ft.GetFlag()) {
				writer.WriteInt64Field(col.Name, 0)
				return nil
			} else {
				writer.WriteNullField(col.Name)
				return nil
			}
		}

		writer.WriteInt64Field(col.Name, t.Unix()/60/60/24)
		return nil

	case mysql.TypeDatetime:
		// Debezium behavior from doc:
		// > Such columns are converted into epoch milliseconds or microseconds based on the
		// > column's precision by using UTC.

		// TODO: For Default Value = CURRENT_TIMESTAMP, the result is incorrect.
		v, ok := col.Value.(string)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for datetime column %s",
				col.Value,
				col.Name)
		}

		t, err := time.Parse("2006-01-02 15:04:05.999999", v)
		if err != nil {
			// For example, time may be 1000-00-00
			if mysql.HasNotNullFlag(ft.GetFlag()) {
				writer.WriteInt64Field(col.Name, 0)
				return nil
			} else {
				writer.WriteNullField(col.Name)
				return nil
			}
		}

		if ft.GetDecimal() <= 3 {
			writer.WriteInt64Field(col.Name, t.UnixMilli())
			return nil
		} else {
			writer.WriteInt64Field(col.Name, t.UnixMicro())
			return nil
		}

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
		v, ok := col.Value.(string)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for timestamp column %s",
				col.Value,
				col.Name)
		}

		t, err := time.ParseInLocation("2006-01-02 15:04:05.999999", v, c.config.TimeZone)
		if err != nil {
			// For example, time may be invalid like 1000-00-00
			if mysql.HasNotNullFlag(ft.GetFlag()) {
				t = time.Unix(0, 0)
			} else {
				writer.WriteNullField(col.Name)
				return nil
			}
		}

		str := t.UTC().Format("2006-01-02T15:04:05")
		fsp := ft.GetDecimal()
		if fsp > 0 {
			tmp := fmt.Sprintf(".%06d", t.Nanosecond()/1000)
			str = str + tmp[:1+fsp]
		}
		str += "Z"

		writer.WriteStringField(col.Name, str)
		return nil

	case mysql.TypeDuration:
		// Debezium behavior from doc:
		// > Represents the time value in microseconds and does not include
		// > time zone information. MySQL allows M to be in the range of 0-6.
		v, ok := col.Value.(string)
		if !ok {
			return cerror.ErrDebeziumEncodeFailed.GenWithStack(
				"unexpected column value type %T for time column %s",
				col.Value,
				col.Name)
		}

		d, _, _, err := types.StrToDuration(types.DefaultStmtNoWarningContext, v, ft.GetDecimal())
		if err != nil {
			return cerror.WrapError(
				cerror.ErrDebeziumEncodeFailed,
				err)
		}

		writer.WriteInt64Field(col.Name, d.Microseconds())
		return nil

	case mysql.TypeLonglong:
		if col.Flag.IsUnsigned() {
			// Handle with BIGINT UNSIGNED.
			// Debezium always produce INT64 instead of UINT64 for BIGINT.
			v, ok := col.Value.(uint64)
			if !ok {
				return cerror.ErrDebeziumEncodeFailed.GenWithStack(
					"unexpected column value type %T for unsigned bigint column %s",
					col.Value,
					col.Name)
			}

			writer.WriteInt64Field(col.Name, int64(v))
			return nil
		}

		// Note: Although Debezium's doc claims to use INT32 for INT, but it
		// actually uses INT64. Debezium also uses INT32 for SMALLINT.
		// So we only handle with TypeLonglong here.
	}

	writer.WriteAnyField(col.Name, col.Value)
	return nil
}

func (c *dbzCodec) writeBinaryField(writer *util.JSONWriter, fieldName string, value []byte) {
	// TODO: Deal with different binary output later.
	writer.WriteBase64StringField(fieldName, value)
}

func (c *dbzCodec) EncodeRowChangedEvent(
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
				err = c.writeDebeziumFieldValues(jWriter, "after", e.GetColumns(), e.TableInfo)
			} else if e.IsDelete() {
				jWriter.WriteStringField("op", "d")
				jWriter.WriteNullField("after")
				err = c.writeDebeziumFieldValues(jWriter, "before", e.GetPreColumns(), e.TableInfo)
			} else if e.IsUpdate() {
				jWriter.WriteStringField("op", "u")
				err = c.writeDebeziumFieldValues(jWriter, "before", e.GetPreColumns(), e.TableInfo)
				if err == nil {
					err = c.writeDebeziumFieldValues(jWriter, "after", e.GetColumns(), e.TableInfo)
				}
			}
		})

		if !c.config.DebeziumDisableSchema {
			jWriter.WriteObjectField("schema", func() {
				jWriter.WriteStringField("type", "struct")
				jWriter.WriteBoolField("optional", false)
				jWriter.WriteStringField("name", fmt.Sprintf("%s.%s.%s.Envelope",
					c.clusterID,
					e.TableInfo.GetSchemaName(),
					e.TableInfo.GetTableName()))
				jWriter.WriteIntField("version", 1)
				jWriter.WriteArrayField("fields", func() {
					// schema is the same for `before` and `after`. So we build a new buffer to
					// build the JSON, so that content can be reused.
					var fieldsJSON string
					{
						fieldsBuf := &bytes.Buffer{}
						fieldsWriter := util.BorrowJSONWriter(fieldsBuf)
						var validCols []*model.Column
						if e.IsInsert() {
							validCols = e.GetColumns()
						} else if e.IsDelete() {
							validCols = e.GetPreColumns()
						} else if e.IsUpdate() {
							validCols = e.GetColumns()
						}
						colInfos := e.TableInfo.GetColInfosForRowChangedEvent()
						for i, col := range validCols {
							c.writeDebeziumFieldSchema(fieldsWriter, col, colInfos[i].Ft)
						}
						util.ReturnJSONWriter(fieldsWriter)
						fieldsJSON = fieldsBuf.String()
					}
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "struct")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("name", fmt.Sprintf("%s.%s.%s.Value",
							c.clusterID,
							e.TableInfo.GetSchemaName(),
							e.TableInfo.GetTableName()))
						jWriter.WriteStringField("field", "before")
						jWriter.WriteArrayField("fields", func() {
							jWriter.WriteRaw(fieldsJSON)
						})
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "struct")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("name", fmt.Sprintf("%s.%s.%s.Value",
							c.clusterID,
							e.TableInfo.GetSchemaName(),
							e.TableInfo.GetTableName()))
						jWriter.WriteStringField("field", "after")
						jWriter.WriteArrayField("fields", func() {
							jWriter.WriteRaw(fieldsJSON)
						})
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "struct")
						jWriter.WriteArrayField("fields", func() {
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "version")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "connector")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "name")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "int64")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "ts_ms")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", true)
								jWriter.WriteStringField("name", "io.debezium.data.Enum")
								jWriter.WriteIntField("version", 1)
								jWriter.WriteObjectField("parameters", func() {
									jWriter.WriteStringField("allowed", "true,last,false,incremental")
								})
								jWriter.WriteStringField("default", "false")
								jWriter.WriteStringField("field", "snapshot")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "db")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", true)
								jWriter.WriteStringField("field", "sequence")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", true)
								jWriter.WriteStringField("field", "table")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "int64")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "server_id")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", true)
								jWriter.WriteStringField("field", "gtid")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "file")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "int64")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "pos")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "int32")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "row")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "int64")
								jWriter.WriteBoolField("optional", true)
								jWriter.WriteStringField("field", "thread")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", true)
								jWriter.WriteStringField("field", "query")
							})
							// Below are extra TiDB fields
							// jWriter.WriteObjectElement(func() {
							// 	jWriter.WriteStringField("type", "int64")
							// 	jWriter.WriteBoolField("optional", false)
							// 	jWriter.WriteStringField("field", "commit_ts")
							// })
							// jWriter.WriteObjectElement(func() {
							// 	jWriter.WriteStringField("type", "string")
							// 	jWriter.WriteBoolField("optional", false)
							// 	jWriter.WriteStringField("field", "cluster_id")
							// })
						})
						jWriter.WriteBoolField("optional", false)
						jWriter.WriteStringField("name", "io.debezium.connector.mysql.Source")
						jWriter.WriteStringField("field", "source")
					})
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
