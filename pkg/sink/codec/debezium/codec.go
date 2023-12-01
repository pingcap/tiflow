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
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"time"

	_ "github.com/goccy/go-json"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/pd/pkg/utils/tsoutil"
)

type debeziumDataChangeMsg struct {
	// schema is unsupported
	Payload *debeziumDataChangeMsgPayload `json:"payload"`
}

type debeziumDataChangeMsgPayload struct {
	// Before: An optional field that specifies the state of the row before the event occurred.
	// When the op field is c for create, the before field is null since this change event is for new content.
	// In a delete event value, the before field contains the values that were in the row before
	// it was deleted with the database commit.
	Before map[string]any `json:"before"`
	// After: An optional field that specifies the state of the row after the event occurred.
	// Optional field that specifies the state of the row after the event occurred.
	// In a delete event value, the after field is null, signifying that the row no longer exists.
	After  map[string]any     `json:"after"`
	Source *debeziumMsgSource `json:"source"`
	// Op: Mandatory string that describes the type of operation that caused the connector to generate the event.
	// Valid values are:
	// c = create
	// u = update
	// d = delete
	// r = read (applies to only snapshots)
	// https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
	Op string `json:"op"`
	// TsMs: displays the time at which the connector processed the event
	// https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
	TsMs int64 `json:"ts_ms"`
	// Transaction: Always null
	Transaction *struct{} `json:"transaction"`
}

type debeziumMsgSource struct {
	Version   string `json:"version"`
	Connector string `json:"connector"`
	Name      string `json:"name"`
	// // TsMs: In the source object, ts_ms indicates the time that the change was made in the database.
	// // https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
	TsMs     int64   `json:"ts_ms"`
	Snapshot bool    `json:"snapshot"`
	Db       string  `json:"db"`
	Table    string  `json:"table"`
	ServerID int64   `json:"server_id"`
	GtID     *string `json:"gtid"`
	File     string  `json:"file"`
	Pos      int64   `json:"pos"`
	Row      int32   `json:"row"`
	Thread   int64   `json:"thread"`
	Query    *string `json:"query"`

	// The followings are TiDB extended fields
	CommitTs  uint64 `json:"commit_ts"`
	ClusterID string `json:"cluster_id"`
}

type Codec struct {
	config    *common.Config
	clusterID string
	nowFunc   func() time.Time
}

func (c *Codec) writeColumnsAsField(writer *util.JSONWriter, fieldName string, cols []*model.Column, colInfos []rowcodec.ColInfo) error {
	var err error
	writer.WriteObjectField(fieldName, func() {
		for i, col := range cols {
			err = c.writeDebeziumField(writer, col, colInfos[i].Ft)
			if err != nil {
				break
			}
		}
	})
	return err
}

// See https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-data-types
func (c *Codec) writeDebeziumField(writer *util.JSONWriter, col *model.Column, ft *types.FieldType) error {
	if col.Value == nil {
		writer.WriteNullField(col.Name)
		return nil
	}
	switch col.Type {
	case mysql.TypeBit:
		if v, ok := col.Value.(uint64); ok {
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
		}
		return cerror.WrapError(
			cerror.ErrDebeziumEncodeFailed,
			errors.Errorf(
				"unexpected column value type %T for bit column %s",
				col.Value,
				col.Name))
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if col.Flag.IsBinary() {
			if v, ok := col.Value.([]byte); ok {
				c.writeBinaryField(writer, col.Name, v)
				return nil
			}
			return cerror.WrapError(
				cerror.ErrDebeziumEncodeFailed,
				errors.Errorf(
					"unexpected column value type %T for binary string column %s",
					col.Value,
					col.Name))
		} else {
			if v, ok := col.Value.([]byte); ok {
				writer.WriteStringField(col.Name, string(hack.String(v)))
				return nil
			}
			return cerror.WrapError(
				cerror.ErrDebeziumEncodeFailed,
				errors.Errorf(
					"unexpected column value type %T for non-binary string column %s",
					col.Value,
					col.Name))
		}
	case mysql.TypeEnum:
		if v, ok := col.Value.(uint64); ok {
			enumVar, err := types.ParseEnumValue(ft.GetElems(), v)
			if err != nil {
				return cerror.WrapError(cerror.ErrDebeziumEncodeFailed, err)
			}
			writer.WriteStringField(col.Name, enumVar.Name)
			return nil
		}
		return cerror.WrapError(
			cerror.ErrDebeziumEncodeFailed,
			errors.Errorf(
				"unexpected column value type %T for enum column %s",
				col.Value,
				col.Name))
	case mysql.TypeSet:
		if v, ok := col.Value.(uint64); ok {
			setVar, err := types.ParseSetValue(ft.GetElems(), v)
			if err != nil {
				return cerror.WrapError(cerror.ErrDebeziumEncodeFailed, err)
			}
			writer.WriteStringField(col.Name, setVar.Name)
			return nil
		}
		return cerror.WrapError(
			cerror.ErrDebeziumEncodeFailed,
			errors.Errorf(
				"unexpected column value type %T for set column %s",
				col.Value,
				col.Name))
	case mysql.TypeNewDecimal:
		if v, ok := col.Value.(string); ok {
			floatV, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return cerror.WrapError(
					cerror.ErrDebeziumEncodeFailed,
					err)
			}
			writer.WriteFloat64Field(col.Name, floatV)
			return nil
		}
		return cerror.WrapError(
			cerror.ErrDebeziumEncodeFailed,
			errors.Errorf(
				"unexpected column value type %T for decimal column %s",
				col.Value,
				col.Name))
	case mysql.TypeDate, mysql.TypeNewDate:
		if v, ok := col.Value.(string); ok {
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
		}
		return cerror.WrapError(
			cerror.ErrDebeziumEncodeFailed,
			errors.Errorf(
				"unexpected column value type %T for date column %s",
				col.Value,
				col.Name))
	case mysql.TypeDatetime:
		// Debezium behavior from doc:
		// > Such columns are converted into epoch milliseconds or microseconds based on the
		// > column's precision by using UTC.

		// TODO: For Default Value = CURRENT_TIMESTAMP, the result is incorrect.
		if v, ok := col.Value.(string); ok {
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
		}
		return cerror.WrapError(
			cerror.ErrDebeziumEncodeFailed,
			errors.Errorf(
				"unexpected column value type %T for datetime column %s",
				col.Value,
				col.Name))
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
		if v, ok := col.Value.(string); ok {
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
		}
		return cerror.WrapError(
			cerror.ErrDebeziumEncodeFailed,
			errors.Errorf(
				"unexpected column value type %T for timestamp column %s",
				col.Value,
				col.Name))
	case mysql.TypeDuration:
		// Debezium behavior from doc:
		// > Represents the time value in microseconds and does not include
		// > time zone information. MySQL allows M to be in the range of 0-6.
		if v, ok := col.Value.(string); ok {
			ctx := &stmtctx.StatementContext{}
			d, _, _, err := types.StrToDuration(ctx, v, ft.GetDecimal())
			if err != nil {
				return cerror.WrapError(
					cerror.ErrDebeziumEncodeFailed,
					err)
			}

			writer.WriteInt64Field(col.Name, d.Microseconds())
			return nil
		}
		return cerror.WrapError(
			cerror.ErrDebeziumEncodeFailed,
			errors.Errorf(
				"unexpected column value type %T for time column %s",
				col.Value,
				col.Name))
	case mysql.TypeLonglong:
		if col.Flag.IsUnsigned() {
			// Handle with BIGINT UNSIGNED.
			// Debezium always produce INT64 instead of UINT64 for BIGINT.
			if v, ok := col.Value.(uint64); ok {
				writer.WriteInt64Field(col.Name, int64(v))
				return nil
			}
			return cerror.WrapError(
				cerror.ErrDebeziumEncodeFailed,
				errors.Errorf(
					"unexpected column value type %T for unsigned bigint column %s",
					col.Value,
					col.Name))
		}

		// Note: Although Debezium's doc claims to use INT32 for INT, but it
		// actually uses INT64. Debezium also uses INT32 for SMALLINT.
		// So we only handle with TypeLonglong here.
	}

	writer.WriteAnyField(col.Name, col.Value)
	return nil
}

func (c *Codec) writeBinaryField(writer *util.JSONWriter, fieldName string, value []byte) {
	// TODO: Deal with different binary output later.
	writer.WriteBase64StringField(fieldName, value)
}

func (c *Codec) EncodeRowChangedEvent(
	e *model.RowChangedEvent,
	dest io.Writer,
) error {
	jWriter := util.BorrowJSONWriter(dest)
	defer util.ReturnJSONWriter(jWriter)

	commitTime, _ := tsoutil.ParseTS(e.CommitTs)

	var err error

	jWriter.WriteObject(func() {
		jWriter.WriteObjectField("payload", func() {
			jWriter.WriteObjectField("source", func() {
				jWriter.WriteStringField("version", "2.4.0.Final")
				jWriter.WriteStringField("connector", "TiCDC")
				jWriter.WriteStringField("name", c.clusterID)
				jWriter.WriteInt64Field("ts_ms", commitTime.UnixMilli())
				jWriter.WriteBoolField("snapshot", false)
				jWriter.WriteStringField("db", e.Table.Schema)
				jWriter.WriteStringField("table", e.Table.Table)
				jWriter.WriteInt64Field("server_id", 0)
				jWriter.WriteNullField("gtid")
				jWriter.WriteStringField("file", "")
				jWriter.WriteInt64Field("pos", 0)
				jWriter.WriteInt64Field("row", 0)
				jWriter.WriteInt64Field("thread", 0)
				jWriter.WriteNullField("query")
				jWriter.WriteUint64Field("commit_ts", e.CommitTs)
				jWriter.WriteStringField("cluster_id", c.clusterID)
			})
			jWriter.WriteInt64Field("ts_ms", c.nowFunc().UnixMilli())
			jWriter.WriteNullField("transaction")

			if e.IsInsert() {
				jWriter.WriteStringField("op", "c")
				jWriter.WriteNullField("before")
				err = c.writeColumnsAsField(jWriter, "after", e.Columns, e.ColInfos)
			} else if e.IsDelete() {
				jWriter.WriteStringField("op", "d")
				jWriter.WriteNullField("after")
				err = c.writeColumnsAsField(jWriter, "before", e.PreColumns, e.ColInfos)
			} else if e.IsUpdate() {
				jWriter.WriteStringField("op", "u")
				err = c.writeColumnsAsField(jWriter, "before", e.PreColumns, e.ColInfos)
				if err == nil {
					err = c.writeColumnsAsField(jWriter, "after", e.Columns, e.ColInfos)
				}
			}
		})
	})

	return err
}
