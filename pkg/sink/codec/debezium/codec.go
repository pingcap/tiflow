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
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
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
	// TsMs: In the source object, ts_ms indicates the time that the change was made in the database.
	// https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
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

// See https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-data-types
func (c *Codec) convertToDebeziumField(col *model.Column, ft *types.FieldType) (any, error) {
	if col.Value == nil {
		return nil, nil
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
				return v != 0, nil
			} else {
				buf := make([]byte, 8)
				binary.LittleEndian.PutUint64(buf, uint64(v))

				numBytes := n / 8
				if n%8 != 0 {
					numBytes += 1
				}
				return base64.StdEncoding.EncodeToString(buf[:numBytes]), nil
			}
		}
		return nil, cerror.WrapError(
			cerror.ErrDebeziumEncodeFailed,
			errors.Errorf(
				"unexpected column value type %T for bit column %s",
				col.Value,
				col.Name))
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if col.Flag.IsBinary() {
			if v, ok := col.Value.([]byte); ok {
				return base64.StdEncoding.EncodeToString(v), nil
			}
			return nil, cerror.WrapError(
				cerror.ErrDebeziumEncodeFailed,
				errors.Errorf(
					"unexpected column value type %T for binary string column %s",
					col.Value,
					col.Name))
		} else {
			if v, ok := col.Value.([]byte); ok {
				return string(v), nil
			}
			return nil, cerror.WrapError(
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
				return nil, cerror.WrapError(cerror.ErrDebeziumEncodeFailed, err)
			}
			return enumVar.Name, nil
		}
		return nil, cerror.WrapError(
			cerror.ErrDebeziumEncodeFailed,
			errors.Errorf(
				"unexpected column value type %T for enum column %s",
				col.Value,
				col.Name))
	case mysql.TypeSet:
		if v, ok := col.Value.(uint64); ok {
			setVar, err := types.ParseSetValue(ft.GetElems(), v)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrDebeziumEncodeFailed, err)
			}
			return setVar.Name, nil
		}
		return nil, cerror.WrapError(
			cerror.ErrDebeziumEncodeFailed,
			errors.Errorf(
				"unexpected column value type %T for set column %s",
				col.Value,
				col.Name))
	case mysql.TypeNewDecimal:
		if v, ok := col.Value.(string); ok {
			floatV, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, cerror.WrapError(
					cerror.ErrDebeziumEncodeFailed,
					err)
			}
			return floatV, nil
		}
		return nil, cerror.WrapError(
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
					return 0, nil
				} else {
					return nil, nil
				}
			}
			return t.Unix() / 60 / 60 / 24, nil
		}
		return nil, cerror.WrapError(
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
					return 0, nil
				} else {
					return nil, nil
				}
			}
			if ft.GetDecimal() <= 3 {
				return t.UnixMilli(), nil
			} else {
				return t.UnixMicro(), nil
			}
		}
		return nil, cerror.WrapError(
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
					return nil, nil
				}
			}

			str := t.UTC().Format("2006-01-02T15:04:05")
			fsp := ft.GetDecimal()
			if fsp > 0 {
				tmp := fmt.Sprintf(".%06d", t.Nanosecond()/1000)
				str = str + tmp[:1+fsp]
			}
			str += "Z"

			return str, nil
		}
		return nil, cerror.WrapError(
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
				return nil, cerror.WrapError(
					cerror.ErrDebeziumEncodeFailed,
					err)
			}
			return d.Microseconds(), nil
		}
		return nil, cerror.WrapError(
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
				return int64(v), nil
			}
			return nil, cerror.WrapError(
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
	return col.Value, nil
}

func (c *Codec) rowChangeToDebeziumMsg(e *model.RowChangedEvent) (*debeziumDataChangeMsg, error) {
	commitTime, _ := tsoutil.ParseTS(e.CommitTs)

	source := &debeziumMsgSource{
		Version:   "2.4.0.Final",
		Connector: "TiCDC",
		Name:      c.clusterID,
		TsMs:      commitTime.UnixMilli(),
		Snapshot:  false,
		Db:        e.Table.Schema,
		Table:     e.Table.Table,
		ServerID:  0,
		GtID:      nil,
		File:      "",
		Pos:       0,
		Row:       0,
		Thread:    0,
		Query:     nil,

		CommitTs:  e.CommitTs,
		ClusterID: c.clusterID,
	}

	payload := &debeziumDataChangeMsgPayload{
		Source:      source,
		TsMs:        c.nowFunc().UnixMilli(),
		Transaction: nil,
	}

	applyBefore := func() error {
		payload.Before = make(map[string]any)
		for i, col := range e.PreColumns {
			value, err := c.convertToDebeziumField(col, e.ColInfos[i].Ft)
			if err != nil {
				return err
			}
			payload.Before[col.Name] = value
		}
		return nil
	}

	applyAfter := func() error {
		payload.After = make(map[string]any)
		for i, col := range e.Columns {
			value, err := c.convertToDebeziumField(col, e.ColInfos[i].Ft)
			if err != nil {
				return err
			}
			payload.After[col.Name] = value
		}
		return nil
	}

	if e.IsInsert() {
		payload.Op = "c"
		payload.Before = nil
		if err := applyAfter(); err != nil {
			return nil, err
		}
	} else if e.IsDelete() {
		payload.Op = "d"
		payload.After = nil
		if err := applyBefore(); err != nil {
			return nil, err
		}
	} else if e.IsUpdate() {
		payload.Op = "u"
		if err := applyBefore(); err != nil {
			return nil, err
		}
		if err := applyAfter(); err != nil {
			return nil, err
		}
	}

	return &debeziumDataChangeMsg{
		Payload: payload,
	}, nil
}

func (c *Codec) EncodeRowChangedEvent(
	e *model.RowChangedEvent,
) ([]byte, error) {
	m, err := c.rowChangeToDebeziumMsg(e)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return json.Marshal(m)
}
