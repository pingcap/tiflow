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

package canal

import (
	"context"
	"time"

	"github.com/goccy/go-json"
	"github.com/mailru/easyjson/jwriter"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

func fillColumns(columns []*model.Column, out *jwriter.Writer,
	onlyOutputUpdatedColumn bool,
	onlyHandleKeyColumns bool,
	newColumnMap map[string]*model.Column,
	builder *canalEntryBuilder,
) error {
	if len(columns) == 0 {
		out.RawString("null")
		return nil
	}
	out.RawByte('[')
	out.RawByte('{')
	isFirst := true
	for _, col := range columns {
		if col != nil {
			// column equal, do not output it
			if onlyOutputUpdatedColumn && shouldIgnoreColumn(col, newColumnMap) {
				continue
			}
			if onlyHandleKeyColumns && !col.Flag.IsHandleKey() {
				continue
			}
			if isFirst {
				isFirst = false
			} else {
				out.RawByte(',')
			}
			mysqlType := getMySQLType(col)
			javaType, err := getJavaSQLType(col, mysqlType)
			if err != nil {
				return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
			}
			value, err := builder.formatValue(col.Value, javaType)
			if err != nil {
				return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
			}
			out.String(col.Name)
			out.RawByte(':')
			if col.Value == nil {
				out.RawString("null")
			} else {
				out.String(value)
			}
		}
	}
	out.RawByte('}')
	out.RawByte(']')
	return nil
}

func newJSONMessageForDML(
	e *model.RowChangedEvent,
	config *common.Config,
	builder *canalEntryBuilder,
	messageTooLarge bool,
) ([]byte, error) {
	isDelete := e.IsDelete()
	mysqlTypeMap := make(map[string]string, len(e.Columns))

	onlyHandleKey := messageTooLarge
	if isDelete && config.DeleteOnlyHandleKeyColumns {
		onlyHandleKey = true
	}

	out := &jwriter.Writer{}
	out.RawByte('{')
	{
		const prefix string = ",\"id\":"
		out.RawString(prefix[1:])
		out.Int64(0) // ignored by both Canal Adapter and Flink
	}
	{
		const prefix string = ",\"database\":"
		out.RawString(prefix)
		out.String(e.Table.Schema)
	}
	{
		const prefix string = ",\"table\":"
		out.RawString(prefix)
		out.String(e.Table.Table)
	}
	{
		const prefix string = ",\"pkNames\":"
		out.RawString(prefix)
		pkNames := e.PrimaryKeyColumnNames()
		if pkNames == nil {
			out.RawString("null")
		} else {
			out.RawByte('[')
			for v25, v26 := range pkNames {
				if v25 > 0 {
					out.RawByte(',')
				}
				out.String(v26)
			}
			out.RawByte(']')
		}
	}
	{
		const prefix string = ",\"isDdl\":"
		out.RawString(prefix)
		out.Bool(false)
	}
	{
		const prefix string = ",\"type\":"
		out.RawString(prefix)
		out.String(eventTypeString(e))
	}
	{
		const prefix string = ",\"es\":"
		out.RawString(prefix)
		out.Int64(convertToCanalTs(e.CommitTs))
	}
	{
		const prefix string = ",\"ts\":"
		out.RawString(prefix)
		out.Int64(time.Now().UnixMilli()) // ignored by both Canal Adapter and Flink
	}
	{
		const prefix string = ",\"sql\":"
		out.RawString(prefix)
		out.String("")
	}
	{
		columns := e.PreColumns
		if !isDelete {
			columns = e.Columns
		}
		const prefix string = ",\"sqlType\":"
		out.RawString(prefix)
		emptyColumn := true
		for _, col := range columns {
			if col != nil {
				if onlyHandleKey && !col.Flag.IsHandleKey() {
					continue
				}
				if emptyColumn {
					out.RawByte('{')
					emptyColumn = false
				} else {
					out.RawByte(',')
				}
				mysqlType := getMySQLType(col)
				javaType, err := getJavaSQLType(col, mysqlType)
				if err != nil {
					return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
				}
				out.String(col.Name)
				out.RawByte(':')
				out.Int32(int32(javaType))
				mysqlTypeMap[col.Name] = mysqlType
			}
		}
		if emptyColumn {
			out.RawString(`null`)
		} else {
			out.RawByte('}')
		}
	}
	{
		const prefix string = ",\"mysqlType\":"
		out.RawString(prefix)
		if mysqlTypeMap == nil {
			out.RawString(`null`)
		} else {
			out.RawByte('{')
			isFirst := true
			for typeKey, typeValue := range mysqlTypeMap {
				if isFirst {
					isFirst = false
				} else {
					out.RawByte(',')
				}
				out.String(typeKey)
				out.RawByte(':')
				out.String(typeValue)
			}
			out.RawByte('}')
		}
	}

	if e.IsDelete() {
		out.RawString(",\"old\":null")
		out.RawString(",\"data\":")
		if err := fillColumns(e.PreColumns, out, false, onlyHandleKey, nil, builder); err != nil {
			return nil, err
		}
	} else if e.IsInsert() {
		out.RawString(",\"old\":null")
		out.RawString(",\"data\":")
		if err := fillColumns(e.Columns, out, false, onlyHandleKey, nil, builder); err != nil {
			return nil, err
		}
	} else if e.IsUpdate() {
		var newColsMap map[string]*model.Column
		if config.OnlyOutputUpdatedColumns {
			newColsMap = make(map[string]*model.Column, len(e.Columns))
			for _, col := range e.Columns {
				newColsMap[col.Name] = col
			}
		}
		out.RawString(",\"old\":")
		if err := fillColumns(e.PreColumns, out, config.OnlyOutputUpdatedColumns, onlyHandleKey, newColsMap, builder); err != nil {
			return nil, err
		}
		out.RawString(",\"data\":")
		if err := fillColumns(e.Columns, out, false, onlyHandleKey, nil, builder); err != nil {
			return nil, err
		}
	} else {
		log.Panic("unreachable event type", zap.Any("event", e))
	}

	if config.EnableTiDBExtension {
		const prefix string = ",\"_tidb\":"
		out.RawString(prefix)
		out.RawByte('{')
		out.RawString("\"commitTs\":")
		out.Uint64(e.CommitTs)

		if messageTooLarge {
			if config.LargeMessageHandle.HandleKeyOnly() {
				out.RawByte(',')
				out.RawString("\"onlyHandleKey\":true")
			}
		}

		out.RawByte('}')
	}
	out.RawByte('}')

	return out.BuildBytes()
}

func eventTypeString(e *model.RowChangedEvent) string {
	if e.IsDelete() {
		return "DELETE"
	}
	if len(e.PreColumns) == 0 {
		return "INSERT"
	}
	return "UPDATE"
}

// JSONRowEventEncoder encodes row event in JSON format
type JSONRowEventEncoder struct {
	builder  *canalEntryBuilder
	messages []*common.Message

	config *common.Config
}

// newJSONRowEventEncoder creates a new JSONRowEventEncoder
func newJSONRowEventEncoder(config *common.Config) codec.RowEventEncoder {
	encoder := &JSONRowEventEncoder{
		builder:  newCanalEntryBuilder(),
		messages: make([]*common.Message, 0, 1),

		config: config,
	}
	return encoder
}

func (c *JSONRowEventEncoder) newJSONMessageForDDL(e *model.DDLEvent) canalJSONMessageInterface {
	msg := &JSONMessage{
		ID:            0, // ignored by both Canal Adapter and Flink
		Schema:        e.TableInfo.TableName.Schema,
		Table:         e.TableInfo.TableName.Table,
		IsDDL:         true,
		EventType:     convertDdlEventType(e).String(),
		ExecutionTime: convertToCanalTs(e.CommitTs),
		BuildTime:     time.Now().UnixMilli(), // timestamp
		Query:         e.Query,
	}

	if !c.config.EnableTiDBExtension {
		return msg
	}

	return &canalJSONMessageWithTiDBExtension{
		JSONMessage: msg,
		Extensions:  &tidbExtension{CommitTs: e.CommitTs},
	}
}

func (c *JSONRowEventEncoder) newJSONMessage4CheckpointEvent(
	ts uint64,
) *canalJSONMessageWithTiDBExtension {
	return &canalJSONMessageWithTiDBExtension{
		JSONMessage: &JSONMessage{
			ID:            0,
			IsDDL:         false,
			EventType:     tidbWaterMarkType,
			ExecutionTime: convertToCanalTs(ts),
			BuildTime:     time.Now().UnixNano() / int64(time.Millisecond), // converts to milliseconds
		},
		Extensions: &tidbExtension{WatermarkTs: ts},
	}
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (c *JSONRowEventEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	if !c.config.EnableTiDBExtension {
		return nil, nil
	}

	msg := c.newJSONMessage4CheckpointEvent(ts)
	value, err := json.Marshal(msg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}
	return common.NewResolvedMsg(config.ProtocolCanalJSON, nil, value, ts), nil
}

// AppendRowChangedEvent implements the interface EventJSONBatchEncoder
func (c *JSONRowEventEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	value, err := newJSONMessageForDML(e, c.config, c.builder, false)
	if err != nil {
		return errors.Trace(err)
	}

	m := &common.Message{
		Key:      nil,
		Value:    value,
		Ts:       e.CommitTs,
		Schema:   &e.Table.Schema,
		Table:    &e.Table.Table,
		Type:     model.MessageTypeRow,
		Protocol: config.ProtocolCanalJSON,
		Callback: callback,
	}
	m.IncRowsCount()

	if m.Length() > c.config.MaxMessageBytes {
		// for single message that is longer than max-message-bytes, do not send it.
		if c.config.LargeMessageHandle.Disabled() {
			log.Error("Single message is too large for canal-json",
				zap.Int("maxMessageBytes", c.config.MaxMessageBytes),
				zap.Int("length", m.Length()),
				zap.Any("table", e.Table))
			return cerror.ErrMessageTooLarge.GenWithStackByArgs()
		}

		if c.config.LargeMessageHandle.HandleKeyOnly() {
			value, err = newJSONMessageForDML(e, c.config, c.builder, true)
			if err != nil {
				return cerror.ErrMessageTooLarge.GenWithStackByArgs()
			}
			m.Value = value
			if m.Length() > c.config.MaxMessageBytes {
				log.Error("Single message is too large for canal-json, only encode handle-key columns",
					zap.Int("maxMessageBytes", c.config.MaxMessageBytes),
					zap.Int("length", m.Length()),
					zap.Any("table", e.Table))
				return cerror.ErrMessageTooLarge.GenWithStackByArgs()
			}
		}
	}

	c.messages = append(c.messages, m)
	return nil
}

// Build implements the RowEventEncoder interface
func (c *JSONRowEventEncoder) Build() []*common.Message {
	if len(c.messages) == 0 {
		return nil
	}

	result := c.messages
	c.messages = nil
	return result
}

// EncodeDDLEvent encodes DDL events
func (c *JSONRowEventEncoder) EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error) {
	message := c.newJSONMessageForDDL(e)
	value, err := json.Marshal(message)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}
	return common.NewDDLMsg(config.ProtocolCanalJSON, nil, value, e), nil
}

type jsonRowEventEncoderBuilder struct {
	config *common.Config
}

// NewJSONRowEventEncoderBuilder creates a canal-json batchEncoderBuilder.
func NewJSONRowEventEncoderBuilder(config *common.Config) codec.RowEventEncoderBuilder {
	return &jsonRowEventEncoderBuilder{config: config}
}

// Build a `jsonRowEventEncoderBuilder`
func (b *jsonRowEventEncoderBuilder) Build() codec.RowEventEncoder {
	return newJSONRowEventEncoder(b.config)
}

func shouldIgnoreColumn(col *model.Column,
	newColumnMap map[string]*model.Column,
) bool {
	newCol, ok := newColumnMap[col.Name]
	if ok && newCol != nil {
		// sql type is not equal
		if newCol.Type != col.Type {
			return false
		}
		// value equal
		if codec.IsColumnValueEqual(newCol.Value, col.Value) {
			return true
		}
	}
	return false
}
