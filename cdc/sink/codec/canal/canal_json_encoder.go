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
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// JSONBatchEncoder encodes Canal json messages in JSON format
type JSONBatchEncoder struct {
	builder *canalEntryBuilder

	// When it is true, canal-json would generate TiDB extension information
	// which, at the moment, only includes `tidbWaterMarkType` and `_tidb` fields.
	enableTiDBExtension bool

	// messageHolder is used to hold each message and will be reset after each message is encoded.
	messageHolder canalJSONMessageInterface
	messages      []*common.Message
}

// newJSONBatchEncoder creates a new JSONBatchEncoder
func newJSONBatchEncoder(enableTiDBExtension bool) codec.EventBatchEncoder {
	encoder := &JSONBatchEncoder{
		builder: newCanalEntryBuilder(),
		messageHolder: &JSONMessage{
			// for Data field, no matter event type, always be filled with only one item.
			Data: make([]map[string]interface{}, 1),
		},
		enableTiDBExtension: enableTiDBExtension,
		messages:            make([]*common.Message, 0, 1),
	}

	if enableTiDBExtension {
		encoder.messageHolder = &canalJSONMessageWithTiDBExtension{
			JSONMessage: encoder.messageHolder.(*JSONMessage),
			Extensions:  &tidbExtension{},
		}
	}

	return encoder
}

func (c *JSONBatchEncoder) newJSONMessageForDML(e *model.RowChangedEvent) error {
	isDelete := e.IsDelete()
	sqlTypeMap := make(map[string]int32, len(e.Columns))
	mysqlTypeMap := make(map[string]string, len(e.Columns))

	filling := func(columns []*model.Column, out *jwriter.Writer) error {
		if len(columns) == 0 {
			out.RawString("null")
			return nil
		}
		out.RawByte('[')
		isFirst := true
		for _, col := range columns {
			if col != nil {
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
				value, err := c.builder.formatValue(col.Value, javaType)
				if err != nil {
					return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
				}

				out.RawByte('{')
				out.String(col.Name)
				out.RawByte(':')
				if col.Value == nil {
					out.RawString("null")
				} else {
					out.String(value)
				}
				out.RawByte('}')
			}
		}
		out.RawByte(']')
		return nil
	}

	var baseMessage *JSONMessage
	if !c.enableTiDBExtension {
		baseMessage = c.messageHolder.(*JSONMessage)
	} else {
		baseMessage = c.messageHolder.(*canalJSONMessageWithTiDBExtension).JSONMessage
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
		for _, col := range columns {
			if col != nil {
				mysqlType := getMySQLType(col)
				javaType, err := getJavaSQLType(col, mysqlType)
				if err != nil {
					return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
				}
				sqlTypeMap[col.Name] = int32(javaType)
				mysqlTypeMap[col.Name] = mysqlType
			}
		}
	}
	{
		const prefix string = ",\"sqlType\":"
		out.RawString(prefix)
		if sqlTypeMap == nil && (out.Flags&jwriter.NilMapAsEmpty) == 0 {
			out.RawString(`null`)
		} else {
			out.RawByte('{')
			isFirst := true
			for typeKey, typeValue := range sqlTypeMap {
				if isFirst {
					isFirst = false
				} else {
					out.RawByte(',')
				}
				out.String(typeKey)
				out.RawByte(':')
				out.Int32(typeValue)
			}
			out.RawByte('}')
		}
	}
	{
		const prefix string = ",\"mysqlType\":"
		out.RawString(prefix)
		if mysqlTypeMap == nil && (out.Flags&jwriter.NilMapAsEmpty) == 0 {
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
		if err := filling(e.PreColumns, out); err != nil {
			return err
		}
	} else if e.IsInsert() {
		out.RawString(",\"old\":null")
		out.RawString(",\"data\":")
		if err := filling(e.Columns, out); err != nil {
			return err
		}
	} else if e.IsUpdate() {
		out.RawString(",\"old\":")
		if err := filling(e.PreColumns, out); err != nil {
			return err
		}
		out.RawString(",\"old\":null")
		out.RawString(",\"data\":")
		if err := filling(e.Columns, out); err != nil {
			return err
		}
	} else {
		log.Panic("unreachable event type", zap.Any("event", e))
	}

	if c.enableTiDBExtension {
		const prefix string = ",\"_tidb\":"
		out.RawString(prefix)
		out.RawByte('{')
		out.RawString("\"commitTs\":")
		out.Uint64(e.CommitTs)
		out.RawByte('}')
	}
	out.RawByte('}')

	var err error
	baseMessage.jsonData, err = out.BuildBytes()
	return err
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

func (c *JSONBatchEncoder) newJSONMessageForDDL(e *model.DDLEvent) canalJSONMessageInterface {
	msg := &JSONMessage{
		ID:            0, // ignored by both Canal Adapter and Flink
		Schema:        e.TableInfo.TableName.Schema,
		Table:         e.TableInfo.TableName.Table,
		IsDDL:         true,
		EventType:     convertDdlEventType(e).String(),
		ExecutionTime: convertToCanalTs(e.CommitTs),
		BuildTime:     time.Now().UnixMilli(), // timestamp
		Query:         e.Query,
		tikvTs:        e.CommitTs,
	}

	if !c.enableTiDBExtension {
		return msg
	}

	return &canalJSONMessageWithTiDBExtension{
		JSONMessage: msg,
		Extensions:  &tidbExtension{CommitTs: e.CommitTs},
	}
}

func (c *JSONBatchEncoder) newJSONMessage4CheckpointEvent(ts uint64) *canalJSONMessageWithTiDBExtension {
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

// EncodeCheckpointEvent implements the EventJSONBatchEncoder interface
func (c *JSONBatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	if !c.enableTiDBExtension {
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
func (c *JSONBatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	if err := c.newJSONMessageForDML(e); err != nil {
		return errors.Trace(err)
	}
	var value []byte
	if !c.enableTiDBExtension {
		value = c.messageHolder.(*JSONMessage).jsonData
	} else {
		value = c.messageHolder.(*canalJSONMessageWithTiDBExtension).jsonData
	}
	m := &common.Message{
		Key:      nil,
		Value:    value,
		Ts:       e.CommitTs,
		Schema:   c.messageHolder.getSchema(),
		Table:    c.messageHolder.getTable(),
		Type:     model.MessageTypeRow,
		Protocol: config.ProtocolCanalJSON,
		Callback: callback,
	}
	m.IncRowsCount()

	c.messages = append(c.messages, m)
	return nil
}

// Build implements the EventJSONBatchEncoder interface
func (c *JSONBatchEncoder) Build() []*common.Message {
	if len(c.messages) == 0 {
		return nil
	}

	result := c.messages
	c.messages = c.messages[:0]
	return result
}

// EncodeDDLEvent encodes DDL events
func (c *JSONBatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error) {
	message := c.newJSONMessageForDDL(e)
	value, err := json.Marshal(message)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}
	return common.NewDDLMsg(config.ProtocolCanalJSON, nil, value, e), nil
}

type jsonBatchEncoderBuilder struct {
	config *common.Config
}

// NewJSONBatchEncoderBuilder creates a canal-json batchEncoderBuilder.
func NewJSONBatchEncoderBuilder(config *common.Config) codec.EncoderBuilder {
	return &jsonBatchEncoderBuilder{config: config}
}

// Build a `JSONBatchEncoder`
func (b *jsonBatchEncoderBuilder) Build() codec.EventBatchEncoder {
	return newJSONBatchEncoder(b.config.EnableTiDBExtension)
}
