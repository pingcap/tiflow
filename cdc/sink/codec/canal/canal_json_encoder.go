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
			Data: make([]map[string]string, 1),
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

	filling := func(columns []*model.Column, fillTypes bool) (map[string]string, error) {
		if len(columns) == 0 {
			return nil, nil
		}
		data := make(map[string]string, len(columns))
		for _, col := range columns {
			if col != nil {
				mysqlType := getMySQLType(col)
				javaType, err := getJavaSQLType(col, mysqlType)
				if err != nil {
					return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
				}
				value, err := c.builder.formatValue(col.Value, javaType)
				if err != nil {
					return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
				}
				if fillTypes {
					sqlTypeMap[col.Name] = int32(javaType)
					mysqlTypeMap[col.Name] = mysqlType
				}

				if col.Value == nil {
					data[col.Name] = ""
				} else {
					data[col.Name] = value
				}
			}
		}
		return data, nil
	}

	oldData, err := filling(e.PreColumns, isDelete)
	if err != nil {
		return err
	}

	data, err := filling(e.Columns, !isDelete)
	if err != nil {
		return err
	}

	var baseMessage *JSONMessage
	if !c.enableTiDBExtension {
		baseMessage = c.messageHolder.(*JSONMessage)
	} else {
		baseMessage = c.messageHolder.(*canalJSONMessageWithTiDBExtension).JSONMessage
	}

	baseMessage.ID = 0 // ignored by both Canal Adapter and Flink
	baseMessage.Schema = e.Table.Schema
	baseMessage.Table = e.Table.Table
	baseMessage.PKNames = e.PrimaryKeyColumnNames()
	baseMessage.IsDDL = false
	baseMessage.EventType = eventTypeString(e)
	baseMessage.ExecutionTime = convertToCanalTs(e.CommitTs)
	baseMessage.BuildTime = time.Now().UnixNano() / 1e6 // ignored by both Canal Adapter and Flink
	baseMessage.Query = ""
	baseMessage.SQLType = sqlTypeMap
	baseMessage.MySQLType = mysqlTypeMap
	baseMessage.Old = nil
	baseMessage.tikvTs = e.CommitTs

	if e.IsDelete() {
		baseMessage.Data[0] = oldData
	} else if e.IsInsert() {
		baseMessage.Data[0] = data
	} else if e.IsUpdate() {
		baseMessage.Data[0] = data
		baseMessage.Old = []map[string]string{oldData}
	} else {
		log.Panic("unreachable event type", zap.Any("event", e))
	}

	if c.enableTiDBExtension {
		c.messageHolder.(*canalJSONMessageWithTiDBExtension).Extensions.CommitTs = e.CommitTs
	}

	return nil
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
		BuildTime:     time.Now().UnixNano() / 1e6, // timestamp
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
	var err error
	if !c.enableTiDBExtension {
		value, err = c.messageHolder.(*JSONMessage).MarshalJSON()
	} else {
		value, err = c.messageHolder.(*canalJSONMessageWithTiDBExtension).MarshalJSON()
	}
	if err != nil {
		log.Panic("JSONBatchEncoder", zap.Error(err))
		return nil
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
