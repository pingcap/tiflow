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

	messageHolder canalJSONMessageInterface
	messages      []*common.Message
}

// newJSONBatchEncoder creates a new JSONBatchEncoder
func newJSONBatchEncoder(enableTiDBExtension bool) codec.EventBatchEncoder {
	encoder := &JSONBatchEncoder{
		builder: newCanalEntryBuilder(),
		messageHolder: &JSONMessage{
			Data: make([]map[string]interface{}, 0),
		},
		enableTiDBExtension: enableTiDBExtension,
		messages:            make([]*common.Message, 0),
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
	var (
		data    map[string]interface{}
		oldData map[string]interface{}
	)
	isDelete := e.IsDelete()
	sqlTypeMap := make(map[string]int32, len(e.Columns))
	mysqlTypeMap := make(map[string]string, len(e.Columns))
	if len(e.PreColumns) > 0 {
		oldData = make(map[string]interface{}, len(e.PreColumns))
	}
	for _, column := range e.PreColumns {
		if column != nil {
			mysqlType := getMySQLType(column)
			javaType, err := getJavaSQLType(column, mysqlType)
			if err != nil {
				return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
			}
			value, err := c.builder.formatValue(column.Value, javaType)
			if err != nil {
				return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
			}
			if isDelete {
				sqlTypeMap[column.Name] = int32(javaType)
				mysqlTypeMap[column.Name] = mysqlType
			}

			if column.Value == nil {
				oldData[column.Name] = nil
			} else {
				oldData[column.Name] = value
			}
		}
	}

	if len(e.Columns) > 0 {
		data = make(map[string]interface{}, len(e.Columns))
	}
	for _, column := range e.Columns {
		if column != nil {
			mysqlType := getMySQLType(column)
			javaType, err := getJavaSQLType(column, mysqlType)
			if err != nil {
				return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
			}
			value, err := c.builder.formatValue(column.Value, javaType)
			if err != nil {
				return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
			}
			if !isDelete {
				sqlTypeMap[column.Name] = int32(javaType)
				mysqlTypeMap[column.Name] = mysqlType
			}
			if column.Value == nil {
				data[column.Name] = nil
			} else {
				data[column.Name] = value
			}
		}
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
	baseMessage.Data = baseMessage.Data[:0]
	baseMessage.Old = nil
	baseMessage.tikvTs = e.CommitTs

	if e.IsDelete() {
		baseMessage.Data = append(baseMessage.Data, oldData)
	} else if e.IsInsert() {
		baseMessage.Data = append(baseMessage.Data, data)
	} else if e.IsUpdate() {
		baseMessage.Old = []map[string]interface{}{oldData}
		baseMessage.Data = append(baseMessage.Data, data)
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

	value, err := json.Marshal(c.messageHolder)
	if err != nil {
		log.Panic("JSONBatchEncoder", zap.Error(err))
		return nil
	}
	m := common.NewMsg(config.ProtocolCanalJSON, nil, value, e.CommitTs,
		model.MessageTypeRow, c.messageHolder.getSchema(), c.messageHolder.getTable())
	m.IncRowsCount()
	m.Callback = callback

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
