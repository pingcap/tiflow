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
	// oldDataHolder is only used for `update` event
	oldDataHolder map[string]interface{}

	messages []*common.Message
}

const (
	// defaultColumnCount is the default column count for each row
	// since we doesn't know the column count in advance, we use a default value to prevent frequent memory allocation.
	defaultColumnCount = 16
)

// newJSONBatchEncoder creates a new JSONBatchEncoder
func newJSONBatchEncoder(enableTiDBExtension bool) codec.EventBatchEncoder {
	messageHolder := &JSONMessage{
		// for Data field, no matter event type, always be filled with only one item.
		Data: make([]map[string]interface{}, 1),
	}
	messageHolder.Data[0] = make(map[string]interface{}, defaultColumnCount)
	messageHolder.SQLType = make(map[string]int32, defaultColumnCount)
	messageHolder.MySQLType = make(map[string]string, defaultColumnCount)

	encoder := &JSONBatchEncoder{
		builder:             newCanalEntryBuilder(),
		messageHolder:       messageHolder,
		enableTiDBExtension: enableTiDBExtension,
		// canal-json does not batch multiple messages, so only one message is delivered each time
		messages: make([]*common.Message, 1),

		// even though `oldDataHolder` is only used for `update` event, we still preallocate it
		oldDataHolder: make(map[string]interface{}, defaultColumnCount),
	}

	if enableTiDBExtension {
		encoder.messageHolder = &canalJSONMessageWithTiDBExtension{
			JSONMessage: encoder.messageHolder.(*JSONMessage),
			Extensions:  &tidbExtension{},
		}
	}

	return encoder
}

func (c *JSONBatchEncoder) fillByColumns(message *JSONMessage, columns []*model.Column, fillTypes bool) error {
	if len(columns) == 0 {
		return nil
	}

	for _, col := range columns {
		if col != nil {
			mysqlType := getMySQLType(col)
			javaType, err := getJavaSQLType(col, mysqlType)
			if err != nil {
				return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
			}
			if fillTypes {
				message.SQLType[col.Name] = int32(javaType)
				message.MySQLType[col.Name] = mysqlType
			}

			if col.Value == nil {
				message.Data[0][col.Name] = nil
			} else {
				value, err := c.builder.formatValue(col.Value, javaType)
				if err != nil {
					return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
				}
				message.Data[0][col.Name] = value
			}
		}
	}
	return nil
}

func (c *JSONBatchEncoder) fillOldData(columns []*model.Column) error {
	if len(columns) == 0 {
		return nil
	}

	for k := range c.oldDataHolder {
		delete(c.oldDataHolder, k)
	}

	for _, col := range columns {
		if col.Value == nil {
			c.oldDataHolder[col.Name] = nil
		} else {
			javaType, err := getJavaSQLType(col, getMySQLType(col))
			if err != nil {
				return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
			}
			value, err := c.builder.formatValue(col.Value, javaType)
			if err != nil {
				return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
			}
			c.oldDataHolder[col.Name] = value
		}
	}
	return nil
}

func (c *JSONBatchEncoder) newJSONMessageForDML(e *model.RowChangedEvent) error {
	var baseMessage *JSONMessage
	if !c.enableTiDBExtension {
		baseMessage = c.messageHolder.(*JSONMessage)
	} else {
		baseMessage = c.messageHolder.(*canalJSONMessageWithTiDBExtension).JSONMessage
	}
	baseMessage.reset()

	if e.IsDelete() {
		if err := c.fillByColumns(baseMessage, e.PreColumns, true); err != nil {
			return err
		}
		baseMessage.EventType = "DELETE"
	} else if e.IsInsert() {
		if err := c.fillByColumns(baseMessage, e.Columns, true); err != nil {
			return err
		}
		baseMessage.EventType = "INSERT"
	} else if e.IsUpdate() {
		if err := c.fillOldData(e.PreColumns); err != nil {
			return err
		}
		baseMessage.Old = []map[string]interface{}{c.oldDataHolder}

		if err := c.fillByColumns(baseMessage, e.Columns, true); err != nil {
			return err
		}
		baseMessage.EventType = "UPDATE"
	} else {
		log.Panic("unreachable event type", zap.Any("event", e))
	}

	baseMessage.ID = 0 // ignored by both Canal Adapter and Flink
	baseMessage.Schema = e.Table.Schema
	baseMessage.Table = e.Table.Table
	baseMessage.PKNames = e.PrimaryKeyColumnNames()
	baseMessage.IsDDL = false
	baseMessage.ExecutionTime = convertToCanalTs(e.CommitTs)
	baseMessage.BuildTime = time.Now().UnixNano() / 1e6 // ignored by both Canal Adapter and Flink
	baseMessage.Query = ""
	baseMessage.tikvTs = e.CommitTs

	if c.enableTiDBExtension {
		c.messageHolder.(*canalJSONMessageWithTiDBExtension).Extensions.CommitTs = e.CommitTs
	}

	return nil
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

	c.messages[0] = m
	return nil
}

// Build implements the EventJSONBatchEncoder interface
func (c *JSONBatchEncoder) Build() []*common.Message {
	return c.messages
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
