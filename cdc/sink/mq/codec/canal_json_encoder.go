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

package codec

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	canal "github.com/pingcap/tiflow/proto/canal"
	"go.uber.org/zap"
)

// canalJSONBatchEncoder encodes Canal json messages in JSON format
type canalJSONBatchEncoder struct {
	builder     *canalEntryBuilder
	messageBuf  []canalJSONMessageInterface
	callbackBuf []func()
	// When it is true, canal-json would generate TiDB extension information
	// which, at the moment, only includes `tidbWaterMarkType` and `_tidb` fields.
	enableTiDBExtension bool
}

// newCanalJSONBatchEncoder creates a new canalJSONBatchEncoder
func newCanalJSONBatchEncoder() EventBatchEncoder {
	return &canalJSONBatchEncoder{
		builder:             newCanalEntryBuilder(),
		messageBuf:          make([]canalJSONMessageInterface, 0),
		callbackBuf:         make([]func(), 0),
		enableTiDBExtension: false,
	}
}

func (c *canalJSONBatchEncoder) newJSONMessageForDML(e *model.RowChangedEvent) (canalJSONMessageInterface, error) {
	eventType := convertRowEventType(e)
	header := c.builder.buildHeader(e.CommitTs, e.Table.Schema, e.Table.Table, eventType, 1)
	rowData, err := c.builder.buildRowData(e)
	if err != nil {
		return nil, cerrors.WrapError(cerrors.ErrCanalEncodeFailed, err)
	}

	pkCols := e.PrimaryKeyColumns()
	pkNames := make([]string, len(pkCols))
	for i := range pkNames {
		pkNames[i] = pkCols[i].Name
	}

	var nonTrivialRow []*canal.Column
	if e.IsDelete() {
		nonTrivialRow = rowData.BeforeColumns
	} else {
		nonTrivialRow = rowData.AfterColumns
	}

	sqlType := make(map[string]int32, len(nonTrivialRow))
	mysqlType := make(map[string]string, len(nonTrivialRow))
	for i := range nonTrivialRow {
		sqlType[nonTrivialRow[i].Name] = nonTrivialRow[i].SqlType
		mysqlType[nonTrivialRow[i].Name] = nonTrivialRow[i].MysqlType
	}

	var (
		data    map[string]interface{}
		oldData map[string]interface{}
	)

	if len(rowData.BeforeColumns) > 0 {
		oldData = make(map[string]interface{}, len(rowData.BeforeColumns))
		for i := range rowData.BeforeColumns {
			if !rowData.BeforeColumns[i].GetIsNull() {
				oldData[rowData.BeforeColumns[i].Name] = rowData.BeforeColumns[i].Value
			} else {
				oldData[rowData.BeforeColumns[i].Name] = nil
			}
		}
	}

	if len(rowData.AfterColumns) > 0 {
		data = make(map[string]interface{}, len(rowData.AfterColumns))
		for i := range rowData.AfterColumns {
			if !rowData.AfterColumns[i].GetIsNull() {
				data[rowData.AfterColumns[i].Name] = rowData.AfterColumns[i].Value
			} else {
				data[rowData.AfterColumns[i].Name] = nil
			}
		}
	}

	msg := &canalJSONMessage{
		ID:            0, // ignored by both Canal Adapter and Flink
		Schema:        header.SchemaName,
		Table:         header.TableName,
		PKNames:       pkNames,
		IsDDL:         false,
		EventType:     header.GetEventType().String(),
		ExecutionTime: header.ExecuteTime,
		BuildTime:     time.Now().UnixNano() / 1e6, // ignored by both Canal Adapter and Flink
		Query:         "",
		SQLType:       sqlType,
		MySQLType:     mysqlType,
		Data:          make([]map[string]interface{}, 0),
		Old:           nil,
		tikvTs:        e.CommitTs,
	}

	if e.IsDelete() {
		msg.Data = append(msg.Data, oldData)
	} else if e.IsInsert() {
		msg.Data = append(msg.Data, data)
	} else if e.IsUpdate() {
		msg.Old = []map[string]interface{}{oldData}
		msg.Data = append(msg.Data, data)
	} else {
		log.Panic("unreachable event type", zap.Any("event", e))
	}

	if !c.enableTiDBExtension {
		return msg, nil
	}

	return &canalJSONMessageWithTiDBExtension{
		canalJSONMessage: msg,
		Extensions:       &tidbExtension{CommitTs: e.CommitTs},
	}, nil
}

func (c *canalJSONBatchEncoder) newJSONMessageForDDL(e *model.DDLEvent) canalJSONMessageInterface {
	header := c.builder.buildHeader(e.CommitTs, e.TableInfo.Schema, e.TableInfo.Table, convertDdlEventType(e), 1)
	msg := &canalJSONMessage{
		ID:            0, // ignored by both Canal Adapter and Flink
		Schema:        header.SchemaName,
		Table:         header.TableName,
		IsDDL:         true,
		EventType:     header.GetEventType().String(),
		ExecutionTime: header.ExecuteTime,
		BuildTime:     time.Now().UnixNano() / 1e6, // timestamp
		Query:         e.Query,
		tikvTs:        e.CommitTs,
	}

	if !c.enableTiDBExtension {
		return msg
	}

	return &canalJSONMessageWithTiDBExtension{
		canalJSONMessage: msg,
		Extensions:       &tidbExtension{CommitTs: e.CommitTs},
	}
}

func (c *canalJSONBatchEncoder) newJSONMessage4CheckpointEvent(ts uint64) *canalJSONMessageWithTiDBExtension {
	return &canalJSONMessageWithTiDBExtension{
		canalJSONMessage: &canalJSONMessage{
			ID:            0,
			IsDDL:         false,
			EventType:     tidbWaterMarkType,
			ExecutionTime: convertToCanalTs(ts),
			BuildTime:     time.Now().UnixNano() / int64(time.Millisecond), // converts to milliseconds
		},
		Extensions: &tidbExtension{WatermarkTs: ts},
	}
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (c *canalJSONBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	if !c.enableTiDBExtension {
		return nil, nil
	}

	msg := c.newJSONMessage4CheckpointEvent(ts)
	value, err := json.Marshal(msg)
	if err != nil {
		return nil, cerrors.WrapError(cerrors.ErrCanalEncodeFailed, err)
	}
	return newResolvedMsg(config.ProtocolCanalJSON, nil, value, ts), nil
}

// AppendRowChangedEvent implements the interface EventBatchEncoder
func (c *canalJSONBatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	message, err := c.newJSONMessageForDML(e)
	if err != nil {
		return errors.Trace(err)
	}
	c.messageBuf = append(c.messageBuf, message)
	if callback != nil {
		c.callbackBuf = append(c.callbackBuf, callback)
	}
	return nil
}

// EncodeDDLEvent encodes DDL events
func (c *canalJSONBatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*MQMessage, error) {
	message := c.newJSONMessageForDDL(e)
	value, err := json.Marshal(message)
	if err != nil {
		return nil, cerrors.WrapError(cerrors.ErrCanalEncodeFailed, err)
	}
	return newDDLMsg(config.ProtocolCanalJSON, nil, value, e), nil
}

// Build implements the EventBatchEncoder interface
func (c *canalJSONBatchEncoder) Build() []*MQMessage {
	if len(c.messageBuf) == 0 {
		return nil
	}
	ret := make([]*MQMessage, len(c.messageBuf))
	for i, msg := range c.messageBuf {
		value, err := json.Marshal(msg)
		if err != nil {
			log.Panic("canalJSONBatchEncoder", zap.Error(err))
			return nil
		}
		m := newMsg(config.ProtocolCanalJSON, nil, value, msg.getTikvTs(), model.MessageTypeRow, msg.getSchema(), msg.getTable())
		m.IncRowsCount()
		ret[i] = m
	}
	c.messageBuf = make([]canalJSONMessageInterface, 0)
	if len(c.callbackBuf) != 0 && len(c.callbackBuf) == len(ret) {
		for i, c := range c.callbackBuf {
			ret[i].Callback = c
		}
		c.callbackBuf = make([]func(), 0)
	}
	return ret
}

type canalJSONBatchEncoderBuilder struct {
	config *Config
}

func newCanalJSONBatchEncoderBuilder(config *Config) EncoderBuilder {
	return &canalJSONBatchEncoderBuilder{config: config}
}

// Build a `canalJSONBatchEncoder`
func (b *canalJSONBatchEncoderBuilder) Build() EventBatchEncoder {
	encoder := newCanalJSONBatchEncoder()
	encoder.(*canalJSONBatchEncoder).enableTiDBExtension = b.config.enableTiDBExtension

	return encoder
}
