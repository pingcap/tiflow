// Copyright 2020 PingCAP, Inc.
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
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	canal "github.com/pingcap/tiflow/proto/canal"
	"go.uber.org/zap"
)

// CanalFlatEventBatchEncoder encodes Canal flat messages in JSON format
type CanalFlatEventBatchEncoder struct {
	builder       *canalEntryBuilder
	unresolvedBuf []*canalFlatMessage
	resolvedBuf   []*canalFlatMessage
}

// NewCanalFlatEventBatchEncoder creates a new CanalFlatEventBatchEncoder
func NewCanalFlatEventBatchEncoder() EventBatchEncoder {
	return &CanalFlatEventBatchEncoder{
		builder:       NewCanalEntryBuilder(),
		unresolvedBuf: make([]*canalFlatMessage, 0),
		resolvedBuf:   make([]*canalFlatMessage, 0),
	}
}

// adapted from https://github.com/alibaba/canal/blob/master/protocol/src/main/java/com/alibaba/otter/canal/protocol/FlatMessage.java
type canalFlatMessage struct {
	// ignored by consumers
	ID            int64    `json:"id"`
	Schema        string   `json:"database"`
	Table         string   `json:"table"`
	PKNames       []string `json:"pkNames"`
	IsDDL         bool     `json:"isDdl"`
	EventType     string   `json:"type"`
	ExecutionTime int64    `json:"es"`
	// officially the time of building the MQ message, actually ignored
	BuildTime uint64            `json:"ts"`
	Query     string            `json:"sql"`
	SQLType   map[string]int32  `json:"sqlType"`
	MySQLType map[string]string `json:"mysqlType"`
	// A Datum should be a string or nil
	Data []map[string]interface{} `json:"data"`
	Old  []map[string]interface{} `json:"old"`
	// Used internally by CanalFlatEventBatchEncoder
	tikvTs uint64
}

func (c *CanalFlatEventBatchEncoder) newFlatMessageForDML(e *model.RowChangedEvent) (*canalFlatMessage, error) {
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
	} else {
		// The event type is DELETE
		// The following line is important because Alibaba's adapter expects this, and so does Flink.
		data = oldData
	}

	ret := &canalFlatMessage{
		ID:            0, // ignored by both Canal Adapter and Flink
		Schema:        header.SchemaName,
		Table:         header.TableName,
		PKNames:       pkNames,
		IsDDL:         false,
		EventType:     header.GetEventType().String(),
		ExecutionTime: header.ExecuteTime,
		BuildTime:     0, // ignored by both Canal Adapter and Flink
		Query:         "",
		SQLType:       sqlType,
		MySQLType:     mysqlType,
		Data:          make([]map[string]interface{}, 0),
		Old:           make([]map[string]interface{}, 0),
		tikvTs:        e.CommitTs,
	}

	// We need to ensure that both Data and Old have exactly one element,
	// even if the element could be nil. Changing this could break Alibaba's adapter
	ret.Data = append(ret.Data, data)
	ret.Old = append(ret.Old, oldData)

	return ret, nil
}

func (c *CanalFlatEventBatchEncoder) newFlatMessageForDDL(e *model.DDLEvent) *canalFlatMessage {
	header := c.builder.buildHeader(e.CommitTs, e.TableInfo.Schema, e.TableInfo.Table, convertDdlEventType(e), 1)

	ret := &canalFlatMessage{
		ID:            0, // ignored by both Canal Adapter and Flink
		Schema:        header.SchemaName,
		Table:         header.TableName,
		IsDDL:         true,
		EventType:     header.GetEventType().String(),
		ExecutionTime: header.ExecuteTime,
		BuildTime:     0, // ignored by both Canal Adapter and Flink
		Query:         e.Query,
		tikvTs:        e.CommitTs,
	}
	return ret
}

// EncodeCheckpointEvent is no-op
func (c *CanalFlatEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	return nil, nil
}

// AppendRowChangedEvent implements the interface EventBatchEncoder
func (c *CanalFlatEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error) {
	msg, err := c.newFlatMessageForDML(e)
	if err != nil {
		return EncoderNoOperation, errors.Trace(err)
	}
	c.unresolvedBuf = append(c.unresolvedBuf, msg)
	return EncoderNoOperation, nil
}

// AppendResolvedEvent receives the latest resolvedTs
func (c *CanalFlatEventBatchEncoder) AppendResolvedEvent(ts uint64) (EncoderResult, error) {
	nextIdx := 0
	for _, msg := range c.unresolvedBuf {
		if msg.tikvTs <= ts {
			c.resolvedBuf = append(c.resolvedBuf, msg)
		} else {
			break
		}
		nextIdx++
	}
	c.unresolvedBuf = c.unresolvedBuf[nextIdx:]
	if len(c.resolvedBuf) > 0 {
		return EncoderNeedAsyncWrite, nil
	}
	return EncoderNoOperation, nil
}

// EncodeDDLEvent encodes DDL events
func (c *CanalFlatEventBatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*MQMessage, error) {
	msg := c.newFlatMessageForDDL(e)
	value, err := json.Marshal(msg)
	if err != nil {
		return nil, cerrors.WrapError(cerrors.ErrCanalEncodeFailed, err)
	}
	return newDDLMQMessage(ProtocolCanalJSON, nil, value, e), nil
}

// Build implements the EventBatchEncoder interface
func (c *CanalFlatEventBatchEncoder) Build() []*MQMessage {
	if len(c.resolvedBuf) == 0 {
		return nil
	}
	ret := make([]*MQMessage, len(c.resolvedBuf))
	for i, msg := range c.resolvedBuf {
		value, err := json.Marshal(msg)
		if err != nil {
			log.Panic("CanalFlatEventBatchEncoder", zap.Error(err))
			return nil
		}
		m := NewMQMessage(ProtocolCanalJSON, nil, value, msg.tikvTs, model.MqMessageTypeRow, &msg.Schema, &msg.Table)
		m.IncRowsCount()
		ret[i] = m
	}
	c.resolvedBuf = c.resolvedBuf[0:0]
	return ret
}

// MixedBuild is not used here
func (c *CanalFlatEventBatchEncoder) MixedBuild(withVersion bool) []byte {
	panic("MixedBuild not supported by CanalFlatEventBatchEncoder")
}

// Size implements the EventBatchEncoder interface
func (c *CanalFlatEventBatchEncoder) Size() int {
	return -1
}

// Reset is only supported by JSONEventBatchEncoder
func (c *CanalFlatEventBatchEncoder) Reset() {
	panic("not supported")
}

// SetParams is no-op for now
func (c *CanalFlatEventBatchEncoder) SetParams(params map[string]string) error {
	// no op
	return nil
}
