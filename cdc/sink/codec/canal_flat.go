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
	"github.com/pingcap/ticdc/cdc/model"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	canal "github.com/pingcap/ticdc/proto/canal"
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

type canalFlatMessage struct {
	ID            int64               `json:"id"`
	Schema        string              `json:"database"`
	Table         string              `json:"table"`
	PKNames       []string            `json:"pkNames"`
	IsDDL         bool                `json:"isDdl"`
	EventType     string              `json:"type"`
	ExecutionTime int64               `json:"es"`
	BuildTime     uint64              `json:"ts"`
	Query         string              `json:"sql"`
	SQLType       map[string]int32    `json:"sqlType"`
	MySQLType     map[string]string   `json:"mysqlType"`
	Data          []map[string]string `json:"data"`
	Old           []map[string]string `json:"old"`
	tikvTs        uint64
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
		data    map[string]string
		oldData map[string]string
	)

	if len(rowData.BeforeColumns) > 0 {
		oldData = make(map[string]string, len(rowData.BeforeColumns))
		for i := range rowData.BeforeColumns {
			oldData[rowData.BeforeColumns[i].Name] = rowData.BeforeColumns[i].Value
		}
	}

	if len(rowData.AfterColumns) > 0 {
		data = make(map[string]string, len(rowData.AfterColumns))
		for i := range rowData.AfterColumns {
			data[rowData.AfterColumns[i].Name] = rowData.AfterColumns[i].Value
		}
	} else {
		// The event type is DELETE
		// The following line is important because Alibaba's adapter expects this
		// TODO figure out whether flink or other potential data consumers expects this!
		data = oldData
	}

	ret := &canalFlatMessage{
		ID:            0,
		Schema:        header.SchemaName,
		Table:         header.TableName,
		PKNames:       pkNames,
		IsDDL:         false,
		EventType:     header.GetEventType().String(),
		ExecutionTime: header.ExecuteTime,
		BuildTime:     0,
		Query:         "",
		SQLType:       sqlType,
		MySQLType:     mysqlType,
		Data:          make([]map[string]string, 0),
		Old:           make([]map[string]string, 0),
		tikvTs:        e.CommitTs,
	}

	// We need to ensure that both Data and Old have exactly one element,
	// even if the element could be nil. Changing this could break Alibaba's adapter
	ret.Data = append(ret.Data, data)
	ret.Old = append(ret.Old, oldData)

	return ret, nil
}

func (c *CanalFlatEventBatchEncoder) newFlatMessageForDDL(e *model.DDLEvent) (*canalFlatMessage, error) {
	header := c.builder.buildHeader(e.CommitTs, e.TableInfo.Schema, e.TableInfo.Table, convertDdlEventType(e), 1)

	ret := &canalFlatMessage{
		ID:            0,
		Schema:        header.SchemaName,
		Table:         header.TableName,
		IsDDL:         true,
		EventType:     header.GetEventType().String(),
		ExecutionTime: header.ExecuteTime,
		BuildTime:     0,
		Query:         e.Query,
		tikvTs:        e.CommitTs,
	}
	return ret, nil
}

// EncodeCheckpointEvent is no-op
func (c *CanalFlatEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	return nil, nil
}

func (c *CanalFlatEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error) {
	msg, err := c.newFlatMessageForDML(e)
	if err != nil {
		return EncoderNoOperation, errors.Trace(err)
	}
	log.Debug("AppendRowChangedEvent", zap.Reflect("msg", msg), zap.Reflect("event", e))
	c.unresolvedBuf = append(c.unresolvedBuf, msg)
	return EncoderNoOperation, nil
}

// AppendResolvedEvent receives the latest resolvedTs
func (c *CanalFlatEventBatchEncoder) AppendResolvedEvent(ts uint64) (EncoderResult, error) {
	log.Debug("AppendResolvedEvent", zap.Uint64("ts", ts))
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
	msg, err := c.newFlatMessageForDDL(e)
	if err != nil {
		return nil, errors.Trace(err)
	}
	value, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return NewMQMessage(nil, value, e.CommitTs), nil
}

// Build implements the EventBatchEncoder interface
func (c *CanalFlatEventBatchEncoder) Build() []*MQMessage {
	log.Debug("Build")
	if len(c.resolvedBuf) == 0 {
		return nil
	}
	ret := make([]*MQMessage, len(c.resolvedBuf))
	for i := range c.resolvedBuf {
		value, err := json.Marshal(c.resolvedBuf[i])
		if err != nil {
			log.Fatal("CanalFlatEventBatchEncoder", zap.Error(err))
			return nil
		}
		ret[i] = NewMQMessage(nil, value, c.resolvedBuf[i].tikvTs)
		log.Debug("CanalJson", zap.ByteString("message", ret[i].Value))
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

// Reset resets the internal state of the encoder
func (c *CanalFlatEventBatchEncoder) Reset() {
	c.unresolvedBuf = make([]*canalFlatMessage, 0)
	c.resolvedBuf = make([]*canalFlatMessage, 0)
}
