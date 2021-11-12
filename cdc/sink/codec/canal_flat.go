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
	"context"
	"encoding/json"
	"strconv"
	"time"

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
	unresolvedBuf []canalFlatMessageInterface
	resolvedBuf   []canalFlatMessageInterface
	// When it is true, canal-json would generate TiDB extension information
	// which, at the moment, only includes `tidbWaterMarkType` and `_tidb` fields.
	enableTiDBExtension bool
}

const tidbWaterMarkType = "TIDB_WATERMARK"

// NewCanalFlatEventBatchEncoder creates a new CanalFlatEventBatchEncoder
func NewCanalFlatEventBatchEncoder() EventBatchEncoder {
	return &CanalFlatEventBatchEncoder{
		builder:             NewCanalEntryBuilder(),
		unresolvedBuf:       make([]canalFlatMessageInterface, 0),
		resolvedBuf:         make([]canalFlatMessageInterface, 0),
		enableTiDBExtension: false,
	}
}

type canalFlatEventBatchEncoderBuilder struct {
	opts map[string]string
}

// Build a `CanalFlatEventBatchEncoder`
func (b *canalFlatEventBatchEncoderBuilder) Build(ctx context.Context) (EventBatchEncoder, error) {
	encoder := NewCanalFlatEventBatchEncoder()
	if err := encoder.SetParams(b.opts); err != nil {
		return nil, cerrors.WrapError(cerrors.ErrKafkaInvalidConfig, err)
	}

	return encoder, nil
}

func newCanalFlatEventBatchEncoderBuilder(opts map[string]string) EncoderBuilder {
	return &canalFlatEventBatchEncoderBuilder{opts: opts}
}

// The TiCDC Canal-JSON implementation extend the official format with a TiDB extension field.
// canalFlatMessageInterface is used to support this without affect the original format.
type canalFlatMessageInterface interface {
	getTikvTs() uint64
	getSchema() *string
	getTable() *string
}

// adapted from https://github.com/alibaba/canal/blob/master/protocol/src/main/java/com/alibaba/otter/canal/protocol/FlatMessage.java
type canalFlatMessage struct {
	// ignored by consumers
	ID        int64    `json:"id"`
	Schema    string   `json:"database"`
	Table     string   `json:"table"`
	PKNames   []string `json:"pkNames"`
	IsDDL     bool     `json:"isDdl"`
	EventType string   `json:"type"`
	// officially the timestamp of the event-time of the message, in milliseconds since Epoch.
	ExecutionTime int64 `json:"es"`
	// officially the timestamp of building the MQ message, in milliseconds since Epoch.
	BuildTime int64 `json:"ts"`
	// SQL that generated the change event, DDL or Query
	Query string `json:"sql"`
	// only works for INSERT / UPDATE / DELETE events, records each column's java representation type.
	SQLType map[string]int32 `json:"sqlType"`
	// only works for INSERT / UPDATE / DELETE events, records each column's mysql representation type.
	MySQLType map[string]string `json:"mysqlType"`
	// A Datum should be a string or nil
	Data []map[string]interface{} `json:"data"`
	Old  []map[string]interface{} `json:"old"`
	// Used internally by CanalFlatEventBatchEncoder
	tikvTs uint64
}

func (c *canalFlatMessage) getTikvTs() uint64 {
	return c.tikvTs
}

func (c *canalFlatMessage) getSchema() *string {
	return &c.Schema
}

func (c *canalFlatMessage) getTable() *string {
	return &c.Table
}

type tidbExtension struct {
	CommitTs    uint64 `json:"commit-ts"`
	WatermarkTs uint64 `json:"watermark-ts"`
}

type canalFlatMessageWithTiDBExtension struct {
	*canalFlatMessage
	// Extensions is a TiCDC custom field that different from official Canal-JSON format.
	// It would be useful to store something for special usage.
	// At the moment, only store the `tso` of each event,
	// which is useful if the message consumer needs to restore the original transactions.
	Extensions *tidbExtension `json:"_tidb"`
}

func (c *canalFlatMessageWithTiDBExtension) getTikvTs() uint64 {
	return c.tikvTs
}

func (c *canalFlatMessageWithTiDBExtension) getSchema() *string {
	return &c.Schema
}

func (c *canalFlatMessageWithTiDBExtension) getTable() *string {
	return &c.Table
}

func (c *CanalFlatEventBatchEncoder) newFlatMessageForDML(e *model.RowChangedEvent) (canalFlatMessageInterface, error) {
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

	flatMessage := &canalFlatMessage{
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
		Old:           make([]map[string]interface{}, 0),
		tikvTs:        e.CommitTs,
	}

	// We need to ensure that both Data and Old have exactly one element,
	// even if the element could be nil. Changing this could break Alibaba's adapter
	flatMessage.Data = append(flatMessage.Data, data)
	flatMessage.Old = append(flatMessage.Old, oldData)

	if !c.enableTiDBExtension {
		return flatMessage, nil
	}

	return &canalFlatMessageWithTiDBExtension{
		canalFlatMessage: flatMessage,
		Extensions:       &tidbExtension{CommitTs: e.CommitTs},
	}, nil
}

func (c *CanalFlatEventBatchEncoder) newFlatMessageForDDL(e *model.DDLEvent) canalFlatMessageInterface {
	header := c.builder.buildHeader(e.CommitTs, e.TableInfo.Schema, e.TableInfo.Table, convertDdlEventType(e), 1)
	flatMessage := &canalFlatMessage{
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
		return flatMessage
	}

	return &canalFlatMessageWithTiDBExtension{
		canalFlatMessage: flatMessage,
		Extensions:       &tidbExtension{CommitTs: e.CommitTs},
	}
}

func (c *CanalFlatEventBatchEncoder) newFlatMessage4CheckpointEvent(ts uint64) *canalFlatMessageWithTiDBExtension {
	return &canalFlatMessageWithTiDBExtension{
		canalFlatMessage: &canalFlatMessage{
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
func (c *CanalFlatEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	if !c.enableTiDBExtension {
		return nil, nil
	}

	msg := c.newFlatMessage4CheckpointEvent(ts)
	value, err := json.Marshal(msg)
	if err != nil {
		return nil, cerrors.WrapError(cerrors.ErrCanalEncodeFailed, err)
	}
	return newResolvedMQMessage(ProtocolCanalJSON, nil, value, ts), nil
}

// AppendRowChangedEvent implements the interface EventBatchEncoder
func (c *CanalFlatEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error) {
	message, err := c.newFlatMessageForDML(e)
	if err != nil {
		return EncoderNoOperation, errors.Trace(err)
	}
	c.unresolvedBuf = append(c.unresolvedBuf, message)
	return EncoderNoOperation, nil
}

// AppendResolvedEvent receives the latest resolvedTs
func (c *CanalFlatEventBatchEncoder) AppendResolvedEvent(ts uint64) (EncoderResult, error) {
	nextIdx := 0
	for _, msg := range c.unresolvedBuf {
		if msg.getTikvTs() <= ts {
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
	message := c.newFlatMessageForDDL(e)
	value, err := json.Marshal(message)
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
		ret[i] = NewMQMessage(ProtocolCanalJSON, nil, value, msg.getTikvTs(), model.MqMessageTypeRow, msg.getSchema(), msg.getTable())
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

func (c *CanalFlatEventBatchEncoder) SetParams(params map[string]string) error {
	if s, ok := params["enable-tidb-extension"]; ok {
		a, err := strconv.ParseBool(s)
		if err != nil {
			return cerrors.WrapError(cerrors.ErrSinkInvalidConfig, err)
		}
		c.enableTiDBExtension = a
	}
	return nil
}
