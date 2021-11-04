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
	// When it is true, canal-json would generate TiDB extended information
	// At the moment, only `tidbWaterMarkType` and `_tidb` fields.
	enableTiDBExtension bool
}

const tidbWaterMarkType = "TiDB_WATERMARK"

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

type canalFlatMessageInterface interface {
	newForDML(e *model.RowChangedEvent, builder *canalEntryBuilder) error
	newForDDL(e *model.DDLEvent, builder *canalEntryBuilder)
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

func (c *canalFlatMessage) newForDML(e *model.RowChangedEvent, builder *canalEntryBuilder) error {
	eventType := convertRowEventType(e)
	header := builder.buildHeader(e.CommitTs, e.Table.Schema, e.Table.Table, eventType, 1)
	rowData, err := builder.buildRowData(e)
	if err != nil {
		return cerrors.WrapError(cerrors.ErrCanalEncodeFailed, err)
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

	c.ID = 0 // ignored by both Canal Adapter and Flink
	c.Schema = header.SchemaName
	c.Table = header.TableName
	c.PKNames = pkNames
	c.IsDDL = false
	c.EventType = header.GetEventType().String()
	c.ExecutionTime = header.ExecuteTime
	c.BuildTime = time.Now().UnixNano() / 1e6 // millisecond timestamp since Epoch.
	c.Query = ""
	c.SQLType = sqlType
	c.MySQLType = mysqlType
	c.Data = make([]map[string]interface{}, 0)
	c.Old = make([]map[string]interface{}, 0)
	c.tikvTs = e.CommitTs

	// We need to ensure that both Data and Old have exactly one element,
	// even if the element could be nil. Changing this could break Alibaba's adapter
	c.Data = append(c.Data, data)
	c.Old = append(c.Old, oldData)

	return nil
}

func (c *canalFlatMessage) newForDDL(e *model.DDLEvent, builder *canalEntryBuilder) {
	header := builder.buildHeader(e.CommitTs, e.TableInfo.Schema, e.TableInfo.Table, convertDdlEventType(e), 1)

	c.ID = 0 // ignored by both Canal Adapter and Flink
	c.Schema = header.SchemaName
	c.Table = header.TableName
	c.IsDDL = true
	c.EventType = header.GetEventType().String()
	c.ExecutionTime = header.ExecuteTime
	c.BuildTime = time.Now().UnixNano() / 1e6
	c.Query = e.Query
	c.tikvTs = e.CommitTs
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

type canalFlatMessageWithTiDBExtension struct {
	*canalFlatMessage
	// Props is a TiCDC custom field that different from official Canal-JSON format.
	// It would be useful to store something for special usage.
	// At the moment, only store the `tso` of each event, which is useful if the message consumer needs to restore the original transactions.
	Props map[string]string `json:"props"`
}

func (c *canalFlatMessageWithTiDBExtension) newForDML(e *model.RowChangedEvent, builder *canalEntryBuilder) error {
	flatMessage := &canalFlatMessage{}
	if err := flatMessage.newForDML(e, builder); err != nil {
		return err
	}

	c.canalFlatMessage = flatMessage
	c.Props = map[string]string{
		"tso": strconv.FormatUint(e.CommitTs, 10),
	}
	return nil
}

func (c *canalFlatMessageWithTiDBExtension) newForDDL(e *model.DDLEvent, builder *canalEntryBuilder) {
	flatMessage := &canalFlatMessage{}
	flatMessage.newForDDL(e, builder)

	c.canalFlatMessage = flatMessage
	c.Props = map[string]string{
		"tso": strconv.FormatUint(e.CommitTs, 10),
	}
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

func (c *canalFlatMessageWithTiDBExtension) newForCheckpointEvent(ts uint64) {
	c.ID = 0
	c.IsDDL = false
	c.EventType = tidbWaterMarkType
	c.ExecutionTime = convertToCanalTs(ts)
	c.BuildTime = time.Now().UnixNano() / 1e6
	c.Props = map[string]string{
		"tso": strconv.FormatUint(ts, 10),
	}
}

// newFlatMessage4CheckpointEvent return a `WATERMARK` event typed message
// Since `WATERMARK` is a TiCDC Custom event type, If the message consumer want to handle it properly,
// they should make sure their consumer code can recognize this type.

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (c *CanalFlatEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	if !c.enableTiDBExtension {
		return nil, nil
	}

	msg := &canalFlatMessageWithTiDBExtension{}
	msg.newForCheckpointEvent(ts)
	value, err := json.Marshal(msg)
	if err != nil {
		return nil, cerrors.WrapError(cerrors.ErrCanalEncodeFailed, err)
	}
	return newResolvedMQMessage(ProtocolCanalJSON, nil, value, ts), nil
}

func (c *CanalFlatEventBatchEncoder) getFlatMessageInterface() canalFlatMessageInterface {
	if c.enableTiDBExtension {
		return &canalFlatMessageWithTiDBExtension{}
	}
	return &canalFlatMessage{}
}

func (c *CanalFlatEventBatchEncoder) newFlatMessageForDML(e *model.RowChangedEvent) (canalFlatMessageInterface, error) {
	result := c.getFlatMessageInterface()
	if err := result.newForDML(e, c.builder); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *CanalFlatEventBatchEncoder) newFlatMessageForDDL(e *model.DDLEvent) canalFlatMessageInterface {
	result := c.getFlatMessageInterface()
	result.newForDDL(e, c.builder)

	return result
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
			return err
		}
		c.enableTiDBExtension = a
	}
	return nil
}
