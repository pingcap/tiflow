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
	"sort"
	"strings"
	"time"

	"github.com/mailru/easyjson/jwriter"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	canal "github.com/pingcap/tiflow/proto/canal"
	"go.uber.org/zap"
)

const tidbWaterMarkType = "TIDB_WATERMARK"

// CanalFlatEventBatchEncoder encodes Canal flat messages in JSON format
type CanalFlatEventBatchEncoder struct {
	builder    *canalEntryBuilder
	messageBuf []*MQMessage
	// When it is true, canal-json would generate TiDB extension information
	// which, at the moment, only includes `tidbWaterMarkType` and `_tidb` fields.
	enableTiDBExtension bool
}

// NewCanalFlatEventBatchEncoder creates a new CanalFlatEventBatchEncoder
func NewCanalFlatEventBatchEncoder() EventBatchEncoder {
	return &CanalFlatEventBatchEncoder{
		builder:             NewCanalEntryBuilder(),
		messageBuf:          make([]*MQMessage, 0),
		enableTiDBExtension: false,
	}
}

type canalFlatEventBatchEncoderBuilder struct {
	config *Config
}

// Build a `CanalFlatEventBatchEncoder`
func (b *canalFlatEventBatchEncoderBuilder) Build() EventBatchEncoder {
	encoder := NewCanalFlatEventBatchEncoder()
	encoder.(*CanalFlatEventBatchEncoder).enableTiDBExtension = b.config.enableTiDBExtension

	return encoder
}

func newCanalFlatEventBatchEncoderBuilder(config *Config) EncoderBuilder {
	return &canalFlatEventBatchEncoderBuilder{config: config}
}

// The TiCDC Canal-JSON implementation extend the official format with a TiDB extension field.
// canalFlatMessageInterface is used to support this without affect the original format.
type canalFlatMessageInterface interface {
	getTikvTs() uint64
	getSchema() *string
	getTable() *string
	getCommitTs() uint64
	getQuery() string
	getOld() map[string]interface{}
	getData() map[string]interface{}
	getMySQLType() map[string]string
	getJavaSQLType() map[string]int32
	mqMessageType() model.MessageType
	eventType() canal.EventType
	pkNameSet() map[string]struct{}
}

// JSONMessage adapted from https://github.com/alibaba/canal/blob/b54bea5e3337c9597c427a53071d214ff04628d1/protocol/src/main/java/com/alibaba/otter/canal/protocol/FlatMessage.java#L1
type JSONMessage struct {
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

func (c *JSONMessage) getTikvTs() uint64 {
	return c.tikvTs
}

func (c *JSONMessage) getSchema() *string {
	return &c.Schema
}

func (c *JSONMessage) getTable() *string {
	return &c.Table
}

// for JSONMessage, we lost the commitTs.
func (c *JSONMessage) getCommitTs() uint64 {
	return 0
}

func (c *JSONMessage) getQuery() string {
	return c.Query
}

func (c *JSONMessage) getOld() map[string]interface{} {
	if c.Old == nil {
		return nil
	}
	return c.Old[0]
}

func (c *JSONMessage) getData() map[string]interface{} {
	if c.Data == nil {
		return nil
	}
	return c.Data[0]
}

func (c *JSONMessage) getMySQLType() map[string]string {
	return c.MySQLType
}

func (c *JSONMessage) getJavaSQLType() map[string]int32 {
	return c.SQLType
}

func (c *JSONMessage) mqMessageType() model.MessageType {
	if c.IsDDL {
		return model.MessageTypeDDL
	}

	if c.EventType == tidbWaterMarkType {
		return model.MessageTypeResolved
	}

	return model.MessageTypeRow
}

func (c *JSONMessage) eventType() canal.EventType {
	return canal.EventType(canal.EventType_value[c.EventType])
}

func (c *JSONMessage) pkNameSet() map[string]struct{} {
	result := make(map[string]struct{}, len(c.PKNames))
	for _, item := range c.PKNames {
		result[item] = struct{}{}
	}
	return result
}

type tidbExtension struct {
	CommitTs    uint64 `json:"commitTs,omitempty"`
	WatermarkTs uint64 `json:"watermarkTs,omitempty"`
}

type canalFlatMessageWithTiDBExtension struct {
	*JSONMessage
	// Extensions is a TiCDC custom field that different from official Canal-JSON format.
	// It would be useful to store something for special usage.
	// At the moment, only store the `tso` of each event,
	// which is useful if the message consumer needs to restore the original transactions.
	Extensions *tidbExtension `json:"_tidb"`
}

func (c *canalFlatMessageWithTiDBExtension) getCommitTs() uint64 {
	return c.Extensions.CommitTs
}

func (c *CanalFlatEventBatchEncoder) newFlatMessageForDML(e *model.RowChangedEvent) (*MQMessage, error) {
	isDelete := e.IsDelete()
	mysqlTypeMap := make(map[string]string, len(e.Columns))

	filling := func(columns []*model.Column, out *jwriter.Writer) error {
		if len(columns) == 0 {
			out.RawString("null")
			return nil
		}
		out.RawByte('[')
		out.RawByte('{')
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
					return cerrors.WrapError(cerrors.ErrCanalEncodeFailed, err)
				}
				value, err := c.builder.formatValue(col.Value, javaType)
				if err != nil {
					return cerrors.WrapError(cerrors.ErrCanalEncodeFailed, err)
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
				if emptyColumn {
					out.RawByte('{')
					emptyColumn = false
				} else {
					out.RawByte(',')
				}
				mysqlType := getMySQLType(col)
				javaType, err := getJavaSQLType(col, mysqlType)
				if err != nil {
					return nil, cerrors.WrapError(cerrors.ErrCanalEncodeFailed, err)
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
		if err := filling(e.PreColumns, out); err != nil {
			return nil, err
		}
	} else if e.IsInsert() {
		out.RawString(",\"old\":null")
		out.RawString(",\"data\":")
		if err := filling(e.Columns, out); err != nil {
			return nil, err
		}
	} else if e.IsUpdate() {
		out.RawString(",\"old\":")
		if err := filling(e.PreColumns, out); err != nil {
			return nil, err
		}
		out.RawString(",\"data\":")
		if err := filling(e.Columns, out); err != nil {
			return nil, err
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

	value, err := out.BuildBytes()
	if err != nil {
		log.Panic("CanalFlatEventBatchEncoder", zap.Error(err))
		return nil, nil
	}

	m := &MQMessage{
		Key:       nil,
		Value:     value,
		Ts:        e.CommitTs,
		Schema:    &e.Table.Schema,
		Table:     &e.Table.Table,
		Type:      model.MessageTypeRow,
		Protocol:  config.ProtocolCanalJSON,
		rowsCount: 0,
	}
	m.IncRowsCount()
	return m, nil
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

func (c *CanalFlatEventBatchEncoder) newFlatMessageForDDL(e *model.DDLEvent) canalFlatMessageInterface {
	header := c.builder.buildHeader(e.CommitTs, e.TableInfo.Schema, e.TableInfo.Table, convertDdlEventType(e), 1)
	flatMessage := &JSONMessage{
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
		JSONMessage: flatMessage,
		Extensions:  &tidbExtension{CommitTs: e.CommitTs},
	}
}

func (c *CanalFlatEventBatchEncoder) newFlatMessage4CheckpointEvent(ts uint64) *canalFlatMessageWithTiDBExtension {
	return &canalFlatMessageWithTiDBExtension{
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
	return newResolvedMQMessage(config.ProtocolCanalJSON, nil, value, ts), nil
}

// AppendRowChangedEvent implements the interface EventBatchEncoder
func (c *CanalFlatEventBatchEncoder) AppendRowChangedEvent(
	ctx context.Context,
	topic string,
	e *model.RowChangedEvent,
) error {
	message, err := c.newFlatMessageForDML(e)
	if err != nil {
		return errors.Trace(err)
	}
	c.messageBuf = append(c.messageBuf, message)
	return nil
}

// EncodeDDLEvent encodes DDL events
func (c *CanalFlatEventBatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*MQMessage, error) {
	message := c.newFlatMessageForDDL(e)
	value, err := json.Marshal(message)
	if err != nil {
		return nil, cerrors.WrapError(cerrors.ErrCanalEncodeFailed, err)
	}
	return newDDLMQMessage(config.ProtocolCanalJSON, nil, value, e), nil
}

// Build implements the EventBatchEncoder interface
func (c *CanalFlatEventBatchEncoder) Build() []*MQMessage {
	if len(c.messageBuf) == 0 {
		return nil
	}
	result := c.messageBuf
	c.messageBuf = nil
	return result
}

// CanalFlatEventBatchDecoder decodes the byte into the original message.
type CanalFlatEventBatchDecoder struct {
	data                []byte
	msg                 canalFlatMessageInterface
	enableTiDBExtension bool
}

// NewCanalFlatEventBatchDecoder return a decoder for canal-json
func NewCanalFlatEventBatchDecoder(data []byte, enableTiDBExtension bool) EventBatchDecoder {
	return &CanalFlatEventBatchDecoder{
		data:                data,
		msg:                 nil,
		enableTiDBExtension: enableTiDBExtension,
	}
}

// HasNext implements the EventBatchDecoder interface
func (b *CanalFlatEventBatchDecoder) HasNext() (model.MessageType, bool, error) {
	if len(b.data) == 0 {
		return model.MessageTypeUnknown, false, nil
	}
	var msg canalFlatMessageInterface = &JSONMessage{}
	if b.enableTiDBExtension {
		msg = &canalFlatMessageWithTiDBExtension{
			JSONMessage: &JSONMessage{},
			Extensions:  &tidbExtension{},
		}
	}
	if err := json.Unmarshal(b.data, msg); err != nil {
		log.Error("canal-json decoder unmarshal data failed",
			zap.Error(err), zap.ByteString("data", b.data))
		return model.MessageTypeUnknown, false, err
	}
	b.msg = msg
	b.data = nil

	return b.msg.mqMessageType(), true, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface
// `HasNext` should be called before this.
func (b *CanalFlatEventBatchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.msg == nil || b.msg.mqMessageType() != model.MessageTypeRow {
		return nil, cerrors.ErrCanalDecodeFailed.
			GenWithStack("not found row changed event message")
	}
	result, err := canalFlatMessage2RowChangedEvent(b.msg)
	if err != nil {
		return nil, err
	}
	b.msg = nil
	return result, nil
}

// NextDDLEvent implements the EventBatchDecoder interface
// `HasNext` should be called before this.
func (b *CanalFlatEventBatchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	if b.msg == nil || b.msg.mqMessageType() != model.MessageTypeDDL {
		return nil, cerrors.ErrCanalDecodeFailed.
			GenWithStack("not found ddl event message")
	}

	result := canalFlatMessage2DDLEvent(b.msg)
	b.msg = nil
	return result, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
// `HasNext` should be called before this.
func (b *CanalFlatEventBatchDecoder) NextResolvedEvent() (uint64, error) {
	if b.msg == nil || b.msg.mqMessageType() != model.MessageTypeResolved {
		return 0, cerrors.ErrCanalDecodeFailed.
			GenWithStack("not found resolved event message")
	}

	withExtensionEvent, ok := b.msg.(*canalFlatMessageWithTiDBExtension)
	if !ok {
		log.Error("canal-json resolved event message should have tidb extension, but not found",
			zap.Any("msg", b.msg))
		return 0, cerrors.ErrCanalDecodeFailed.
			GenWithStack("MqMessageTypeResolved tidb extension not found")
	}
	b.msg = nil
	return withExtensionEvent.Extensions.WatermarkTs, nil
}

func canalFlatMessage2RowChangedEvent(flatMessage canalFlatMessageInterface) (*model.RowChangedEvent, error) {
	result := new(model.RowChangedEvent)
	result.CommitTs = flatMessage.getCommitTs()
	result.Table = &model.TableName{
		Schema: *flatMessage.getSchema(),
		Table:  *flatMessage.getTable(),
	}

	mysqlType := flatMessage.getMySQLType()
	javaSQLType := flatMessage.getJavaSQLType()

	var err error
	if flatMessage.eventType() == canal.EventType_DELETE {
		// for `DELETE` event, `data` contain the old data, set it as the `PreColumns`
		result.PreColumns, err = canalFlatJSONColumnMap2SinkColumns(
			flatMessage.getData(), mysqlType, javaSQLType)
		// canal-json encoder does not encode `Flag` information into the result,
		// we have to set the `Flag` to make it can be handled by MySQL Sink.
		// see https://github.com/pingcap/tiflow/blob/7bfce98/cdc/sink/mysql.go#L869-L888
		result.WithHandlePrimaryFlag(flatMessage.pkNameSet())
		return result, err
	}

	// for `INSERT` and `UPDATE`, `data` contain fresh data, set it as the `Columns`
	result.Columns, err = canalFlatJSONColumnMap2SinkColumns(flatMessage.getData(),
		mysqlType, javaSQLType)
	if err != nil {
		return nil, err
	}

	// for `UPDATE`, `old` contain old data, set it as the `PreColumns`
	if flatMessage.eventType() == canal.EventType_UPDATE {
		result.PreColumns, err = canalFlatJSONColumnMap2SinkColumns(flatMessage.getOld(),
			mysqlType, javaSQLType)
		if err != nil {
			return nil, err
		}
	}
	result.WithHandlePrimaryFlag(flatMessage.pkNameSet())

	return result, nil
}

func canalFlatJSONColumnMap2SinkColumns(cols map[string]interface{}, mysqlType map[string]string, javaSQLType map[string]int32) ([]*model.Column, error) {
	result := make([]*model.Column, 0, len(cols))
	for name, value := range cols {
		javaType, ok := javaSQLType[name]
		if !ok {
			// this should not happen, else we have to check encoding for javaSQLType.
			return nil, cerrors.ErrCanalDecodeFailed.GenWithStack(
				"java sql type does not found, column: %+v, mysqlType: %+v", name, javaSQLType)
		}
		mysqlTypeStr, ok := mysqlType[name]
		if !ok {
			// this should not happen, else we have to check encoding for mysqlType.
			return nil, cerrors.ErrCanalDecodeFailed.GenWithStack(
				"mysql type does not found, column: %+v, mysqlType: %+v", name, mysqlType)
		}
		mysqlTypeStr = trimUnsignedFromMySQLType(mysqlTypeStr)
		mysqlType := types.StrToType(mysqlTypeStr)
		col := newColumn(value, mysqlType).decodeCanalJSONColumn(name, JavaSQLType(javaType))
		result = append(result, col)
	}
	if len(result) == 0 {
		return nil, nil
	}
	sort.Slice(result, func(i, j int) bool {
		return strings.Compare(result[i].Name, result[j].Name) > 0
	})
	return result, nil
}

func canalFlatMessage2DDLEvent(flatDDL canalFlatMessageInterface) *model.DDLEvent {
	result := new(model.DDLEvent)
	// we lost the startTs from kafka message
	result.CommitTs = flatDDL.getCommitTs()

	result.TableInfo = new(model.SimpleTableInfo)
	result.TableInfo.Schema = *flatDDL.getSchema()
	result.TableInfo.Table = *flatDDL.getTable()

	// we lost DDL type from canal flat json format, only got the DDL SQL.
	result.Query = flatDDL.getQuery()

	// hack the DDL Type to be compatible with MySQL sink's logic
	// see https://github.com/pingcap/tiflow/blob/0578db337d/cdc/sink/mysql.go#L362-L370
	result.Type = getDDLActionType(result.Query)
	return result
}

// return DDL ActionType by the prefix
// see https://github.com/pingcap/tidb/blob/6dbf2de2f/parser/model/ddl.go#L101-L102
func getDDLActionType(query string) timodel.ActionType {
	query = strings.ToLower(query)
	if strings.HasPrefix(query, "create schema") || strings.HasPrefix(query, "create database") {
		return timodel.ActionCreateSchema
	}
	if strings.HasPrefix(query, "drop schema") || strings.HasPrefix(query, "drop database") {
		return timodel.ActionDropSchema
	}

	return timodel.ActionNone
}
