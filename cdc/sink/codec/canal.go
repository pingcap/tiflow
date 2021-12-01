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
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto" // nolint:staticcheck
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	canal "github.com/pingcap/ticdc/proto/canal"
	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"go.uber.org/zap"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

// compatible with canal-1.1.4
// https://github.com/alibaba/canal/tree/canal-1.1.4
const (
	CanalPacketVersion   int32  = 1
	CanalProtocolVersion int32  = 1
	CanalServerEncode    string = "UTF-8"
)

// convert ts in tidb to timestamp(in ms) in canal
func convertToCanalTs(commitTs uint64) int64 {
	return int64(commitTs >> 18)
}

// get the canal EventType according to the RowChangedEvent
func convertRowEventType(e *model.RowChangedEvent) canal.EventType {
	if e.IsDelete() {
		return canal.EventType_DELETE
	}
	if len(e.PreColumns) == 0 {
		return canal.EventType_INSERT
	}
	return canal.EventType_UPDATE
}

// get the canal EventType according to the DDLEvent
func convertDdlEventType(e *model.DDLEvent) canal.EventType {
	// see https://github.com/alibaba/canal/blob/d53bfd7ee76f8fe6eb581049d64b07d4fcdd692d/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/ddl/DruidDdlParser.java
	switch e.Type {
	case mm.ActionCreateSchema, mm.ActionDropSchema, mm.ActionShardRowID, mm.ActionCreateView,
		mm.ActionDropView, mm.ActionRecoverTable, mm.ActionModifySchemaCharsetAndCollate,
		mm.ActionLockTable, mm.ActionUnlockTable, mm.ActionRepairTable, mm.ActionSetTiFlashReplica,
		mm.ActionUpdateTiFlashReplicaStatus, mm.ActionCreateSequence, mm.ActionAlterSequence,
		mm.ActionDropSequence, mm.ActionModifyTableAutoIdCache, mm.ActionRebaseAutoRandomBase:
		return canal.EventType_QUERY
	case mm.ActionCreateTable:
		return canal.EventType_CREATE
	case mm.ActionRenameTable:
		return canal.EventType_RENAME
	case mm.ActionAddIndex, mm.ActionAddForeignKey, mm.ActionAddPrimaryKey:
		return canal.EventType_CINDEX
	case mm.ActionDropIndex, mm.ActionDropForeignKey, mm.ActionDropPrimaryKey:
		return canal.EventType_DINDEX
	case mm.ActionAddColumn, mm.ActionDropColumn, mm.ActionModifyColumn, mm.ActionRebaseAutoID,
		mm.ActionSetDefaultValue, mm.ActionModifyTableComment, mm.ActionRenameIndex, mm.ActionAddTablePartition,
		mm.ActionDropTablePartition, mm.ActionModifyTableCharsetAndCollate, mm.ActionTruncateTablePartition,
		mm.ActionAddColumns, mm.ActionDropColumns:
		return canal.EventType_ALTER
	case mm.ActionDropTable:
		return canal.EventType_ERASE
	case mm.ActionTruncateTable:
		return canal.EventType_TRUNCATE
	default:
		return canal.EventType_QUERY
	}
}

func isCanalDdl(t canal.EventType) bool {
	// EventType_QUERY is not a ddl type in canal, but in cdc it is.
	// see https://github.com/alibaba/canal/blob/d53bfd7ee76f8fe6eb581049d64b07d4fcdd692d/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/ddl/DruidDdlParser.java
	// &   https://github.com/alibaba/canal/blob/d53bfd7ee76f8fe6eb581049d64b07d4fcdd692d/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L278
	switch t {
	case canal.EventType_CREATE,
		canal.EventType_RENAME,
		canal.EventType_CINDEX,
		canal.EventType_DINDEX,
		canal.EventType_ALTER,
		canal.EventType_ERASE,
		canal.EventType_TRUNCATE:
		return true
	}
	return false
}

type canalEntryBuilder struct {
	bytesDecoder *encoding.Decoder // default charset is ISO-8859-1
}

// build the header of a canal entry
func (b *canalEntryBuilder) buildHeader(commitTs uint64, schema string, table string, eventType canal.EventType, rowCount int) *canal.Header {
	t := convertToCanalTs(commitTs)
	h := &canal.Header{
		VersionPresent:    &canal.Header_Version{Version: CanalProtocolVersion},
		ServerenCode:      CanalServerEncode,
		ExecuteTime:       t,
		SourceTypePresent: &canal.Header_SourceType{SourceType: canal.Type_MYSQL},
		SchemaName:        schema,
		TableName:         table,
		EventTypePresent:  &canal.Header_EventType{EventType: eventType},
	}
	if rowCount > 0 {
		p := &canal.Pair{
			Key:   "rowsCount",
			Value: strconv.Itoa(rowCount),
		}
		h.Props = append(h.Props, p)
	}
	return h
}

func checkIntNumberNegative(value interface{}) bool {
	switch v := value.(type) {
	case int:
		return v < 0
	case int8:
		return v < 0
	case int16:
		return v < 0
	case int32:
		return v < 0
	case int64:
		return v < 0
	case float32:
		return v < 0
	case float64:
		return v < 0
	default:
		log.Panic("get unexpected value type", zap.Any("type", v))
	}
	return false
}

func isText(mysqlType string) bool {
	return strings.Contains(mysqlType, "text")
}

func getJavaSQLType(c *model.Column, mysqlType string) (result JavaSQLType) {
	javaType := MySQLType2JavaType(c.Type, c.Flag.IsBinary())

	switch javaType {
	case JavaSQLTypeBINARY, JavaSQLTypeVARBINARY, JavaSQLTypeLONGVARBINARY:
		if isText(mysqlType) {
			return JavaSQLTypeCLOB
		}
		return JavaSQLTypeBLOB
	}

	// Some special cases handled in canal
	// see https://github.com/alibaba/canal/blob/d53bfd7ee76f8fe6eb581049d64b07d4fcdd692d/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L733
	shouldPromote := c.Flag.IsUnsigned() && checkIntNumberNegative(c.Value)
	if !shouldPromote {
		return javaType
	}

	switch c.Type {
	case mysql.TypeTiny:
		result = JavaSQLTypeSMALLINT
	case mysql.TypeShort, mysql.TypeInt24:
		result = JavaSQLTypeINTEGER
	case mysql.TypeLong:
		result = JavaSQLTypeBIGINT
	case mysql.TypeLonglong:
		result = JavaSQLTypeDECIMAL
	}

	return result
}

// canal-json would convert all value to string.
func (b *canalEntryBuilder) formatValue(value interface{}, mysqlType string, javaType JavaSQLType) (result string, err error) {
	if value == nil {
		return "", nil
	}

	switch v := value.(type) {
	case int64:
		result = strconv.FormatInt(v, 10)
	case uint64:
		result = strconv.FormatUint(v, 10)
	case float32:
		result = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		result = strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		result = v
	case []byte:
		// special handle for text and blob
		// see https://github.com/alibaba/canal/blob/9f6021cf36f78cc8ac853dcf37a1769f359b868b/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L801
		switch javaType {
		case JavaSQLTypeVARCHAR, JavaSQLTypeCHAR:
			result = string(v)
		default:
			// for `JavaSQLTypeBINARY`, `JavaSQLTypeVARBINARY`, `JavaSQLTypeLONGVARBINARY`
			if isText(mysqlType) {
				result = ""
			}
			decoded, err := b.bytesDecoder.Bytes(v)
			if err != nil {
				return "", err
			}
			result = string(decoded)
		}
	default:
		result = fmt.Sprintf("%v", v)
	}
	return result, nil
}

func getMySQLType(c *model.Column, isBinary bool) string {
	mysqlType := types.TypeStr(c.Type)
	if !isBinary {
		return mysqlType
	}

	if types.IsTypeBlob(c.Type) {
		return strings.Replace(mysqlType, "text", "blob", 1)
	}

	if types.IsTypeChar(c.Type) {
		return strings.Replace(mysqlType, "char", "binary", 1)
	}

	return mysqlType
}

// build the Column in the canal RowData
// reference: https://github.com/alibaba/canal/blob/master/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L756-L872
func (b *canalEntryBuilder) buildColumn(c *model.Column, colName string, updated bool) (*canal.Column, error) {
	isBinary := c.Flag.IsBinary()
	mysqlType := getMySQLType(c, isBinary)
	javaType := getJavaSQLType(c, mysqlType)

	value, err := b.formatValue(c.Value, mysqlType, javaType)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	canalColumn := &canal.Column{
		SqlType:       int32(javaType),
		Name:          colName,
		IsKey:         c.Flag.IsPrimaryKey(),
		Updated:       updated,
		IsNullPresent: &canal.Column_IsNull{IsNull: c.Value == nil},
		Value:         value,
		MysqlType:     mysqlType,
	}
	return canalColumn, nil
}

// build the RowData of a canal entry
func (b *canalEntryBuilder) buildRowData(e *model.RowChangedEvent) (*canal.RowData, error) {
	var columns []*canal.Column
	for _, column := range e.Columns {
		if column == nil {
			continue
		}
		c, err := b.buildColumn(column, column.Name, !e.IsDelete())
		if err != nil {
			return nil, errors.Trace(err)
		}
		columns = append(columns, c)
	}
	var preColumns []*canal.Column
	for _, column := range e.PreColumns {
		if column == nil {
			continue
		}
		c, err := b.buildColumn(column, column.Name, !e.IsDelete())
		if err != nil {
			return nil, errors.Trace(err)
		}
		preColumns = append(preColumns, c)
	}

	rowData := &canal.RowData{}
	rowData.BeforeColumns = preColumns
	rowData.AfterColumns = columns
	return rowData, nil
}

// FromRowEvent builds canal entry from cdc RowChangedEvent
func (b *canalEntryBuilder) FromRowEvent(e *model.RowChangedEvent) (*canal.Entry, error) {
	eventType := convertRowEventType(e)
	header := b.buildHeader(e.CommitTs, e.Table.Schema, e.Table.Table, eventType, 1)
	isDdl := isCanalDdl(eventType) // false
	rowData, err := b.buildRowData(e)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rc := &canal.RowChange{
		EventTypePresent: &canal.RowChange_EventType{EventType: eventType},
		IsDdlPresent:     &canal.RowChange_IsDdl{IsDdl: isDdl},
		RowDatas:         []*canal.RowData{rowData},
	}
	rcBytes, err := proto.Marshal(rc)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	// build entry
	entry := &canal.Entry{
		Header:           header,
		EntryTypePresent: &canal.Entry_EntryType{EntryType: canal.EntryType_ROWDATA},
		StoreValue:       rcBytes,
	}
	return entry, nil
}

// FromDdlEvent builds canal entry from cdc DDLEvent
func (b *canalEntryBuilder) FromDdlEvent(e *model.DDLEvent) (*canal.Entry, error) {
	eventType := convertDdlEventType(e)
	header := b.buildHeader(e.CommitTs, e.TableInfo.Schema, e.TableInfo.Table, eventType, -1)
	isDdl := isCanalDdl(eventType)
	rc := &canal.RowChange{
		EventTypePresent: &canal.RowChange_EventType{EventType: eventType},
		IsDdlPresent:     &canal.RowChange_IsDdl{IsDdl: isDdl},
		Sql:              e.Query,
		RowDatas:         nil,
		DdlSchemaName:    e.TableInfo.Schema,
	}
	rcBytes, err := proto.Marshal(rc)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	// build entry
	entry := &canal.Entry{
		Header:           header,
		EntryTypePresent: &canal.Entry_EntryType{EntryType: canal.EntryType_ROWDATA},
		StoreValue:       rcBytes,
	}
	return entry, nil
}

// NewCanalEntryBuilder creates a new canalEntryBuilder
func NewCanalEntryBuilder() *canalEntryBuilder {
	d := charmap.ISO8859_1.NewDecoder()
	return &canalEntryBuilder{
		bytesDecoder: d,
	}
}

// CanalEventBatchEncoder encodes the events into the byte of a batch into.
type CanalEventBatchEncoder struct {
	messages     *canal.Messages
	packet       *canal.Packet
	entryBuilder *canalEntryBuilder
}

// AppendResolvedEvent appends a resolved event to the encoder
// TODO TXN support
func (d *CanalEventBatchEncoder) AppendResolvedEvent(ts uint64) (EncoderResult, error) {
	return EncoderNoOperation, nil
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	// For canal now, there is no such a corresponding type to ResolvedEvent so far.
	// Therefore the event is ignored.
	return nil, nil
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error) {
	entry, err := d.entryBuilder.FromRowEvent(e)
	if err != nil {
		return EncoderNoOperation, errors.Trace(err)
	}
	b, err := proto.Marshal(entry)
	if err != nil {
		return EncoderNoOperation, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}
	d.messages.Messages = append(d.messages.Messages, b)
	return EncoderNoOperation, nil
}

// EncodeDDLEvent implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*MQMessage, error) {
	entry, err := d.entryBuilder.FromDdlEvent(e)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := proto.Marshal(entry)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	messages := new(canal.Messages)
	messages.Messages = append(messages.Messages, b)
	b, err = messages.Marshal()
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	packet := &canal.Packet{
		VersionPresent: &canal.Packet_Version{
			Version: CanalPacketVersion,
		},
		Type: canal.PacketType_MESSAGES,
	}
	packet.Body = b
	b, err = packet.Marshal()
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	return newDDLMQMessage(config.ProtocolCanal, nil, b, e), nil
}

// Build implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) Build() []*MQMessage {
	if len(d.messages.Messages) == 0 {
		return nil
	}

	err := d.refreshPacketBody()
	if err != nil {
		log.Panic("Error when generating Canal packet", zap.Error(err))
	}

	value, err := proto.Marshal(d.packet)
	if err != nil {
		log.Panic("Error when serializing Canal packet", zap.Error(err))
	}
	ret := NewMQMessage(config.ProtocolCanal, nil, value, 0, model.MqMessageTypeRow, nil, nil)
	d.messages.Reset()
	d.resetPacket()
	return []*MQMessage{ret}
}

// MixedBuild implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) MixedBuild(withVersion bool) []byte {
	panic("Mixed Build only use for JsonEncoder")
}

// Size implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) Size() int {
	// TODO: avoid marshaling the messages every time for calculating the size of the packet
	err := d.refreshPacketBody()
	if err != nil {
		panic(err)
	}
	return proto.Size(d.packet)
}

// Reset implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) Reset() {
	panic("Reset only used for JsonEncoder")
}

// SetParams is no-op for now
func (d *CanalEventBatchEncoder) SetParams(params map[string]string) error {
	// no op
	return nil
}

// refreshPacketBody() marshals the messages to the packet body
func (d *CanalEventBatchEncoder) refreshPacketBody() error {
	oldSize := len(d.packet.Body)
	newSize := proto.Size(d.messages)
	if newSize > oldSize {
		// resize packet body slice
		d.packet.Body = append(d.packet.Body, make([]byte, newSize-oldSize)...)
	} else {
		d.packet.Body = d.packet.Body[:newSize]
	}

	_, err := d.messages.MarshalToSizedBuffer(d.packet.Body)
	return err
}

func (d *CanalEventBatchEncoder) resetPacket() {
	d.packet = &canal.Packet{
		VersionPresent: &canal.Packet_Version{
			Version: CanalPacketVersion,
		},
		Type: canal.PacketType_MESSAGES,
	}
}

// NewCanalEventBatchEncoder creates a new CanalEventBatchEncoder.
func NewCanalEventBatchEncoder() EventBatchEncoder {
	encoder := &CanalEventBatchEncoder{
		messages:     &canal.Messages{},
		entryBuilder: NewCanalEntryBuilder(),
	}

	encoder.resetPacket()
	return encoder
}

type canalEventBatchEncoderBuilder struct {
	opts map[string]string
}

// Build a `CanalEventBatchEncoder`
func (b *canalEventBatchEncoderBuilder) Build(ctx context.Context) (EventBatchEncoder, error) {
	encoder := NewCanalEventBatchEncoder()
	if err := encoder.SetParams(b.opts); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	return encoder, nil
}

func newCanalEventBatchEncoderBuilder(opts map[string]string) EncoderBuilder {
	return &canalEventBatchEncoderBuilder{opts: opts}
}
