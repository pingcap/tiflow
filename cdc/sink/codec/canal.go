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
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	mm "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"golang.org/x/text/encoding/charmap"

	canal "github.com/pingcap/ticdc/proto/canal"
	"strconv"
)

//compatible with canal-1.1.4
const (
	CanalPacketVersion   int32  = 1
	CanalProtocolVersion int32  = 1
	CanalServerEncode    string = "UTF-8"
)

func convertToCanalTs(commitTs uint64) int64 {
	return int64(commitTs >> 18)
}

func convertRowEventType(e *model.RowChangedEvent) canal.EventType {
	if e.Delete {
		return canal.EventType_DELETE
	} else {
		return canal.EventType_UPDATE
	}
}

func convertDdlEventType(e *model.DDLEvent) canal.EventType {
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

func buildHeader(commitTs uint64, schema string, table string, eventType canal.EventType, rowCount int) *canal.Header {
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

func buildColumn(c *model.Column, colName string, updated bool) (*canal.Column, error) {
	sqlType := MysqlToJavaType(c.Type)
	// Some cases specially handled in canal
	// see https://github.com/alibaba/canal/blob/d53bfd7ee76f8fe6eb581049d64b07d4fcdd692d/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L733
	switch c.Type {
	// Since we cannot get the signed/unsigned flag of the column in the RowChangedEvent currently,
	// we promote the sqlTypes regardless of the flag.
	case mysql.TypeTiny:
		sqlType = JavaSqlTypeSMALLINT
	case mysql.TypeShort:
		sqlType = JavaSqlTypeINTEGER
	case mysql.TypeInt24:
		sqlType = JavaSqlTypeINTEGER
	case mysql.TypeLong:
		sqlType = JavaSqlTypeBIGINT
	case mysql.TypeLonglong:
		sqlType = JavaSqlTypeDECIMAL
	}

	isKey := c.WhereHandle != nil && *c.WhereHandle
	isNull := c.Value == nil
	value := ""
	if !isNull {
		switch v := c.Value.(type) {
		case int64:
			value = strconv.FormatInt(v, 10)
		case uint64:
			value = strconv.FormatUint(v, 10)
		case float32:
			value = strconv.FormatFloat(float64(v), 'f', -1, 32)
		case float64:
			value = strconv.FormatFloat(v, 'f', -1, 64)
		case string:
			value = v
		case []byte:
			b, err := charmap.ISO8859_1.NewDecoder().Bytes(v)
			if err != nil {
				return nil, errors.Trace(err)
			}
			value = string(b)
			sqlType = JavaSqlTypeBLOB // change sql type to Blob according to canal
		default:
			value = fmt.Sprintf("%v", v)
		}
	}

	canalColumn := &canal.Column{
		SqlType:       int32(sqlType),
		Name:          colName,
		IsKey:         isKey,
		Updated:       updated,
		IsNullPresent: &canal.Column_IsNull{IsNull: isNull},
		Value:         value,
	}
	return canalColumn, nil
}

func buildRowData(e *model.RowChangedEvent) (*canal.RowData, error) {
	var columns []*canal.Column
	for n, c := range e.Columns {
		cc, err := buildColumn(c, n, !e.Delete)
		if err != nil {
			return nil, errors.Trace(err)
		}
		columns = append(columns, cc)
	}

	rowData := &canal.RowData{}
	if e.Delete {
		rowData.BeforeColumns = columns
	} else {
		rowData.AfterColumns = columns
	}
	return rowData, nil
}

func rowEventToCanalEntry(e *model.RowChangedEvent) (*canal.Entry, error) {
	eventType := convertRowEventType(e)
	header := buildHeader(e.CommitTs, e.Table.Schema, e.Table.Table, eventType, 1)
	isDdl := isCanalDdl(eventType) // false
	rowData, err := buildRowData(e)
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
		return nil, errors.Trace(err)
	}

	// build entry
	entry := &canal.Entry{
		Header:           header,
		EntryTypePresent: &canal.Entry_EntryType{EntryType: canal.EntryType_ROWDATA},
		StoreValue:       rcBytes,
	}
	return entry, nil
}

func ddlEventToCanalEntry(e *model.DDLEvent) (*canal.Entry, error) {
	eventType := convertDdlEventType(e)
	header := buildHeader(e.CommitTs, e.Schema, e.Table, eventType, -1)
	isDdl := isCanalDdl(eventType)
	rc := &canal.RowChange{
		EventTypePresent: &canal.RowChange_EventType{EventType: eventType},
		IsDdlPresent:     &canal.RowChange_IsDdl{IsDdl: isDdl},
		Sql:              e.Query,
		RowDatas:         nil,
		DdlSchemaName:    e.Schema,
	}
	rcBytes, err := proto.Marshal(rc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// build entry
	entry := &canal.Entry{
		Header:           header,
		EntryTypePresent: &canal.Entry_EntryType{EntryType: canal.EntryType_ROWDATA},
		StoreValue:       rcBytes,
	}
	return entry, nil
}

// CanalEventBatchEncoder encodes the events into the byte of a batch into.
type CanalEventBatchEncoder struct {
	messages *canal.Messages
	packet   *canal.Packet
}

// AppendResolvedEvent implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) AppendResolvedEvent(ts uint64) error {
	// For canal now, there is no such a corresponding type to ResolvedEvent so far.
	// Therefore the event is ignored.
	return nil
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) error {
	entry, err := rowEventToCanalEntry(e)
	if err != nil {
		return errors.Trace(err)
	}
	b, err := proto.Marshal(entry)
	if err != nil {
		return errors.Trace(err)
	}
	d.messages.Messages = append(d.messages.Messages, b)
	return nil
}

// AppendDDLEvent implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) AppendDDLEvent(e *model.DDLEvent) error {
	entry, err := ddlEventToCanalEntry(e)
	if err != nil {
		return errors.Trace(err)
	}
	b, err := proto.Marshal(entry)
	if err != nil {
		return errors.Trace(err)
	}
	d.messages.Messages = append(d.messages.Messages, b)
	return nil
}

// Build implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) Build() (key []byte, value []byte) {
	err := d.refreshPacketBody()
	if err != nil {
		panic(err)
	}
	value, err = proto.Marshal(d.packet)
	if err != nil {
		panic(err)
	}
	return nil, value
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

// refreshPacketBody() marshals the messages to the packet body
func (d *CanalEventBatchEncoder) refreshPacketBody() error {
	oldSize := len(d.packet.Body)
	newSize := proto.Size(d.messages)
	if newSize > oldSize {
		// resize packet body slice
		d.packet.Body = append(d.packet.Body, make([]byte, newSize-oldSize)...)
	}
	_, err := d.messages.MarshalToSizedBuffer(d.packet.Body[:newSize])
	return err
}

// NewCanalEventBatchEncoder creates a new CanalEventBatchEncoder.
func NewCanalEventBatchEncoder() EventBatchEncoder {
	p := &canal.Packet{
		VersionPresent: &canal.Packet_Version{
			Version: CanalPacketVersion,
		},
		Type: canal.PacketType_MESSAGES,
	}

	encoder := &CanalEventBatchEncoder{
		messages: &canal.Messages{},
		packet:   p,
	}
	return encoder
}
