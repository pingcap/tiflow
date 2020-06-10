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

//compatible with canal-1.1.4

package codec

import (
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	mm "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"

	canal "github.com/pingcap/ticdc/proto/canal"
	"strconv"
)

const (
	CanalPacketVersion   int32  = 1
	CanalProtocolVersion int32  = 1
	CanalServerEncode    string = "UTF-8"
)

func convertToCanalTs(commitTs uint64) int64 {
	return int64(commitTs >> 18)
}

func ddlTypeToEventType(t mm.ActionType) canal.EventType {
	switch t {
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

func createHeader(commitTs uint64, schema string, table string, eventType canal.EventType, rowCount int) *canal.Header {
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

func rowEventToCanalEntry(e *model.RowChangedEvent) (*canal.Entry, error) {
	panic("implement me")
}

func ddlEventToCanalEntry(e *model.DDLEvent) (*canal.Entry, error) {
	eventType := ddlTypeToEventType(e.Type)
	header := createHeader(e.CommitTs, e.Schema, e.Table, eventType, -1)
	isDdl := true
	switch eventType {
	case canal.EventType_QUERY, canal.EventType_INSERT, canal.EventType_UPDATE, canal.EventType_DELETE:
		// see https://github.com/alibaba/canal/blob/d53bfd7ee76f8fe6eb581049d64b07d4fcdd692d/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L278
		isDdl = false
	}
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
