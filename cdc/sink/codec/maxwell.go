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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strconv"

	"github.com/pingcap/errors"
	model2 "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/tikv/pd/pkg/tsoutil"
)

// MaxwellEventBatchEncoder is a maxwell format encoder implementation
type MaxwellEventBatchEncoder struct {
	keyBuf    *bytes.Buffer
	valueBuf  *bytes.Buffer
	batchSize int
}

type maxwellMessage struct {
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Type     string                 `json:"type"`
	Ts       int64                  `json:"ts"`
	Xid      int                    `json:"xid,omitempty"`
	Xoffset  int                    `json:"xoffset,omitempty"`
	Position string                 `json:"position,omitempty"`
	Gtid     string                 `json:"gtid,omitempty"`
	Data     map[string]interface{} `json:"data"`
	Old      map[string]interface{} `json:"old,omitempty"`
}

// Encode encodes the message to bytes
func (m *maxwellMessage) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMaxwellEncodeFailed, err)
}

// Decode decodes the message from bytes
func (m *maxwellMessage) Decode(data []byte) error {
	return cerror.WrapError(cerror.ErrMaxwellDecodeFailed, json.Unmarshal(data, m))
}

// Encode encodes the message to bytes
func (m *DdlMaxwellMessage) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMaxwellEncodeFailed, err)
}

// Decode the message from bytes
func (m *DdlMaxwellMessage) Decode(data []byte) error {
	return cerror.WrapError(cerror.ErrMaxwellDecodeFailed, json.Unmarshal(data, m))
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	// For maxwell now, there is no such a corresponding type to ResolvedEvent so far.
	// Therefore the event is ignored.
	return nil, nil
}

// AppendResolvedEvent implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) AppendResolvedEvent(ts uint64) (EncoderResult, error) {
	return EncoderNoOperation, nil
}

func rowEventToMaxwellMessage(e *model.RowChangedEvent) (*mqMessageKey, *maxwellMessage) {
	var partition *int64
	if e.Table.IsPartition {
		partition = &e.Table.TableID
	}
	key := &mqMessageKey{
		Ts:        e.CommitTs,
		Schema:    e.Table.Schema,
		Table:     e.Table.Table,
		Partition: partition,
		Type:      model.MqMessageTypeRow,
	}
	value := &maxwellMessage{
		Ts:       000000000,
		Database: e.Table.Schema,
		Table:    e.Table.Table,
		Data:     make(map[string]interface{}),
		Old:      make(map[string]interface{}),
	}

	physicalTime, _ := tsoutil.ParseTS(e.CommitTs)
	value.Ts = physicalTime.Unix()
	if e.PreColumns == nil {
		value.Type = "insert"
		for _, v := range e.Columns {
			switch v.Type {
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
				if v.Value != nil {
					str := string(v.Value.([]byte))
					if v.Flag.IsBinary() {
						str = strconv.Quote(str)
						str = str[1 : len(str)-1]
					}
					value.Data[v.Name] = str
				} else {
					value.Data[v.Name] = nil
				}
			default:
				value.Data[v.Name] = v.Value
			}
		}
	} else if e.IsDelete() {
		value.Type = "delete"
		for _, v := range e.PreColumns {
			switch v.Type {
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
				if v.Value != nil {
					str := string(v.Value.([]byte))
					if v.Flag.IsBinary() {
						str = strconv.Quote(str)
						str = str[1 : len(str)-1]
					}
					value.Old[v.Name] = str
				} else {
					value.Old[v.Name] = nil
				}
			default:
				value.Old[v.Name] = v.Value
			}
		}
	} else {
		value.Type = "update"
		for _, v := range e.Columns {
			switch v.Type {
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
				if v.Value != nil {
					str := string(v.Value.([]byte))
					if v.Flag.IsBinary() {
						str = strconv.Quote(str)
						str = str[1 : len(str)-1]
					}
					value.Data[v.Name] = str
				} else {
					value.Data[v.Name] = nil
				}
			default:
				value.Data[v.Name] = v.Value
			}
		}
		for _, v := range e.PreColumns {
			switch v.Type {
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
				if v.Value != nil {
					str := string(v.Value.([]byte))
					if v.Flag.IsBinary() {
						str = strconv.Quote(str)
						str = str[1 : len(str)-1]
					}
					if value.Data[v.Name] != str {
						value.Old[v.Name] = str
					}
				} else if value.Data[v.Name] != nil && v.Value == nil {
					value.Old[v.Name] = nil
				}
			default:
				if value.Data[v.Name] != v.Value {
					value.Old[v.Name] = v.Value
				}
			}
		}
	}
	return key, value
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error) {
	keyMsg, valueMsg := rowEventToMaxwellMessage(e)
	key, err := keyMsg.Encode()
	if err != nil {
		return EncoderNoOperation, errors.Trace(err)
	}
	value, err := valueMsg.Encode()
	if err != nil {
		return EncoderNoOperation, errors.Trace(err)
	}
	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	d.valueBuf.Write(value)

	d.batchSize++
	return EncoderNoOperation, nil
}

// SetParams is no-op for Maxwell for now
func (d *MaxwellEventBatchEncoder) SetParams(params map[string]string) error {
	return nil
}

// Column represents a column in maxwell
type Column struct {
	Type string `json:"type"`
	Name string `json:"name"`
	//Do not mark the unique key temporarily
	Signed       bool   `json:"signed,omitempty"`
	ColumnLength int    `json:"column-length,omitempty"`
	Charset      string `json:"charset,omitempty"`
}

// TableStruct represents a table structure includes some table info
type TableStruct struct {
	Database string    `json:"database"`
	Charset  string    `json:"charset,omitempty"`
	Table    string    `json:"table"`
	Columns  []*Column `json:"columns"`
	//Do not output whether it is a primary key temporarily
	PrimaryKey []string `json:"primary-key"`
}

// DdlMaxwellMessage represents a DDL maxwell message
// Old for table old schema
// Def for table after ddl schema
type DdlMaxwellMessage struct {
	Type     string      `json:"type"`
	Database string      `json:"database"`
	Table    string      `json:"table"`
	Old      TableStruct `json:"old,omitempty"`
	Def      TableStruct `json:"def,omitempty"`
	Ts       uint64      `json:"ts"`
	SQL      string      `json:"sql"`
	Position string      `json:"position,omitempty"`
}

func ddlEventtoMaxwellMessage(e *model.DDLEvent) (*mqMessageKey, *DdlMaxwellMessage) {
	key := &mqMessageKey{
		Ts:     e.CommitTs,
		Schema: e.TableInfo.Schema,
		Table:  e.TableInfo.Table,
		Type:   model.MqMessageTypeDDL,
	}
	value := &DdlMaxwellMessage{
		Ts:       e.CommitTs,
		Database: e.TableInfo.Schema,
		Type:     "table-create",
		Table:    e.TableInfo.Table,
		Old:      TableStruct{},
		Def:      TableStruct{},
		SQL:      e.Query,
	}

	value.Type = ddlToMaxwellType(e.Type)

	if e.PreTableInfo != nil {
		value.Old.Database = e.PreTableInfo.Schema
		value.Old.Table = e.PreTableInfo.Table
		for _, v := range e.PreTableInfo.ColumnInfo {
			maxwellcolumntype, _ := columnToMaxwellType(v.Type)
			value.Old.Columns = append(value.Old.Columns, &Column{
				Name: v.Name,
				Type: maxwellcolumntype,
			})
		}
	}

	value.Def.Database = e.TableInfo.Schema
	value.Def.Table = e.TableInfo.Table
	for _, v := range e.TableInfo.ColumnInfo {
		maxwellcolumntype, err := columnToMaxwellType(v.Type)
		if err != nil {
			value.Old.Columns = append(value.Old.Columns, &Column{
				Name: v.Name,
				Type: err.Error(),
			})

		}
		value.Def.Columns = append(value.Def.Columns, &Column{
			Name: v.Name,
			Type: maxwellcolumntype,
		})
	}
	return key, value
}

// EncodeDDLEvent implements the EventBatchEncoder interface
// DDL message unresolved tso
func (d *MaxwellEventBatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*MQMessage, error) {
	keyMsg, valueMsg := ddlEventtoMaxwellMessage(e)
	key, err := keyMsg.Encode()
	if err != nil {
		return nil, errors.Trace(err)
	}
	value, err := valueMsg.Encode()
	if err != nil {
		return nil, errors.Trace(err)
	}

	keyBuf := new(bytes.Buffer)
	keyBuf.Write(key)
	valueBuf := new(bytes.Buffer)
	valueBuf.Write(value)
	return NewMQMessage(keyBuf.Bytes(), valueBuf.Bytes(), e.CommitTs), nil
}

// Build implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) Build() []*MQMessage {
	if d.batchSize == 0 {
		return nil
	}

	ret := NewMQMessage(d.keyBuf.Bytes(), d.valueBuf.Bytes(), 0)
	d.Reset()
	return []*MQMessage{ret}
}

// MixedBuild implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) MixedBuild(withVersion bool) []byte {
	return nil
}

// Reset implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) Reset() {
	d.keyBuf.Reset()
	d.valueBuf.Reset()
	d.batchSize = 0
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	d.keyBuf.Write(versionByte[:])
}

// Size implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) Size() int {
	return d.keyBuf.Len() + d.valueBuf.Len()
}

// NewMaxwellEventBatchEncoder creates a new MaxwellEventBatchEncoder.
func NewMaxwellEventBatchEncoder() EventBatchEncoder {
	batch := &MaxwellEventBatchEncoder{
		keyBuf:   &bytes.Buffer{},
		valueBuf: &bytes.Buffer{},
	}
	batch.Reset()
	return batch
}

func (b *MaxwellEventBatchDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	msgKey := new(mqMessageKey)
	err := msgKey.Decode(key)
	if err != nil {
		return errors.Trace(err)
	}
	b.nextKey = msgKey
	b.nextKeyLen = keyLen
	return nil
}

func (b *MaxwellEventBatchDecoder) hasNext() bool {
	return false
}

// MaxwellEventBatchDecoder decodes the byte of a batch into the original messages.
type MaxwellEventBatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte
	nextKey    *mqMessageKey
	nextKeyLen uint64
}

// HasNext implements the EventBatchDecoder interface
func (b *MaxwellEventBatchDecoder) HasNext() (model.MqMessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

// decode
func maxwellMessageToRowEvent(key *mqMessageKey, value *maxwellMessage) *model.RowChangedEvent {
	e := new(model.RowChangedEvent)
	e.Table = &model.TableName{
		Schema: key.Schema,
		Table:  key.Table,
	}
	if key.Partition != nil {
		e.Table.TableID = *key.Partition
		e.Table.IsPartition = true
	}
	if value.Old == nil {
		for k, v := range value.Data {
			for _, n := range e.Columns {
				n.Name = k
				n.Value = v
				n.Type = 3

			}
		}
	}

	return e
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *MaxwellEventBatchDecoder) NextResolvedEvent() (uint64, error) {
	// For maxwell now, there is no such a corresponding type to ResolvedEvent so far.
	// Therefore the event is ignored.
	return 0, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface
func (b *MaxwellEventBatchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.keyBytes = b.keyBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeRow {
		return nil, cerror.ErrMaxwellInvalidData.GenWithStack("row event message not found")
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]
	b.valueBytes = b.valueBytes[valueLen+8:]
	rowMsg := new(maxwellMessage)
	if err := rowMsg.Decode(value); err != nil {
		return nil, errors.Trace(err)
	}

	rowEvent := maxwellMessageToRowEvent(b.nextKey, rowMsg)
	b.nextKey = nil
	return rowEvent, nil
}

// NextDDLEvent implements the EventBatchDecoder interface
func (b *MaxwellEventBatchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.keyBytes = b.keyBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeDDL {
		return nil, cerror.ErrMaxwellInvalidData.GenWithStack("ddl event message not found")
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]
	b.valueBytes = b.valueBytes[valueLen+8:]
	ddlMsg := new(mqMessageDDL)
	if err := ddlMsg.Decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	ddlEvent := mqMessageToDDLEvent(b.nextKey, ddlMsg)
	b.nextKey = nil
	return ddlEvent, nil
}

// NewMaxwellEventBatchDecoder creates a new JSONEventBatchDecoder.
func NewMaxwellEventBatchDecoder(key []byte, value []byte) (EventBatchDecoder, error) {
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != BatchVersion1 {
		return nil, cerror.ErrMaxwellInvalidData.GenWithStack("unexpected key format version")
	}
	return &MaxwellEventBatchDecoder{
		keyBytes:   key,
		valueBytes: value,
	}, nil
}

//ddl typecode from parser/model/ddl.go
func ddlToMaxwellType(ddlType model2.ActionType) string {
	if ddlType >= model2.ActionAddColumn && ddlType <= model2.ActionDropTablePartition {
		return "table-alter"
	}
	switch ddlType {
	case model2.ActionCreateTable:
		return "table-create"
	case model2.ActionDropTable:
		return "table-drop"
	case 22, 23, 27, 28, 29, 33, 37, 38, 41, 42:
		return "table-alter"
	case model2.ActionCreateSchema:
		return "database-create"
	case model2.ActionDropSchema:
		return "database-drop"
	case model2.ActionModifySchemaCharsetAndCollate:
		return "database-alter"
	default:
		return ddlType.String()
	}
}

//Convert column type code to maxwell column type
func columnToMaxwellType(columnType byte) (string, error) {
	switch columnType {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeInt24:
		return "int", nil
	case mysql.TypeLonglong:
		return "bigint", nil
	case mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeString, mysql.TypeVarchar:
		return "string", nil
	case mysql.TypeDate:
		return "date", nil
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		return "datetime", nil
	case mysql.TypeDuration:
		return "time", nil
	case mysql.TypeYear:
		return "year", nil
	case mysql.TypeEnum:
		return "enum", nil
	case mysql.TypeSet:
		return "set", nil
	case mysql.TypeBit:
		return "bit", nil
	case mysql.TypeJSON:
		return "json", nil
	case mysql.TypeFloat, mysql.TypeDouble:
		return "float", nil
	case mysql.TypeNewDecimal:
		return "decimal", nil
	default:
		return "", cerror.ErrMaxwellInvalidData.GenWithStack("unsupported column type - %v", columnType)
	}
}
