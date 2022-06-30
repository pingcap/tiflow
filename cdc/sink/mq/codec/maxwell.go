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
	"context"
	"encoding/binary"
	"encoding/json"

	"github.com/pingcap/errors"
	model2 "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/tikv/pd/pkg/tsoutil"
)

type maxwellEventBatchEncoderBuilder struct{}

func newMaxwellEventBatchEncoderBuilder() EncoderBuilder {
	return &maxwellEventBatchEncoderBuilder{}
}

// Build a `MaxwellEventBatchEncoder`
func (b *maxwellEventBatchEncoderBuilder) Build() EventBatchEncoder {
	return NewMaxwellEventBatchEncoder()
}

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
	Data     map[string]interface{} `json:"data,omitempty"`
	Old      map[string]interface{} `json:"old,omitempty"`
}

// Encode encodes the message to bytes
func (m *maxwellMessage) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMaxwellEncodeFailed, err)
}

// Encode encodes the message to bytes
func (m *DdlMaxwellMessage) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMaxwellEncodeFailed, err)
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	// For maxwell now, there is no such a corresponding type to ResolvedEvent so far.
	// Therefore the event is ignored.
	return nil, nil
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
		Type:      model.MessageTypeRow,
	}
	value := &maxwellMessage{
		Ts:       0,
		Database: e.Table.Schema,
		Table:    e.Table.Table,
		Data:     make(map[string]interface{}),
		Old:      make(map[string]interface{}),
	}

	physicalTime, _ := tsoutil.ParseTS(e.CommitTs)
	value.Ts = physicalTime.Unix()
	if e.IsDelete() {
		value.Type = "delete"
		for _, v := range e.PreColumns {
			switch v.Type {
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
				if v.Value == nil {
					value.Old[v.Name] = nil
				} else if v.Flag.IsBinary() {
					value.Old[v.Name] = v.Value
				} else {
					value.Old[v.Name] = string(v.Value.([]byte))
				}
			default:
				value.Old[v.Name] = v.Value
			}
		}
	} else {
		for _, v := range e.Columns {
			switch v.Type {
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
				if v.Value == nil {
					value.Data[v.Name] = nil
				} else if v.Flag.IsBinary() {
					value.Data[v.Name] = v.Value
				} else {
					value.Data[v.Name] = string(v.Value.([]byte))
				}
			default:
				value.Data[v.Name] = v.Value
			}
		}
		if e.PreColumns == nil {
			value.Type = "insert"
		} else {
			value.Type = "update"
			for _, v := range e.PreColumns {
				switch v.Type {
				case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
					if v.Value == nil {
						if value.Data[v.Name] != nil {
							value.Old[v.Name] = nil
						}
					} else if v.Flag.IsBinary() {
						if value.Data[v.Name] != v.Value {
							value.Old[v.Name] = v.Value
						}
					} else {
						if value.Data[v.Name] != string(v.Value.([]byte)) {
							value.Old[v.Name] = string(v.Value.([]byte))
						}
					}
				default:
					if value.Data[v.Name] != v.Value {
						value.Old[v.Name] = v.Value
					}
				}
			}

		}
	}
	return key, value
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) AppendRowChangedEvent(
	ctx context.Context,
	topic string,
	e *model.RowChangedEvent,
) error {
	_, valueMsg := rowEventToMaxwellMessage(e)
	value, err := valueMsg.Encode()
	if err != nil {
		return errors.Trace(err)
	}
	d.valueBuf.Write(value)
	d.batchSize++
	return nil
}

// Column represents a column in maxwell
type Column struct {
	Type string `json:"type"`
	Name string `json:"name"`
	// Do not mark the unique key temporarily
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
	// Do not output whether it is a primary key temporarily
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
		Type:   model.MessageTypeDDL,
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

	return newDDLMQMessage(config.ProtocolMaxwell, key, value, e), nil
}

// Build implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) Build() []*MQMessage {
	if d.batchSize == 0 {
		return nil
	}

	ret := NewMQMessage(config.ProtocolMaxwell, d.keyBuf.Bytes(), d.valueBuf.Bytes(), 0, model.MessageTypeRow, nil, nil)
	ret.SetRowsCount(d.batchSize)
	d.Reset()
	return []*MQMessage{ret}
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

// NewMaxwellEventBatchEncoder creates a new MaxwellEventBatchEncoder.
func NewMaxwellEventBatchEncoder() EventBatchEncoder {
	batch := &MaxwellEventBatchEncoder{
		keyBuf:   &bytes.Buffer{},
		valueBuf: &bytes.Buffer{},
	}
	batch.Reset()
	return batch
}

// ddl typecode from parser/model/ddl.go
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

// Convert column type code to maxwell column type
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
