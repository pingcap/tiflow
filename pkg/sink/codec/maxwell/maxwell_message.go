// Copyright 2022 PingCAP, Inc.
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

package maxwell

import (
	"encoding/json"

	model2 "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"
	"github.com/tikv/client-go/v2/oracle"
)

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
func (m *maxwellMessage) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMaxwellEncodeFailed, err)
}

func rowChangeToMaxwellMsg(e *model.RowChangedEvent, onlyHandleKeyColumns bool) (*internal.MessageKey, *maxwellMessage) {
	var partition *int64
	if e.TableInfo.IsPartitionTable() {
		tableID := e.GetTableID()
		partition = &tableID
	}
	key := &internal.MessageKey{
		Ts:        e.CommitTs,
		Schema:    e.TableInfo.GetSchemaName(),
		Table:     e.TableInfo.GetTableName(),
		Partition: partition,
		Type:      model.MessageTypeRow,
	}
	value := &maxwellMessage{
		Ts:       0,
		Database: e.TableInfo.GetSchemaName(),
		Table:    e.TableInfo.GetTableName(),
		Data:     make(map[string]interface{}),
		Old:      make(map[string]interface{}),
	}
	physicalTime := oracle.GetTimeFromTS(e.CommitTs)
	value.Ts = physicalTime.Unix()
	tableInfo := e.TableInfo
	if e.IsDelete() {
		value.Type = "delete"
		for _, v := range e.PreColumns {
			colFlag := tableInfo.ForceGetColumnFlagType(v.ColumnID)
			if onlyHandleKeyColumns && !colFlag.IsHandleKey() {
				continue
			}
			colInfo := tableInfo.ForceGetColumnInfo(v.ColumnID)
			colName := tableInfo.ForceGetColumnName(v.ColumnID)
			switch colInfo.GetType() {
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
				if v.Value == nil {
					value.Old[colName] = nil
				} else if colFlag.IsBinary() {
					value.Old[colName] = v.Value
				} else {
					value.Old[colName] = string(v.Value.([]byte))
				}
			case mysql.TypeTiDBVectorFloat32:
				value.Old[v.Name] = v.Value.(types.VectorFloat32).String()
			default:
				value.Old[colName] = v.Value
			}
		}
	} else {
		for _, v := range e.Columns {
			colFlag := tableInfo.ForceGetColumnFlagType(v.ColumnID)
			colInfo := tableInfo.ForceGetColumnInfo(v.ColumnID)
			colName := tableInfo.ForceGetColumnName(v.ColumnID)
			switch colInfo.GetType() {
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
				if v.Value == nil {
					value.Data[colName] = nil
				} else if colFlag.IsBinary() {
					value.Data[colName] = v.Value
				} else {
					value.Data[colName] = string(v.Value.([]byte))
				}
			case mysql.TypeTiDBVectorFloat32:
				value.Data[v.Name] = v.Value.(types.VectorFloat32).String()
			default:
				value.Data[colName] = v.Value
			}
		}
		if e.PreColumns == nil {
			value.Type = "insert"
		} else {
			value.Type = "update"
			for _, v := range e.PreColumns {
				colFlag := tableInfo.ForceGetColumnFlagType(v.ColumnID)
				colInfo := tableInfo.ForceGetColumnInfo(v.ColumnID)
				colName := tableInfo.ForceGetColumnName(v.ColumnID)
				switch colInfo.GetType() {
				case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
					if v.Value == nil {
						if value.Data[colName] != nil {
							value.Old[colName] = nil
						}
					} else if colFlag.IsBinary() {
						if value.Data[colName] != v.Value {
							value.Old[colName] = v.Value
						}
					} else {
						if value.Data[colName] != string(v.Value.([]byte)) {
							value.Old[colName] = string(v.Value.([]byte))
						}
					}
				case mysql.TypeTiDBVectorFloat32:
					val := v.Value.(types.VectorFloat32).String()
					if value.Old[v.Name] != val {
						value.Old[v.Name] = val
					}
				default:
					if value.Data[colName] != v.Value {
						value.Old[colName] = v.Value
					}
				}
			}

		}
	}
	return key, value
}

// maxwellColumn represents a column in maxwell
type maxwellColumn struct {
	Type string `json:"type"`
	Name string `json:"name"`
	// Do not mark the unique key temporarily
	Signed       bool   `json:"signed,omitempty"`
	ColumnLength int    `json:"column-length,omitempty"`
	Charset      string `json:"charset,omitempty"`
}

// tableStruct represents a table structure includes some table info
type tableStruct struct {
	Database string           `json:"database"`
	Charset  string           `json:"charset,omitempty"`
	Table    string           `json:"table"`
	Columns  []*maxwellColumn `json:"columns"`
	// Do not output whether it is a primary key temporarily
	PrimaryKey []string `json:"primary-key"`
}

// ddlMaxwellMessage represents a DDL maxwell message
// Old for table old schema
// Def for table after ddl schema
type ddlMaxwellMessage struct {
	Type     string      `json:"type"`
	Database string      `json:"database"`
	Table    string      `json:"table"`
	Old      tableStruct `json:"old,omitempty"`
	Def      tableStruct `json:"def,omitempty"`
	Ts       uint64      `json:"ts"`
	SQL      string      `json:"sql"`
	Position string      `json:"position,omitempty"`
}

// Encode encodes the message to bytes
func (m *ddlMaxwellMessage) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMaxwellEncodeFailed, err)
}

func ddlEventToMaxwellMsg(e *model.DDLEvent) (*internal.MessageKey, *ddlMaxwellMessage) {
	key := &internal.MessageKey{
		Ts:     e.CommitTs,
		Schema: e.TableInfo.TableName.Schema,
		Table:  e.TableInfo.TableName.Table,
		Type:   model.MessageTypeDDL,
	}
	value := &ddlMaxwellMessage{
		Ts:       e.CommitTs,
		Database: e.TableInfo.TableName.Schema,
		Type:     "table-create",
		Table:    e.TableInfo.TableName.Table,
		Old:      tableStruct{},
		Def:      tableStruct{},
		SQL:      e.Query,
	}

	value.Type = ddlToMaxwellType(e.Type)

	if e.PreTableInfo != nil {
		value.Old.Database = e.PreTableInfo.TableName.Schema
		value.Old.Table = e.PreTableInfo.TableName.Table
		for _, v := range e.PreTableInfo.TableInfo.Columns {
			maxwellcolumntype, _ := columnToMaxwellType(v.FieldType.GetType())
			value.Old.Columns = append(value.Old.Columns, &maxwellColumn{
				Name: v.Name.O,
				Type: maxwellcolumntype,
			})
		}
	}

	value.Def.Database = e.TableInfo.TableName.Schema
	value.Def.Table = e.TableInfo.TableName.Table
	for _, v := range e.TableInfo.TableInfo.Columns {
		maxwellcolumntype, err := columnToMaxwellType(v.FieldType.GetType())
		if err != nil {
			value.Old.Columns = append(value.Old.Columns, &maxwellColumn{
				Name: v.Name.O,
				Type: err.Error(),
			})
		}
		value.Def.Columns = append(value.Def.Columns, &maxwellColumn{
			Name: v.Name.O,
			Type: maxwellcolumntype,
		})
	}
	return key, value
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
	case mysql.TypeTiDBVectorFloat32:
		return "string", nil
	default:
		return "", cerror.ErrMaxwellInvalidData.GenWithStack("unsupported column type - %v", columnType)
	}
}
