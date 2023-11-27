// Copyright 2023 PingCAP, Inc.
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

package simple

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/charset"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"go.uber.org/zap"
)

const (
	defaultVersion = 1
)

// EventType describes the type of the event.
type EventType string

// The list of event types.
const (
	// WatermarkType is the type of the watermark event.
	WatermarkType EventType = "WATERMARK"
	// DDLType is the type of the DDL event.
	DDLType EventType = "DDL"
	// BootstrapType is the type of the bootstrap event.
	BootstrapType EventType = "BOOTSTRAP"
	// InsertType is the type of the insert event.
	InsertType EventType = "INSERT"
	// UpdateType is the type of the update event.
	UpdateType EventType = "UPDATE"
	// DeleteType is the type of the delete event.
	DeleteType EventType = "DELETE"
)

// columnSchema is the schema of the column.
type columnSchema struct {
	// ID is used to sort all column schema, should not be exposed to the outside.
	ID       int64       `json:"-"`
	Name     string      `json:"name"`
	DataType string      `json:"dataType"`
	Nullable bool        `json:"nullable"`
	Default  interface{} `json:"default"`
}

func newColumnSchema(col *timodel.ColumnInfo) *columnSchema {
	return &columnSchema{
		ID:       col.ID,
		Name:     col.Name.O,
		DataType: col.GetTypeDesc(),
		Nullable: mysql.HasNotNullFlag(col.GetFlag()),
		Default:  entry.GetDDLDefaultDefinition(col),
	}
}

func newTiColumnInfo(column *columnSchema, indexes []*IndexSchema) *timodel.ColumnInfo {
	col := new(timodel.ColumnInfo)
	col.Name = timodel.NewCIStr(column.Name)

	tp := utils.ExtractBasicMySQLType(column.DataType)
	col.FieldType = *types.NewFieldType(tp)
	if strings.Contains(column.DataType, "unsigned") {
		col.AddFlag(mysql.UnsignedFlag)
	}
	if strings.Contains(column.DataType, "zerofill") {
		col.AddFlag(mysql.ZerofillFlag)
	}

	if utils.IsBinaryMySQLType(column.DataType) {
		col.SetCharset(charset.CharsetBin)
		types.SetBinChsClnFlag(&col.FieldType)
	} else {
		col.SetCharset(charset.CharsetUTF8MB4)
	}

	for _, index := range indexes {
		if index.Primary {
			for _, name := range index.Columns {
				if name == column.Name {
					col.AddFlag(mysql.PriKeyFlag)
					break
				}
			}
			break
		}
	}

	if column.Nullable {
		col.AddFlag(mysql.NotNullFlag)
	}

	col.DefaultValue = column.Default

	return col
}

// IndexSchema is the schema of the index.
type IndexSchema struct {
	Name     string   `json:"name"`
	Unique   bool     `json:"unique"`
	Primary  bool     `json:"primary"`
	Nullable bool     `json:"nullable"`
	Columns  []string `json:"columns"`
}

// FromTiIndexInfo converts from TiDB IndexInfo to Index.
func newIndexSchema(index *timodel.IndexInfo, columns []*timodel.ColumnInfo) *IndexSchema {
	indexSchema := &IndexSchema{
		Name:    index.Name.O,
		Unique:  index.Unique,
		Primary: index.Primary,
	}

	for _, col := range index.Columns {
		indexSchema.Columns = append(indexSchema.Columns, col.Name.O)
		colInfo := columns[col.Offset]
		if mysql.HasNotNullFlag(colInfo.GetFlag()) {
			indexSchema.Nullable = false
			break
		}
	}
	return indexSchema
}

func newTiIndexInfo(indexSchema *IndexSchema) *timodel.IndexInfo {
	indexColumns := make([]*timodel.IndexColumn, len(indexSchema.Columns))
	for i, col := range indexSchema.Columns {
		indexColumns[i] = &timodel.IndexColumn{
			Name:   timodel.NewCIStr(col),
			Offset: i,
		}
	}

	return &timodel.IndexInfo{
		Name:    timodel.NewCIStr(indexSchema.Name),
		Columns: indexColumns,
		Unique:  indexSchema.Unique,
		Primary: indexSchema.Primary,
	}
}

// TableSchema is the schema of the table.
type TableSchema struct {
	Columns []*columnSchema `json:"columns"`
	Indexes []*IndexSchema  `json:"indexes"`
}

func newTableSchema(tableInfo *model.TableInfo) *TableSchema {
	columns := make([]*columnSchema, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columns = append(columns, newColumnSchema(col))
	}

	// sort by column by its id
	sort.SliceStable(columns, func(i, j int) bool {
		return int(columns[i].ID) < int(columns[j].ID)
	})

	indexes := make([]*IndexSchema, 0, len(tableInfo.Indices))
	for _, idx := range tableInfo.Indices {
		indexes = append(indexes, newIndexSchema(idx, tableInfo.TableInfo.Columns))
	}

	return &TableSchema{
		Columns: columns,
		Indexes: indexes,
	}
}

func newTableInfo(msg *message) *model.TableInfo {
	info := &model.TableInfo{
		TableName: model.TableName{
			Schema: msg.Database,
			Table:  msg.Table,
		},
		TableInfo: &timodel.TableInfo{
			Name:     timodel.NewCIStr(msg.Table),
			UpdateTS: msg.SchemaVersion,
		},
	}
	for _, col := range msg.TableSchema.Columns {
		tiCol := newTiColumnInfo(col, msg.TableSchema.Indexes)
		info.Columns = append(info.Columns, tiCol)
	}
	for _, idx := range msg.TableSchema.Indexes {
		index := newTiIndexInfo(idx)
		info.Indices = append(info.Indices, index)
	}

	return info
}

func newDDLEvent(msg *message) *model.DDLEvent {
	tableInfo := newTableInfo(msg)
	return &model.DDLEvent{
		StartTs:   msg.CommitTs,
		CommitTs:  msg.CommitTs,
		TableInfo: tableInfo,
		Query:     msg.SQL,
	}
}

func buildRowChangedEvent(msg *message, tableInfo *model.TableInfo) (*model.RowChangedEvent, error) {
	result := &model.RowChangedEvent{
		CommitTs: msg.CommitTs,
		Table: &model.TableName{
			Schema: msg.Database,
			Table:  msg.Table,
		},
		TableInfo: tableInfo,
	}

	fieldTypeMap := make(map[string]*types.FieldType, len(tableInfo.Columns))
	for _, columnInfo := range tableInfo.Columns {
		fieldTypeMap[columnInfo.Name.O] = &columnInfo.FieldType
	}

	columns, err := decodeColumns(msg.Data, fieldTypeMap)
	if err != nil {
		return nil, err
	}
	result.Columns = columns

	columns, err = decodeColumns(msg.Old, fieldTypeMap)
	if err != nil {
		return nil, err
	}
	result.PreColumns = columns

	result.WithHandlePrimaryFlag(tableInfo.GetPrimaryKeyColumnNames())

	return result, nil
}

func decodeColumns(rawData map[string]interface{}, fieldTypeMap map[string]*types.FieldType) ([]*model.Column, error) {
	result := make([]*model.Column, 0, len(rawData))
	for name, value := range rawData {
		fieldType, ok := fieldTypeMap[name]
		if !ok {
			log.Error("cannot found the fieldType for the column", zap.String("column", name))
			return nil, cerror.ErrDecodeFailed.GenWithStack("cannot found the fieldType for the column %s", name)
		}
		col, err := decodeColumn(name, value, fieldType)
		if err != nil {
			return nil, err
		}
		result = append(result, col)
	}
	return result, nil
}

type message struct {
	Version int `json:"version"`
	// Scheme and Table is empty for the resolved ts event.
	Database string    `json:"database,omitempty"`
	Table    string    `json:"table,omitempty"`
	Type     EventType `json:"type"`
	CommitTs uint64    `json:"commitTs"`
	// Data is available for the Insert and Update event.
	Data map[string]interface{} `json:"data,omitempty"`
	// Old is available for the Update and Delete event.
	Old map[string]interface{} `json:"old,omitempty"`
	// TableSchema is for the DDL and Bootstrap event.
	TableSchema *TableSchema `json:"tableSchema,omitempty"`
	// SQL is only for the DDL event.
	SQL string `json:"sql,omitempty"`
	// SchemaVersion is for the DDL, Bootstrap and DML event.
	SchemaVersion uint64 `json:"schemaVersion,omitempty"`
}

func newResolvedMessage(ts uint64) *message {
	return &message{
		Version:  defaultVersion,
		Type:     WatermarkType,
		CommitTs: ts,
	}
}

func newDDLMessage(ddl *model.DDLEvent) *message {
	tableSchema := newTableSchema(ddl.TableInfo)
	msg := &message{
		Version:       defaultVersion,
		Database:      ddl.TableInfo.TableName.Schema,
		Table:         ddl.TableInfo.TableName.Table,
		Type:          DDLType,
		CommitTs:      ddl.CommitTs,
		SQL:           ddl.Query,
		TableSchema:   tableSchema,
		SchemaVersion: ddl.TableInfo.UpdateTS,
	}
	if ddl.IsBootstrap {
		msg.Type = BootstrapType
		msg.SQL = ""
	}

	return msg
}

func newDMLMessage(event *model.RowChangedEvent) (*message, error) {
	m := &message{
		Version:       defaultVersion,
		Database:      event.Table.Schema,
		Table:         event.Table.Table,
		CommitTs:      event.CommitTs,
		SchemaVersion: event.TableInfo.UpdateTS,
	}
	var err error
	if event.IsInsert() {
		m.Type = InsertType
		m.Data, err = formatColumns(event.Columns, event.ColInfos)
		if err != nil {
			return nil, err
		}
	} else if event.IsDelete() {
		m.Type = DeleteType
		m.Old, err = formatColumns(event.PreColumns, event.ColInfos)
		if err != nil {
			return nil, err
		}
	} else if event.IsUpdate() {
		m.Type = UpdateType
		m.Data, err = formatColumns(event.Columns, event.ColInfos)
		if err != nil {
			return nil, err
		}
		m.Old, err = formatColumns(event.PreColumns, event.ColInfos)
		if err != nil {
			return nil, err
		}
	} else {
		log.Panic("invalid event type, this should not hit", zap.Any("event", event))
	}

	return m, nil
}

func formatColumns(
	columns []*model.Column, columnInfos []rowcodec.ColInfo,
) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(columns))
	for idx, col := range columns {
		value, err := encodeValue(col.Value, columnInfos[idx].Ft)
		if err != nil {
			return nil, err
		}
		result[col.Name] = value
	}
	return result, nil
}

func encodeValue(value interface{}, ft *types.FieldType) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch ft.GetType() {
	case mysql.TypeEnum:
		if v, ok := value.(string); ok {
			return v, nil
		}
		element := ft.GetElems()
		number := value.(uint64)
		enumVar, err := types.ParseEnumValue(element, number)
		if err != nil {
			return "", cerror.WrapError(cerror.ErrEncodeFailed, err)
		}
		return enumVar.Name, nil
	case mysql.TypeSet:
		if v, ok := value.(string); ok {
			return v, nil
		}
		elements := ft.GetElems()
		number := value.(uint64)
		setVar, err := types.ParseSetValue(elements, number)
		if err != nil {
			return "", cerror.WrapError(cerror.ErrEncodeFailed, err)
		}
		return setVar.Name, nil
	default:
	}

	var result string
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
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			result = base64.StdEncoding.EncodeToString(v)
		} else {
			result = string(v)
		}
	default:
		result = fmt.Sprintf("%v", v)
	}

	return result, nil
}

func decodeColumn(name string, value interface{}, fieldType *types.FieldType) (*model.Column, error) {
	result := &model.Column{
		Type:  fieldType.GetType(),
		Name:  name,
		Value: value,
	}
	if value == nil {
		return result, nil
	}

	data, ok := value.(string)
	if !ok {
		log.Panic("simple encode message should have type in `string`")
	}

	if mysql.HasBinaryFlag(fieldType.GetFlag()) {
		v, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
		}
		result.Value = v
	}

	return result, nil
}
