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
	"time"

	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
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
	DataType dataType    `json:"dataType"`
	Nullable bool        `json:"nullable"`
	Default  interface{} `json:"default"`
}

type dataType struct {
	// MySQLType represent the basic mysql type
	MySQLType string `json:"mysqlType"`

	Charset string `json:"charset"`
	Collate string `json:"collate"`

	// length represent size of bytes of the field
	Length int `json:"length,omitempty"`
	// Decimal represent decimal length of the field
	Decimal int `json:"decimal,omitempty"`
	// Elements represent the element list for enum and set type.
	Elements []string `json:"elements,omitempty"`

	Unsigned bool `json:"unsigned,omitempty"`
	Zerofill bool `json:"zerofill,omitempty"`
}

// newColumnSchema converts from TiDB ColumnInfo to columnSchema.
func newColumnSchema(col *timodel.ColumnInfo) *columnSchema {
	tp := dataType{
		MySQLType: types.TypeToStr(col.GetType(), col.GetCharset()),
		Charset:   col.GetCharset(),
		Collate:   col.GetCollate(),
		Length:    col.GetFlen(),
		Decimal:   col.GetDecimal(),
		Elements:  col.GetElems(),
		Unsigned:  mysql.HasUnsignedFlag(col.GetFlag()),
		Zerofill:  mysql.HasZerofillFlag(col.GetFlag()),
	}
	return &columnSchema{
		ID:       col.ID,
		Name:     col.Name.O,
		DataType: tp,
		Nullable: !mysql.HasNotNullFlag(col.GetFlag()),
		Default:  entry.GetColumnDefaultValue(col),
	}
}

// newTiColumnInfo uses columnSchema and IndexSchema to construct a tidb column info.
func newTiColumnInfo(column *columnSchema, indexes []*IndexSchema) *timodel.ColumnInfo {
	col := new(timodel.ColumnInfo)
	col.Name = timodel.NewCIStr(column.Name)

	col.FieldType = *types.NewFieldType(types.StrToType(column.DataType.MySQLType))
	col.SetCharset(column.DataType.Charset)
	col.SetCollate(column.DataType.Collate)
	if column.DataType.Unsigned {
		col.AddFlag(mysql.UnsignedFlag)
	}
	if column.DataType.Zerofill {
		col.AddFlag(mysql.ZerofillFlag)
	}
	col.SetFlen(column.DataType.Length)
	col.SetDecimal(column.DataType.Decimal)
	col.SetElems(column.DataType.Elements)

	if utils.IsBinaryMySQLType(column.DataType.MySQLType) {
		col.AddFlag(mysql.BinaryFlag)
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

// newIndexSchema converts from TiDB IndexInfo to IndexSchema.
func newIndexSchema(index *timodel.IndexInfo, columns []*timodel.ColumnInfo) *IndexSchema {
	indexSchema := &IndexSchema{
		Name:    index.Name.O,
		Unique:  index.Unique,
		Primary: index.Primary,
	}
	for _, col := range index.Columns {
		indexSchema.Columns = append(indexSchema.Columns, col.Name.O)
		colInfo := columns[col.Offset]
		// An index is not null when all columns of aer not null
		if !mysql.HasNotNullFlag(colInfo.GetFlag()) {
			indexSchema.Nullable = true
		}
	}
	return indexSchema
}

// newTiIndexInfo convert IndexSchema to a tidb index info.
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
	Database string          `json:"database"`
	Table    string          `json:"table"`
	Version  uint64          `json:"version"`
	Columns  []*columnSchema `json:"columns"`
	Indexes  []*IndexSchema  `json:"indexes"`
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

	pkInIndexes := false
	indexes := make([]*IndexSchema, 0, len(tableInfo.Indices))
	for _, idx := range tableInfo.Indices {
		index := newIndexSchema(idx, tableInfo.Columns)
		if index.Primary {
			pkInIndexes = true
		}
		indexes = append(indexes, index)
	}

	// sometimes the primary key is not in the index, we need to find it manually.
	if !pkInIndexes {
		pkColumns := tableInfo.GetPrimaryKeyColumnNames()
		if len(pkColumns) != 0 {
			index := &IndexSchema{
				Name:     "primary",
				Nullable: false,
				Primary:  true,
				Unique:   true,
			}
			for col := range pkColumns {
				index.Columns = append(index.Columns, col)
			}
			indexes = append(indexes, index)
		}
	}

	return &TableSchema{
		Database: tableInfo.TableName.Schema,
		Table:    tableInfo.TableName.Table,
		Version:  tableInfo.UpdateTS,
		Columns:  columns,
		Indexes:  indexes,
	}
}

// newTableInfo converts from TableSchema to TableInfo.
func newTableInfo(m *TableSchema) *model.TableInfo {
	var (
		database      string
		table         string
		schemaVersion uint64
	)
	if m != nil {
		database = m.Database
		table = m.Table
		schemaVersion = m.Version
	}
	info := &model.TableInfo{
		TableName: model.TableName{
			Schema: database,
			Table:  table,
		},
		TableInfo: &timodel.TableInfo{
			Name:     timodel.NewCIStr(table),
			UpdateTS: schemaVersion,
		},
	}

	if m == nil {
		return info
	}

	for _, col := range m.Columns {
		tiCol := newTiColumnInfo(col, m.Indexes)
		info.Columns = append(info.Columns, tiCol)
	}
	for _, idx := range m.Indexes {
		index := newTiIndexInfo(idx)
		info.Indices = append(info.Indices, index)
	}

	return info
}

// newDDLEvent converts from message to DDLEvent.
func newDDLEvent(msg *message) *model.DDLEvent {
	var preTableInfo *model.TableInfo
	if msg.PreTableSchema != nil {
		preTableInfo = newTableInfo(msg.PreTableSchema)
	}
	return &model.DDLEvent{
		StartTs:      msg.CommitTs,
		CommitTs:     msg.CommitTs,
		TableInfo:    newTableInfo(msg.TableSchema),
		PreTableInfo: preTableInfo,
		Query:        msg.SQL,
	}
}

// buildRowChangedEvent converts from message to RowChangedEvent.
func buildRowChangedEvent(msg *message, tableInfo *model.TableInfo) (*model.RowChangedEvent, error) {
	result := &model.RowChangedEvent{
		CommitTs: msg.CommitTs,
		Table: &model.TableName{
			Schema: msg.Schema,
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
	var result []*model.Column
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
	// Schema and Table is empty for the resolved ts event.
	Schema string    `json:"schema,omitempty"`
	Table  string    `json:"table,omitempty"`
	Type   EventType `json:"type"`
	// SQL is only for the DDL event.
	SQL      string `json:"sql,omitempty"`
	CommitTs uint64 `json:"commitTs"`
	BuildTs  int64  `json:"buildTs"`
	// SchemaVersion is for the DML event.
	SchemaVersion uint64 `json:"schemaVersion,omitempty"`

	// ClaimCheckLocation is only for the DML event.
	ClaimCheckLocation string `json:"claimCheckLocation,omitempty"`
	// HandleKeyOnly is only for the DML event.
	HandleKeyOnly bool `json:"handleKeyOnly,omitempty"`

	// E2E checksum related fields, only set when enable checksum functionality.
	Checksum        string `json:"checksum,omitempty"`
	OldChecksum     string `json:"oldChecksum,omitempty"`
	Corrupted       bool   `json:"corrupted,omitempty"`
	ChecksumVersion int    `json:"checksumVersion,omitempty"`

	// Data is available for the Insert and Update event.
	Data map[string]interface{} `json:"data,omitempty"`
	// Old is available for the Update and Delete event.
	Old map[string]interface{} `json:"old,omitempty"`
	// TableSchema is for the DDL and Bootstrap event.
	TableSchema *TableSchema `json:"tableSchema,omitempty"`
	// PreTableSchema holds schema information before the DDL executed.
	PreTableSchema *TableSchema `json:"preTableSchema,omitempty"`
}

func newResolvedMessage(ts uint64) *message {
	return &message{
		Version:  defaultVersion,
		Type:     WatermarkType,
		CommitTs: ts,
		BuildTs:  time.Now().UnixMilli(),
	}
}

func newDDLMessage(ddl *model.DDLEvent) *message {
	var (
		schema    *TableSchema
		preSchema *TableSchema
	)
	// the tableInfo maybe nil if the DDL is `drop database`
	if ddl.TableInfo != nil && ddl.TableInfo.TableInfo != nil {
		schema = newTableSchema(ddl.TableInfo)
	}
	if !ddl.IsBootstrap {
		// `PreTableInfo` may not exist for some DDL, such as `create table`
		if ddl.PreTableInfo != nil && ddl.PreTableInfo.TableInfo != nil {
			preSchema = newTableSchema(ddl.PreTableInfo)
		}
	}

	msg := &message{
		Version:        defaultVersion,
		Type:           DDLType,
		CommitTs:       ddl.CommitTs,
		BuildTs:        time.Now().UnixMilli(),
		SQL:            ddl.Query,
		TableSchema:    schema,
		PreTableSchema: preSchema,
	}
	if ddl.IsBootstrap {
		msg.Type = BootstrapType
		msg.SQL = ""
	}

	return msg
}

func newDMLMessage(
	event *model.RowChangedEvent, onlyHandleKey bool,
) (*message, error) {
	m := &message{
		Version:       defaultVersion,
		Schema:        event.Table.Schema,
		Table:         event.Table.Table,
		CommitTs:      event.CommitTs,
		BuildTs:       time.Now().UnixMilli(),
		SchemaVersion: event.TableInfo.UpdateTS,
		HandleKeyOnly: onlyHandleKey,
	}
	var err error
	if event.IsInsert() {
		m.Type = InsertType
		m.Data, err = formatColumns(event.Columns, event.ColInfos, onlyHandleKey)
		if err != nil {
			return nil, err
		}
	} else if event.IsDelete() {
		m.Type = DeleteType
		m.Old, err = formatColumns(event.PreColumns, event.ColInfos, onlyHandleKey)
		if err != nil {
			return nil, err
		}
	} else if event.IsUpdate() {
		m.Type = UpdateType
		m.Data, err = formatColumns(event.Columns, event.ColInfos, onlyHandleKey)
		if err != nil {
			return nil, err
		}
		m.Old, err = formatColumns(event.PreColumns, event.ColInfos, onlyHandleKey)
		if err != nil {
			return nil, err
		}
	} else {
		log.Panic("invalid event type, this should not hit", zap.Any("event", event))
	}

	return m, nil
}

func formatColumns(
	columns []*model.Column, columnInfos []rowcodec.ColInfo, onlyHandleKey bool,
) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(columns))
	for idx, col := range columns {
		if col == nil {
			continue
		}
		if onlyHandleKey && !col.Flag.IsHandleKey() {
			continue
		}
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
		switch v := value.(type) {
		case uint64:
			enumVar, err := tiTypes.ParseEnumValue(element, v)
			if err != nil {
				return "", cerror.WrapError(cerror.ErrEncodeFailed, err)
			}
			return enumVar.Name, nil
		case []uint8:
			return string(v), nil
		default:
			log.Panic("unexpected type for enum value", zap.Any("value", value))
		}
	case mysql.TypeSet:
		switch v := value.(type) {
		case uint64:
			setValue, err := tiTypes.ParseSetValue(ft.GetElems(), v)
			if err != nil {
				return "", cerror.WrapError(cerror.ErrEncodeFailed, err)
			}
			return setValue.Name, nil
		case []uint8:
			return string(v), nil
		default:
			log.Panic("unexpected type for set value", zap.Any("value", value))
		}
	case mysql.TypeBit:
		switch v := value.(type) {
		case []uint8:
			bitValue, err := common.BinaryLiteralToInt(v)
			if err != nil {
				return "", cerror.WrapError(cerror.ErrEncodeFailed, err)
			}
			value = bitValue
		default:
		}
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
		Type:      fieldType.GetType(),
		Charset:   fieldType.GetCharset(),
		Collation: fieldType.GetCollate(),
		Name:      name,
		Value:     value,
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
		return result, nil
	}

	var err error
	switch fieldType.GetType() {
	case mysql.TypeBit:
		value, err = strconv.ParseUint(data, 10, 64)
		if err != nil {
			log.Error("invalid column value for bit",
				zap.String("name", name), zap.Any("data", data),
				zap.Any("type", fieldType.GetType()), zap.Error(err))
			return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeInt24, mysql.TypeYear:
		value, err = strconv.ParseInt(data, 10, 64)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
		}
	case mysql.TypeLonglong:
		value, err = strconv.ParseInt(data, 10, 64)
		if err != nil {
			value, err = strconv.ParseUint(data, 10, 64)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
			}
		}
	case mysql.TypeFloat:
		value, err = strconv.ParseFloat(data, 32)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
		}
	case mysql.TypeDouble:
		value, err = strconv.ParseFloat(data, 64)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
		}
	case mysql.TypeEnum:
		element := fieldType.GetElems()
		enumVar, err := tiTypes.ParseEnumName(element, data, fieldType.GetCharset())
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
		}
		value = enumVar.Value
	case mysql.TypeSet:
		elements := fieldType.GetElems()
		setVar, err := tiTypes.ParseSetName(elements, data, fieldType.GetCharset())
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
		}
		value = setVar.Value
	default:
	}

	result.Value = value
	return result, nil
}
