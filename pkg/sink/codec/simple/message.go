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
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/integrity"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"go.uber.org/zap"
)

const (
	defaultVersion = 1
)

// MessageType is the type of the message.
type MessageType string

const (
	// MessageTypeWatermark is the type of the watermark event.
	MessageTypeWatermark MessageType = "WATERMARK"
	// MessageTypeBootstrap is the type of the bootstrap event.
	MessageTypeBootstrap MessageType = "BOOTSTRAP"
	// MessageTypeDDL is the type of the ddl event.
	MessageTypeDDL MessageType = "DDL"
	// MessageTypeDML is the type of the row event.
	MessageTypeDML MessageType = "DML"
)

// DML Message types
const (
	// DMLTypeInsert is the type of the insert event.
	DMLTypeInsert MessageType = "INSERT"
	// DMLTypeUpdate is the type of the update event.
	DMLTypeUpdate MessageType = "UPDATE"
	// DMLTypeDelete is the type of the delete event.
	DMLTypeDelete MessageType = "DELETE"
)

// DDL message types
const (
	DDLTypeCreate   MessageType = "CREATE"
	DDLTypeRename   MessageType = "RENAME"
	DDLTypeCIndex   MessageType = "CINDEX"
	DDLTypeDIndex   MessageType = "DINDEX"
	DDLTypeErase    MessageType = "ERASE"
	DDLTypeTruncate MessageType = "TRUNCATE"
	DDLTypeAlter    MessageType = "ALTER"
	DDLTypeQuery    MessageType = "QUERY"
)

func getDDLType(t timodel.ActionType) MessageType {
	switch t {
	case timodel.ActionCreateTable:
		return DDLTypeCreate
	case timodel.ActionRenameTable, timodel.ActionRenameTables:
		return DDLTypeRename
	case timodel.ActionAddIndex, timodel.ActionAddForeignKey, timodel.ActionAddPrimaryKey:
		return DDLTypeCIndex
	case timodel.ActionDropIndex, timodel.ActionDropForeignKey, timodel.ActionDropPrimaryKey:
		return DDLTypeDIndex
	case timodel.ActionDropTable:
		return DDLTypeErase
	case timodel.ActionTruncateTable:
		return DDLTypeTruncate
	case timodel.ActionAddColumn, timodel.ActionDropColumn, timodel.ActionModifyColumn, timodel.ActionRebaseAutoID,
		timodel.ActionSetDefaultValue, timodel.ActionModifyTableComment, timodel.ActionRenameIndex, timodel.ActionAddTablePartition,
		timodel.ActionDropTablePartition, timodel.ActionModifyTableCharsetAndCollate, timodel.ActionTruncateTablePartition,
		timodel.ActionAlterIndexVisibility, timodel.ActionMultiSchemaChange, timodel.ActionReorganizePartition,
		timodel.ActionAlterTablePartitioning, timodel.ActionRemovePartitioning:
		return DDLTypeAlter
	default:
		return DDLTypeQuery
	}
}

// columnSchema is the schema of the column.
type columnSchema struct {
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
func newColumnSchema(col *timodel.ColumnInfo) (*columnSchema, error) {
	tp := dataType{
		MySQLType: types.TypeToStr(col.GetType(), col.GetCharset()),
		Charset:   col.GetCharset(),
		Collate:   col.GetCollate(),
		Length:    col.GetFlen(),
		Elements:  col.GetElems(),
		Unsigned:  mysql.HasUnsignedFlag(col.GetFlag()),
		Zerofill:  mysql.HasZerofillFlag(col.GetFlag()),
	}

	switch col.GetType() {
	// Float and Double decimal is always -1, do not encode it into the schema.
	case mysql.TypeFloat, mysql.TypeDouble:
	default:
		tp.Decimal = col.GetDecimal()
	}

	defaultValue := model.GetColumnDefaultValue(col)
	if defaultValue != nil && col.GetType() == mysql.TypeBit {
		var err error
		defaultValue, err = common.BinaryLiteralToInt([]byte(defaultValue.(string)))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrEncodeFailed, err)
		}
	}
	return &columnSchema{
		Name:     col.Name.O,
		DataType: tp,
		Nullable: !mysql.HasNotNullFlag(col.GetFlag()),
		Default:  defaultValue,
	}, nil
}

// newTiColumnInfo uses columnSchema and IndexSchema to construct a tidb column info.
func newTiColumnInfo(
	column *columnSchema, colID int64, indexes []*IndexSchema,
) (*timodel.ColumnInfo, error) {
	col := new(timodel.ColumnInfo)
	col.ID = colID
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

	if !column.Nullable {
		col.AddFlag(mysql.NotNullFlag)
	}

	defaultValue := column.Default
	if defaultValue != nil && col.GetType() == mysql.TypeBit {
		switch v := defaultValue.(type) {
		case float64:
			byteSize := (col.GetFlen() + 7) >> 3
			defaultValue = tiTypes.NewBinaryLiteralFromUint(uint64(v), byteSize)
			defaultValue = defaultValue.(tiTypes.BinaryLiteral).ToString()
		default:
		}
	}
	err := col.SetDefaultValue(defaultValue)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
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

	return col, nil
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
	Schema  string          `json:"schema"`
	Table   string          `json:"table"`
	TableID int64           `json:"tableID"`
	Version uint64          `json:"version"`
	Columns []*columnSchema `json:"columns"`
	Indexes []*IndexSchema  `json:"indexes"`
}

func newTableSchema(tableInfo *model.TableInfo) (*TableSchema, error) {
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
				Columns:  pkColumns,
			}
			indexes = append(indexes, index)
		}
	}

	sort.SliceStable(tableInfo.Columns, func(i, j int) bool {
		return tableInfo.Columns[i].ID < tableInfo.Columns[j].ID
	})

	columns := make([]*columnSchema, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		colSchema, err := newColumnSchema(col)
		if err != nil {
			return nil, err
		}
		columns = append(columns, colSchema)
	}

	return &TableSchema{
		Schema:  tableInfo.TableName.Schema,
		Table:   tableInfo.TableName.Table,
		TableID: tableInfo.ID,
		Version: tableInfo.UpdateTS,
		Columns: columns,
		Indexes: indexes,
	}, nil
}

// newTableInfo converts from TableSchema to TableInfo.
func newTableInfo(m *TableSchema) (*model.TableInfo, error) {
	var (
		database      string
		table         string
		tableID       int64
		schemaVersion uint64
	)
	if m != nil {
		database = m.Schema
		table = m.Table
		tableID = m.TableID
		schemaVersion = m.Version
	}
	tidbTableInfo := &timodel.TableInfo{
		ID:       tableID,
		Name:     timodel.NewCIStr(table),
		UpdateTS: schemaVersion,
	}

	if m == nil {
		return &model.TableInfo{
			TableName: model.TableName{
				Schema:  database,
				Table:   table,
				TableID: tableID,
			},
			TableInfo: tidbTableInfo,
		}, nil
	}

	nextMockID := int64(100)
	for _, col := range m.Columns {
		tiCol, err := newTiColumnInfo(col, nextMockID, m.Indexes)
		nextMockID += 100
		if err != nil {
			return nil, err
		}
		tidbTableInfo.Columns = append(tidbTableInfo.Columns, tiCol)
	}
	for _, idx := range m.Indexes {
		index := newTiIndexInfo(idx)
		tidbTableInfo.Indices = append(tidbTableInfo.Indices, index)
	}
	info := model.WrapTableInfo(100, database, schemaVersion, tidbTableInfo)

	return info, nil
}

// newDDLEvent converts from message to DDLEvent.
func newDDLEvent(msg *message) (*model.DDLEvent, error) {
	var (
		tableInfo    *model.TableInfo
		preTableInfo *model.TableInfo
		err          error
	)

	tableInfo, err = newTableInfo(msg.TableSchema)
	if err != nil {
		return nil, err
	}

	if msg.PreTableSchema != nil {
		preTableInfo, err = newTableInfo(msg.PreTableSchema)
		if err != nil {
			return nil, err
		}
	}
	return &model.DDLEvent{
		StartTs:      msg.CommitTs,
		CommitTs:     msg.CommitTs,
		TableInfo:    tableInfo,
		PreTableInfo: preTableInfo,
		Query:        msg.SQL,
	}, nil
}

// buildRowChangedEvent converts from message to RowChangedEvent.
func buildRowChangedEvent(
	msg *message, tableInfo *model.TableInfo, enableRowChecksum bool,
) (*model.RowChangedEvent, error) {
	result := &model.RowChangedEvent{
		CommitTs:        msg.CommitTs,
		PhysicalTableID: msg.TableID,
		TableInfo:       tableInfo,
	}

	columns, err := decodeColumns(msg.Data, tableInfo.Columns)
	if err != nil {
		return nil, err
	}
	result.Columns = model.Columns2ColumnDatas(columns, tableInfo)

	preColumns, err := decodeColumns(msg.Old, tableInfo.Columns)
	if err != nil {
		return nil, err
	}
	result.PreColumns = model.Columns2ColumnDatas(preColumns, tableInfo)

	if enableRowChecksum && msg.Checksum != nil {
		err = common.VerifyChecksum(preColumns, msg.Checksum.Previous)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
		}
		err = common.VerifyChecksum(columns, msg.Checksum.Current)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
		}

		result.Checksum = &integrity.Checksum{
			Previous:  msg.Checksum.Previous,
			Current:   msg.Checksum.Current,
			Corrupted: msg.Checksum.Corrupted,
			Version:   msg.Checksum.Version,
		}

		if msg.Checksum.Corrupted {
			log.Warn("cdc detect checksum corrupted",
				zap.String("schema", msg.Schema),
				zap.String("table", msg.Table))
			for _, col := range preColumns {
				log.Info("data corrupted, print each previous column for debugging",
					zap.String("name", col.Name),
					zap.Any("type", col.Type),
					zap.Any("charset", col.Charset),
					zap.Any("flag", col.Flag),
					zap.Any("value", col.Value),
					zap.Any("default", col.Default))
			}
			for _, col := range columns {
				log.Info("data corrupted, print each column for debugging",
					zap.String("name", col.Name),
					zap.Any("type", col.Type),
					zap.Any("charset", col.Charset),
					zap.Any("flag", col.Flag),
					zap.Any("value", col.Value),
					zap.Any("default", col.Default))
			}
		}
	}

	return result, nil
}

func decodeColumns(
	rawData map[string]interface{}, columnInfos []*timodel.ColumnInfo,
) ([]*model.Column, error) {
	if rawData == nil {
		return nil, nil
	}
	var result []*model.Column
	for _, info := range columnInfos {
		value, ok := rawData[info.Name.O]
		if !ok {
			log.Error("cannot found the value for the column",
				zap.String("column", info.Name.O))
		}
		col, err := decodeColumn(info.Name.O, value, &info.FieldType)
		if err != nil {
			return nil, err
		}
		result = append(result, col)
	}
	return result, nil
}

type checksum struct {
	Version   int    `json:"version"`
	Corrupted bool   `json:"corrupted"`
	Current   uint32 `json:"current"`
	Previous  uint32 `json:"previous"`
}

type message struct {
	Version int `json:"version"`
	// Schema and Table is empty for the resolved ts event.
	Schema  string      `json:"database,omitempty"`
	Table   string      `json:"table,omitempty"`
	TableID int64       `json:"tableID,omitempty"`
	Type    MessageType `json:"type"`
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
	Checksum *checksum `json:"checksum,omitempty"`

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
		Type:     MessageTypeWatermark,
		CommitTs: ts,
		BuildTs:  time.Now().UnixMilli(),
	}
}

func newBootstrapMessage(tableInfo *model.TableInfo) (*message, error) {
	schema, err := newTableSchema(tableInfo)
	if err != nil {
		return nil, err
	}
	msg := &message{
		Version:     defaultVersion,
		Type:        MessageTypeBootstrap,
		BuildTs:     time.Now().UnixMilli(),
		TableSchema: schema,
	}
	return msg, nil
}

func newDDLMessage(ddl *model.DDLEvent) (*message, error) {
	var (
		schema    *TableSchema
		preSchema *TableSchema
		err       error
	)
	// the tableInfo maybe nil if the DDL is `drop database`
	if ddl.TableInfo != nil && ddl.TableInfo.TableInfo != nil {
		schema, err = newTableSchema(ddl.TableInfo)
		if err != nil {
			return nil, err
		}
	}
	// `PreTableInfo` may not exist for some DDL, such as `create table`
	if ddl.PreTableInfo != nil && ddl.PreTableInfo.TableInfo != nil {
		preSchema, err = newTableSchema(ddl.PreTableInfo)
		if err != nil {
			return nil, err
		}
	}
	msg := &message{
		Version:        defaultVersion,
		Type:           getDDLType(ddl.Type),
		CommitTs:       ddl.CommitTs,
		BuildTs:        time.Now().UnixMilli(),
		SQL:            ddl.Query,
		TableSchema:    schema,
		PreTableSchema: preSchema,
	}
	return msg, nil
}

func (a *jsonMarshaller) newDMLMessage(
	event *model.RowChangedEvent,
	onlyHandleKey bool, claimCheckFileName string,
) (*message, error) {
	m := &message{
		Version:            defaultVersion,
		Schema:             event.TableInfo.GetSchemaName(),
		Table:              event.TableInfo.GetTableName(),
		TableID:            event.TableInfo.ID,
		CommitTs:           event.CommitTs,
		BuildTs:            time.Now().UnixMilli(),
		SchemaVersion:      event.TableInfo.UpdateTS,
		HandleKeyOnly:      onlyHandleKey,
		ClaimCheckLocation: claimCheckFileName,
	}
	var err error
	if event.IsInsert() {
		m.Type = DMLTypeInsert
		m.Data, err = a.formatColumns(event.Columns, event.TableInfo, onlyHandleKey)
		if err != nil {
			return nil, err
		}
	} else if event.IsDelete() {
		m.Type = DMLTypeDelete
		m.Old, err = a.formatColumns(event.PreColumns, event.TableInfo, onlyHandleKey)
		if err != nil {
			return nil, err
		}
	} else if event.IsUpdate() {
		m.Type = DMLTypeUpdate
		m.Data, err = a.formatColumns(event.Columns, event.TableInfo, onlyHandleKey)
		if err != nil {
			return nil, err
		}
		m.Old, err = a.formatColumns(event.PreColumns, event.TableInfo, onlyHandleKey)
		if err != nil {
			return nil, err
		}
	} else {
		log.Panic("invalid event type, this should not hit", zap.Any("event", event))
	}

	if a.config.EnableRowChecksum && event.Checksum != nil {
		m.Checksum = &checksum{
			Version:   event.Checksum.Version,
			Corrupted: event.Checksum.Corrupted,
			Current:   event.Checksum.Current,
			Previous:  event.Checksum.Previous,
		}
	}

	return m, nil
}

func (a *jsonMarshaller) formatColumns(
	columns []*model.ColumnData, tableInfo *model.TableInfo, onlyHandleKey bool,
) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(columns))
	colInfos := tableInfo.GetColInfosForRowChangedEvent()
	for i, col := range columns {
		if col == nil {
			continue
		}
		flag := tableInfo.ForceGetColumnFlagType(col.ColumnID)
		if onlyHandleKey && !flag.IsHandleKey() {
			continue
		}
		value, err := encodeValue(col.Value, colInfos[i].Ft, a.config.TimeZone.String())
		if err != nil {
			return nil, err
		}
		result[tableInfo.ForceGetColumnName(col.ColumnID)] = value
	}
	return result, nil
}

func (a *avroMarshaller) encodeValue4Avro(
	value interface{}, ft *types.FieldType,
) (interface{}, string, error) {
	if value == nil {
		return nil, "null", nil
	}

	switch ft.GetType() {
	case mysql.TypeEnum:
		v, ok := value.(uint64)
		if !ok {
			return nil, "", cerror.ErrEncodeFailed.
				GenWithStack("unexpected type for the enum value: %+v, tp: %+v", value, reflect.TypeOf(value))
		}

		enumVar, err := tiTypes.ParseEnumValue(ft.GetElems(), v)
		if err != nil {
			return nil, "", cerror.WrapError(cerror.ErrEncodeFailed, err)
		}
		value = enumVar.Name
	case mysql.TypeSet:
		v, ok := value.(uint64)
		if !ok {
			return nil, "", cerror.ErrEncodeFailed.
				GenWithStack("unexpected type for the set value: %+v, tp: %+v", value, reflect.TypeOf(value))
		}
		setValue, err := tiTypes.ParseSetValue(ft.GetElems(), v)
		if err != nil {
			return nil, "", cerror.WrapError(cerror.ErrEncodeFailed, err)
		}
		value = setValue.Name
	case mysql.TypeTimestamp:
		v, ok := value.(string)
		if !ok {
			return nil, "", cerror.ErrEncodeFailed.
				GenWithStack("unexpected type for the timestamp value: %+v, tp: %+v", value, reflect.TypeOf(value))
		}
		return map[string]interface{}{
			"location": a.config.TimeZone.String(),
			"value":    v,
		}, "com.pingcap.simple.avro.Timestamp", nil
	case mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			v, ok := value.(uint64)
			if !ok {
				return nil, "", cerror.ErrEncodeFailed.
					GenWithStack("unexpected type for the unsigned bigint value: %+v, tp: %+v", value, reflect.TypeOf(value))
			}
			return map[string]interface{}{
				"value": int64(v),
			}, "com.pingcap.simple.avro.UnsignedBigint", nil
		}
	}

	switch v := value.(type) {
	case uint64:
		return int64(v), "long", nil
	case int64:
		return v, "long", nil
	case []byte:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			return v, "bytes", nil
		}
		return string(v), "string", nil
	case float32:
		return v, "float", nil
	case float64:
		return v, "double", nil
	case string:
		return v, "string", nil
	default:
		log.Panic("unexpected type for avro value", zap.Any("value", value))
	}

	return value, "", nil
}

type timestamp struct {
	// location specifies the location of the `timestamp` typed value,
	// so that the consumer can convert it to any other timezone location.
	Location string `json:"location"`
	Value    string `json:"value"`
}

func encodeValue(
	value interface{}, ft *types.FieldType, location string,
) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch ft.GetType() {
	case mysql.TypeEnum:
		switch v := value.(type) {
		case uint64:
			enumVar, err := tiTypes.ParseEnumValue(ft.GetElems(), v)
			if err != nil {
				return "", cerror.WrapError(cerror.ErrEncodeFailed, err)
			}
			return enumVar.Name, nil
		case []uint8:
			return string(v), nil
		case string:
			return v, nil
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
	case mysql.TypeTimestamp:
		return timestamp{
			Location: location,
			Value:    value.(string),
		}, nil
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

	var err error
	if mysql.HasBinaryFlag(fieldType.GetFlag()) {
		switch v := value.(type) {
		case string:
			value, err = base64.StdEncoding.DecodeString(v)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
			}
		default:
		}
		result.Value = value
		return result, nil
	}

	switch fieldType.GetType() {
	case mysql.TypeBit:
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseUint(v, 10, 64)
			if err != nil {
				log.Error("invalid column value for bit",
					zap.String("name", name), zap.Any("data", v),
					zap.Any("type", fieldType.GetType()), zap.Error(err))
				return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
			}
		case []uint8:
			value, err = common.BinaryLiteralToInt(v)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
			}
		case uint64:
			value = v
		case int64:
			value = uint64(v)
		default:
			log.Panic("unexpected type for bit value", zap.Any("value", value))
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeInt24, mysql.TypeYear:
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
			}
		default:
			value = v
		}
	case mysql.TypeLonglong:
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				value, err = strconv.ParseUint(v, 10, 64)
				if err != nil {
					return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
				}
			}
		case map[string]interface{}:
			value = uint64(v["value"].(int64))
		default:
			value = v
		}
	case mysql.TypeFloat:
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseFloat(v, 32)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
			}
		default:
			value = v
		}
	case mysql.TypeDouble:
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
			}
		default:
			value = v
		}
	case mysql.TypeEnum:
		switch v := value.(type) {
		case string:
			element := fieldType.GetElems()
			enumVar, err := tiTypes.ParseEnumName(element, v, fieldType.GetCharset())
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
			}
			value = enumVar.Value
		case uint64:
			log.Panic("unexpected type for enum value", zap.Any("value", value))
		}
	case mysql.TypeSet:
		switch v := value.(type) {
		case string:
			elements := fieldType.GetElems()
			setVar, err := tiTypes.ParseSetName(elements, v, fieldType.GetCharset())
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrDecodeFailed, err)
			}
			value = setVar.Value
		case uint64:
			log.Panic("unexpected type for set value", zap.Any("value", value))
		}
	case mysql.TypeTimestamp:
		v := value.(map[string]interface{})
		value = v["value"].(string)
	default:
	}

	result.Value = value
	return result, nil
}
