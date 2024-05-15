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
func newColumnSchema(col *timodel.ColumnInfo) *columnSchema {
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
		defaultValue = common.MustBinaryLiteralToInt([]byte(defaultValue.(string)))
	}
	return &columnSchema{
		Name:     col.Name.O,
		DataType: tp,
		Nullable: !mysql.HasNotNullFlag(col.GetFlag()),
		Default:  defaultValue,
	}
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

func newTableSchema(tableInfo *model.TableInfo) *TableSchema {
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
		colSchema := newColumnSchema(col)
		columns = append(columns, colSchema)
	}

	return &TableSchema{
		Schema:  tableInfo.TableName.Schema,
		Table:   tableInfo.TableName.Table,
		TableID: tableInfo.ID,
		Version: tableInfo.UpdateTS,
		Columns: columns,
		Indexes: indexes,
	}
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

	result.Columns = decodeColumns(msg.Data, tableInfo)
	result.PreColumns = decodeColumns(msg.Old, tableInfo)

	if enableRowChecksum && msg.Checksum != nil {
		var (
			previousCorrupted bool
			currentCorrupted  bool
		)
		err := common.VerifyChecksum(result.PreColumns, tableInfo.Columns, msg.Checksum.Previous)
		if err != nil {
			log.Info("checksum corrupted on the previous columns", zap.Any("message", msg))
			previousCorrupted = true
		}
		err = common.VerifyChecksum(result.Columns, tableInfo.Columns, msg.Checksum.Current)
		if err != nil {
			log.Info("checksum corrupted on the current columns", zap.Any("message", msg))
			currentCorrupted = true
		}

		result.Checksum = &integrity.Checksum{
			Previous:  msg.Checksum.Previous,
			Current:   msg.Checksum.Current,
			Corrupted: msg.Checksum.Corrupted,
			Version:   msg.Checksum.Version,
		}

		corrupted := msg.Checksum.Corrupted || previousCorrupted || currentCorrupted
		if corrupted {
			log.Warn("consumer detect checksum corrupted",
				zap.String("schema", msg.Schema),
				zap.String("table", msg.Table))
			for _, col := range result.PreColumns {
				colInfo := tableInfo.ForceGetColumnInfo(col.ColumnID)
				log.Info("data corrupted, print each previous column for debugging",
					zap.String("name", colInfo.Name.O),
					zap.Any("type", colInfo.GetType()),
					zap.Any("charset", colInfo.GetCharset()),
					zap.Any("flag", colInfo.GetFlag()),
					zap.Any("value", col.Value),
					zap.Any("default", colInfo.GetDefaultValue()))
			}
			for _, col := range result.Columns {
				colInfo := tableInfo.ForceGetColumnInfo(col.ColumnID)
				log.Info("data corrupted, print each column for debugging",
					zap.String("name", colInfo.Name.O),
					zap.Any("type", colInfo.GetType()),
					zap.Any("charset", colInfo.GetCharset()),
					zap.Any("flag", colInfo.GetFlag()),
					zap.Any("value", col.Value),
					zap.Any("default", colInfo.GetDefaultValue()))
			}
			return nil, cerror.ErrDecodeFailed.GenWithStackByArgs("checksum corrupted")
		}
	}

	for _, col := range result.Columns {
		adjustTimestampValue(col, tableInfo.ForceGetColumnInfo(col.ColumnID).FieldType)
	}
	for _, col := range result.PreColumns {
		adjustTimestampValue(col, tableInfo.ForceGetColumnInfo(col.ColumnID).FieldType)
	}

	return result, nil
}

func adjustTimestampValue(column *model.ColumnData, flag types.FieldType) {
	if flag.GetType() != mysql.TypeTimestamp {
		return
	}
	if column.Value == nil {
		return
	}
	var ts string
	switch v := column.Value.(type) {
	case map[string]string:
		ts = v["value"]
	case map[string]interface{}:
		ts = v["value"].(string)
	}
	column.Value = ts
}

func decodeColumns(
	rawData map[string]interface{}, tableInfo *model.TableInfo,
) []*model.ColumnData {
	if rawData == nil {
		return nil
	}
	var result []*model.ColumnData
	for _, info := range tableInfo.Columns {
		value, ok := rawData[info.Name.O]
		if !ok {
			log.Warn("cannot found the value for the column, "+
				"it must be a generated column and TiCDC does not replicate generated column value",
				zap.String("column", info.Name.O))
			continue
		}
		columnID := tableInfo.ForceGetColumnIDByName(info.Name.O)
		col := decodeColumn(value, columnID, &info.FieldType)
		if col == nil {
			log.Panic("cannot decode column",
				zap.String("name", info.Name.O), zap.Any("data", value))
		}

		result = append(result, col)
	}
	return result
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

func newBootstrapMessage(tableInfo *model.TableInfo) *message {
	schema := newTableSchema(tableInfo)
	msg := &message{
		Version:     defaultVersion,
		Type:        MessageTypeBootstrap,
		BuildTs:     time.Now().UnixMilli(),
		TableSchema: schema,
	}
	return msg
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
	// `PreTableInfo` may not exist for some DDL, such as `create table`
	if ddl.PreTableInfo != nil && ddl.PreTableInfo.TableInfo != nil {
		preSchema = newTableSchema(ddl.PreTableInfo)
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
	return msg
}

func (a *jsonMarshaller) newDMLMessage(
	event *model.RowChangedEvent,
	onlyHandleKey bool, claimCheckFileName string,
) *message {
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
	if event.IsInsert() {
		m.Type = DMLTypeInsert
		m.Data = a.formatColumns(event.Columns, event.TableInfo, onlyHandleKey)
	} else if event.IsDelete() {
		m.Type = DMLTypeDelete
		m.Old = a.formatColumns(event.PreColumns, event.TableInfo, onlyHandleKey)
	} else if event.IsUpdate() {
		m.Type = DMLTypeUpdate
		m.Data = a.formatColumns(event.Columns, event.TableInfo, onlyHandleKey)
		m.Old = a.formatColumns(event.PreColumns, event.TableInfo, onlyHandleKey)
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

	return m
}

func (a *jsonMarshaller) formatColumns(
	columns []*model.ColumnData, tableInfo *model.TableInfo, onlyHandleKey bool,
) map[string]interface{} {
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
		value := encodeValue(col.Value, colInfos[i].Ft, a.config.TimeZone.String())
		result[tableInfo.ForceGetColumnName(col.ColumnID)] = value
	}
	return result
}

func (a *avroMarshaller) encodeValue4Avro(
	value interface{}, ft *types.FieldType,
) (interface{}, string) {
	if value == nil {
		return nil, "null"
	}

	switch ft.GetType() {
	case mysql.TypeTimestamp:
		return map[string]interface{}{
			"location": a.config.TimeZone.String(),
			"value":    value.(string),
		}, "com.pingcap.simple.avro.Timestamp"
	case mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			return map[string]interface{}{
				"value": int64(value.(uint64)),
			}, "com.pingcap.simple.avro.UnsignedBigint"
		}
	}

	switch v := value.(type) {
	case uint64:
		return int64(v), "long"
	case int64:
		return v, "long"
	case []byte:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			return v, "bytes"
		}
		return string(v), "string"
	case float32:
		return v, "float"
	case float64:
		return v, "double"
	case string:
		return v, "string"
	default:
		log.Panic("unexpected type for avro value", zap.Any("value", value))
	}

	return value, ""
}

func encodeValue(
	value interface{}, ft *types.FieldType, location string,
) interface{} {
	if value == nil {
		return nil
	}

	switch ft.GetType() {
	case mysql.TypeBit:
		switch v := value.(type) {
		case []uint8:
			value = common.MustBinaryLiteralToInt(v)
		default:
		}
	case mysql.TypeTimestamp:
		var ts string
		switch v := value.(type) {
		case string:
			ts = v
		// the timestamp value maybe []uint8 if it's queried from upstream TiDB.
		case []uint8:
			ts = string(v)
		}
		return map[string]string{
			"location": location,
			"value":    ts,
		}
	case mysql.TypeEnum:
		switch v := value.(type) {
		case []uint8:
			data := string(v)
			enum, err := tiTypes.ParseEnumName(ft.GetElems(), data, ft.GetCollate())
			if err != nil {
				log.Panic("parse enum name failed",
					zap.Any("elems", ft.GetElems()), zap.String("name", data), zap.Error(err))
			}
			return enum.Value
		}
	case mysql.TypeSet:
		switch v := value.(type) {
		case []uint8:
			data := string(v)
			set, err := tiTypes.ParseSetName(ft.GetElems(), data, ft.GetCollate())
			if err != nil {
				log.Panic("parse set name failed",
					zap.Any("elems", ft.GetElems()), zap.String("name", data), zap.Error(err))
			}
			return set.Value
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

	return result
}

func decodeColumn(value interface{}, id int64, fieldType *types.FieldType) *model.ColumnData {
	result := &model.ColumnData{
		ColumnID: id,
		Value:    value,
	}
	if value == nil {
		return result
	}

	var err error
	if mysql.HasBinaryFlag(fieldType.GetFlag()) {
		switch v := value.(type) {
		case string:
			value, err = base64.StdEncoding.DecodeString(v)
			if err != nil {
				return nil
			}
		default:
		}
		result.Value = value
		return result
	}

	switch fieldType.GetType() {
	case mysql.TypeBit, mysql.TypeSet:
		switch v := value.(type) {
		// avro encoding, set is encoded as `int64`, bit encoded as `string`
		// json encoding, set is encoded as `string`, bit encoded as `string`
		case string:
			value, err = strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil
			}
		case []uint8:
			value = common.MustBinaryLiteralToInt(v)
		case uint64:
			value = v
		case int64:
			value = uint64(v)
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeInt24, mysql.TypeYear:
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil
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
					return nil
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
				return nil
			}
		default:
			value = v
		}
	case mysql.TypeDouble:
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return nil
			}
		default:
			value = v
		}
	case mysql.TypeEnum:
		// avro encoding, enum is encoded as `int64`, use it directly.
		// json encoding, enum is encoded as `string`
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil
			}
		}
	default:
	}

	result.Value = value
	return result
}
