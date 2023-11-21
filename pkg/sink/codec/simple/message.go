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
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
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
	tp := types.StrToType(strings.ToLower(strings.TrimSuffix(column.DataType, " UNSIGNED")))
	col.FieldType = *types.NewFieldType(tp)
	if strings.Contains(column.DataType, "UNSIGNED") {
		col.AddFlag(mysql.UnsignedFlag)
	}
	if strings.Contains(column.DataType, "BLOB") || strings.Contains(column.DataType, "BINARY") {
		col.SetCharset(charset.CharsetBin)
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

	if column.Nullable == false {
		col.AddFlag(mysql.NotNullFlag)
	}

	col.DefaultValue = column.Default

	return col
}

// IndexSchema is the schema of the index.
type IndexSchema struct {
	Name     string   `json:"name"`
	Unique   bool     `json:"unique,omitempty"`
	Primary  bool     `json:"primary,omitempty"`
	Nullable bool     `json:"nullable,omitempty"`
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
			Name: timodel.NewCIStr(msg.Table),
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

func buildRowChangedEvent(msg *message) (*model.RowChangedEvent, error) {
	result := &model.RowChangedEvent{
		CommitTs: msg.CommitTs,
		Table: &model.TableName{
			Schema: msg.Database,
			Table:  msg.Table,
		},
	}

	if msg.Data != nil {
		columns, err := buildColumns(msg.Data)
		if err != nil {
			return nil, err
		}
		result.Columns = columns
	}

	if msg.Old != nil {
		columns, err := buildColumns(msg.Old)
		if err != nil {
			return nil, err
		}
		result.PreColumns = columns
	}
	return result, nil
}

func buildColumns(rawData map[string]string) ([]*model.Column, error) {
	result := make([]*model.Column, 0, len(rawData))
	for name, value := range rawData {
		col := &model.Column{
			Name:  name,
			Value: value,
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
	Data map[string]string `json:"data,omitempty"`
	// Old is available for the Update and Delete event.
	Old map[string]string `json:"old,omitempty"`
	// TableSchema is for the DDL and Bootstrap event.
	TableSchema *TableSchema `json:"tableSchema,omitempty"`
	// SQL is only for the DDL event.
	SQL string `json:"sql,omitempty"`
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
		Version:     defaultVersion,
		Database:    ddl.TableInfo.TableName.Schema,
		Table:       ddl.TableInfo.TableName.Table,
		Type:        DDLType,
		CommitTs:    ddl.CommitTs,
		SQL:         ddl.Query,
		TableSchema: tableSchema,
	}
	if ddl.IsBootstrap {
		msg.Type = BootstrapType
		msg.SQL = ""
	}

	return msg
}

func newDMLMessage(event *model.RowChangedEvent) *message {
	m := &message{
		Version:  defaultVersion,
		Database: event.Table.Schema,
		Table:    event.Table.Table,
		CommitTs: event.CommitTs,
	}
	if event.IsInsert() {
		m.Type = InsertType
		m.Data = formatColumns(event.Columns)
	} else if event.IsDelete() {
		m.Type = DeleteType
		m.Old = formatColumns(event.PreColumns)
	} else if event.IsUpdate() {
		m.Type = UpdateType
		m.Data = formatColumns(event.Columns)
		m.Old = formatColumns(event.PreColumns)
	} else {
		log.Panic("invalid event type, this should not hit", zap.Any("event", event))
	}

	return m
}

func formatColumns(columns []*model.Column) map[string]string {
	result := make(map[string]string, len(columns))
	for _, col := range columns {
		result[col.Name] = formatValue(col.Value, col.Flag.IsBinary())
	}
	return result
}

func formatValue(value interface{}, isBinary bool) string {
	if value == nil {
		return ""
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
		if isBinary {
			result = base64.StdEncoding.EncodeToString(v)
		} else {
			result = string(v)
		}
	default:
		result = fmt.Sprintf("%v", v)
	}
	return result
}
