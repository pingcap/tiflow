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

// ColumnSchema is the schema of the column.
type ColumnSchema struct {
	Name     string      `json:"name"`
	DataType string      `json:"dataType"`
	Nullable string      `json:"nullable"`
	Default  interface{} `json:"default"`
}

// FromTiColumnInfo converts from TiDB ColumnInfo to TableCol.
func (c *ColumnSchema) FromTiColumnInfo(col *timodel.ColumnInfo) {
	c.Name = col.Name.O
	c.DataType = strings.ToUpper(types.TypeToStr(col.GetType(), col.GetCharset()))
	if mysql.HasUnsignedFlag(col.GetFlag()) {
		c.DataType += " UNSIGNED"
	}
	if mysql.HasNotNullFlag(col.GetFlag()) {
		c.Nullable = "false"
	}
	c.Default = entry.GetDDLDefaultDefinition(col)
}

// ToTiColumnInfo converts from TableCol to TiDB ColumnInfo.
func (c *ColumnSchema) ToTiColumnInfo(indexes []*IndexSchema) (*timodel.ColumnInfo, error) {
	col := new(timodel.ColumnInfo)

	col.Name = timodel.NewCIStr(c.Name)
	tp := types.StrToType(strings.ToLower(strings.TrimSuffix(c.DataType, " UNSIGNED")))
	col.FieldType = *types.NewFieldType(tp)
	if strings.Contains(c.DataType, "UNSIGNED") {
		col.AddFlag(mysql.UnsignedFlag)
	}
	if strings.Contains(c.DataType, "BLOB") || strings.Contains(c.DataType, "BINARY") {
		col.SetCharset(charset.CharsetBin)
	} else {
		col.SetCharset(charset.CharsetUTF8MB4)
	}

	var primaryKey *IndexSchema
	for _, index := range indexes {
		if index.Primary == "true" {
			primaryKey = index
			break
		}
	}

	// add primary key flag
	if primaryKey != nil {
		for _, name := range primaryKey.Columns {
			if name == c.Name {
				col.AddFlag(mysql.PriKeyFlag)
				break
			}
		}
	}

	if c.Nullable == "false" {
		col.AddFlag(mysql.NotNullFlag)
	}

	col.DefaultValue = c.Default

	return col, nil
}

// IndexSchema is the schema of the index.
type IndexSchema struct {
	Name     string   `json:"name"`
	Unique   string   `json:"unique,omitempty"`
	Primary  string   `json:"primary,omitempty"`
	Nullable string   `json:"nullable,omitempty"`
	Columns  []string `json:"columns"`
}

// FromTiIndexInfo converts from TiDB IndexInfo to Index.
func (i *IndexSchema) FromTiIndexInfo(index *timodel.IndexInfo, tableInfo *timodel.TableInfo) {
	i.Name = index.Name.O

	if index.Unique {
		i.Unique = "true"
	} else {
		i.Unique = "false"
	}

	if index.Primary {
		i.Primary = "true"
	} else {
		i.Primary = "false"
	}

	for _, col := range index.Columns {
		i.Columns = append(i.Columns, col.Name.O)
	}

	for _, col := range index.Columns {
		offset := col.Offset
		colInfo := tableInfo.Columns[offset]
		if mysql.HasNotNullFlag(colInfo.GetFlag()) {
			i.Nullable = "false"
			break
		}
	}
}

// ToTiIndexInfo converts from Index to TiDB IndexInfo.
func (i *IndexSchema) ToTiIndexInfo() (*timodel.IndexInfo, error) {
	index := new(timodel.IndexInfo)
	index.Columns = make([]*timodel.IndexColumn, 0)
	for i, col := range i.Columns {
		index.Columns = append(index.Columns, &timodel.IndexColumn{
			Name:   timodel.NewCIStr(col),
			Offset: i,
		})
	}
	if i.Unique == "true" {
		index.Unique = true
	}
	if i.Primary == "true" {
		index.Primary = true
	}
	return index, nil
}

// TableSchema is the schema of the table.
type TableSchema struct {
	Columns []*ColumnSchema `json:"columns"`
	Indexes []*IndexSchema  `json:"indexes"`
}

// FromTableInfo converts from model.TableInfo to TableSchema.
func (t *TableSchema) FromTableInfo(tableInfo *model.TableInfo) {
	tiColumns := make([]*timodel.ColumnInfo, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		tiColumns = append(tiColumns, col.Clone())
	}
	// sort by column by its id
	sort.SliceStable(tiColumns, func(i, j int) bool {
		return int(tiColumns[i].ID) < int(tiColumns[j].ID)
	})

	columns := make([]*ColumnSchema, 0, len(tiColumns))
	for _, col := range tiColumns {
		column := &ColumnSchema{}
		column.FromTiColumnInfo(col)
		columns = append(columns, column)
	}

	indexes := make([]*IndexSchema, 0, len(tableInfo.Indices))
	for _, idx := range tableInfo.Indices {
		index := &IndexSchema{}
		index.FromTiIndexInfo(idx, tableInfo.TableInfo)
		indexes = append(indexes, index)
	}

	t.Columns = columns
	t.Indexes = indexes
}

// ToTableInfo converts from TableSchema to TableInfo.
func (t *TableSchema) ToTableInfo(msg *message) (*model.TableInfo, error) {
	info := &model.TableInfo{
		TableName: model.TableName{
			Schema: msg.Database,
			Table:  msg.Table,
		},
		TableInfo: &timodel.TableInfo{
			Name: timodel.NewCIStr(msg.Table),
		},
	}
	for _, col := range t.Columns {
		tiCol, err := col.ToTiColumnInfo(t.Indexes)
		if err != nil {
			return nil, err
		}
		info.Columns = append(info.Columns, tiCol)
	}
	for _, idx := range t.Indexes {
		tiIdx, err := idx.ToTiIndexInfo()
		if err != nil {
			return nil, err
		}
		info.Indices = append(info.Indices, tiIdx)
	}

	return info, nil
}

// ToDDLEvent converts from message to DDLEvent.
func (t *TableSchema) ToDDLEvent(msg *message) (*model.DDLEvent, error) {
	info, err := t.ToTableInfo(msg)
	if err != nil {
		return nil, err
	}
	return &model.DDLEvent{
		StartTs:   msg.CommitTs,
		CommitTs:  msg.CommitTs,
		TableInfo: info,
		Query:     msg.SQL,
	}, nil
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
	var tableSchema TableSchema
	tableSchema.FromTableInfo(ddl.TableInfo)

	msg := &message{
		Version:     defaultVersion,
		CommitTs:    ddl.CommitTs,
		Database:    ddl.TableInfo.TableName.Schema,
		Table:       ddl.TableInfo.TableName.Table,
		Type:        DDLType,
		SQL:         ddl.Query,
		TableSchema: &tableSchema,
	}
	return msg
}

func newBootstrapMessage(ddl *model.DDLEvent) *message {
	var tableSchema TableSchema
	tableSchema.FromTableInfo(ddl.TableInfo)

	return &message{
		Version:     defaultVersion,
		CommitTs:    ddl.CommitTs,
		Database:    ddl.TableInfo.TableName.Schema,
		Table:       ddl.TableInfo.TableName.Table,
		Type:        BootstrapType,
		TableSchema: &tableSchema,
	}
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
