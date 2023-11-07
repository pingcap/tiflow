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
	"sort"
	"strings"

	"github.com/pingcap/tidb/parser/charset"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
)

const (
	defaultVersion = 0
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

type Column struct {
	Name     string      `json:"name"`
	DataType string      `json:"dataType"`
	Nullable string      `json:"nullable"`
	Default  interface{} `json:"default,omitempty"`
}

// FromTiColumnInfo converts from TiDB ColumnInfo to TableCol.
func (c *Column) FromTiColumnInfo(col *timodel.ColumnInfo, outputColumnID bool) {
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
func (c *Column) ToTiColumnInfo(indexes []*Index) (*timodel.ColumnInfo, error) {
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

	var primaryKey *Index
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

type Index struct {
	Name     string   `json:"name"`
	Unique   string   `json:"unique,omitempty"`
	Primary  string   `json:"primary,omitempty"`
	Nullable string   `json:"nullable,omitempty"`
	Columns  []string `json:"columns"`
}

// FromTiIndexInfo converts from TiDB IndexInfo to Index.
func (i *Index) FromTiIndexInfo(index *timodel.IndexInfo, tableInfo *timodel.TableInfo) {
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
func (i *Index) ToTiIndexInfo() (*timodel.IndexInfo, error) {
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

type TableSchema struct {
	Columns []*Column `json:"columns"`
	Indexes []*Index  `json:"indexes"`
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

	columns := make([]*Column, 0, len(tiColumns))
	for _, col := range tiColumns {
		column := &Column{}
		column.FromTiColumnInfo(col, false)
		columns = append(columns, column)
	}

	indexes := make([]*Index, 0, len(tableInfo.Indices))
	for _, idx := range tableInfo.Indices {
		index := &Index{}
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
	// PreSchema and PreTable is only for rename table ddl event.
	PreSchema string `json:"preSchema,omitempty"`
	PreTable  string `json:"preTable,omitempty"`
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
	if ddl.Type == timodel.ActionRenameTable {
		msg.PreSchema = ddl.PreTableInfo.TableName.Schema
		msg.PreTable = ddl.PreTableInfo.TableName.Table
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
