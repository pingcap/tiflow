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

package model

import (
	"fmt"

	bitflag "github.com/mvpninjas/go-bitflag"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"
)

// MqMessageType is the type of message
type MqMessageType int

const (
	// MqMessageTypeUnknow is unknown type of message key
	MqMessageTypeUnknow MqMessageType = iota
	// MqMessageTypeRow is row type of message key
	MqMessageTypeRow
	// MqMessageTypeDDL is ddl type of message key
	MqMessageTypeDDL
	// MqMessageTypeResolved is resolved type of message key
	MqMessageTypeResolved
)

const (
	// BinaryFlag means col charset is binary
	BinaryFlag bitflag.Flag = 1 << bitflag.Flag(iota)
)

// TableName represents name of a table, includes table name and schema name.
type TableName struct {
	Schema    string `toml:"db-name" json:"db-name"`
	Table     string `toml:"tbl-name" json:"tbl-name"`
	Partition int64  `json:"partition"`
}

// String implements fmt.Stringer interface.
func (t TableName) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

// QuoteString returns quoted full table name
func (t TableName) QuoteString() string {
	return QuoteSchema(t.Schema, t.Table)
}

// GetSchema returns schema name.
func (t *TableName) GetSchema() string {
	return t.Schema
}

// GetTable returns table name.
func (t *TableName) GetTable() string {
	return t.Table
}

// RowChangedEvent represents a row changed event
type RowChangedEvent struct {
	StartTs  uint64 `json:"start-ts"`
	CommitTs uint64 `json:"commit-ts"`

	RowID int64 `json:"row-id"`

	Table *TableName `json:"table"`

	Delete bool `json:"delete"`

	SchemaID int64 `json:"schema-id,omitempty"`

	TableUpdateTs uint64 `json:"table-update-ts,omitempty"`

	// if the table of this row only has one unique index(includes primary key),
	// IndieMarkCol will be set to the name of the unique index
	IndieMarkCol string             `json:"indie-mark-col"`
	Columns      map[string]*Column `json:"columns"`
	Keys         []string           `json:"keys"`
}

// Column represents a column value in row changed event
type Column struct {
	Type        byte         `json:"t"`
	WhereHandle *bool        `json:"h,omitempty"`
	Flag        bitflag.Flag `json:"f"`
	Value       interface{}  `json:"v"`
}

// ColumnInfo represents the name and type information passed to the sink
type ColumnInfo struct {
	Name string
	Type byte
}

// FromTiColumnInfo populates cdc's ColumnInfo from TiDB's model.ColumnInfo
func (c *ColumnInfo) FromTiColumnInfo(tiColumnInfo *model.ColumnInfo) {
	c.Type = tiColumnInfo.Tp
	c.Name = tiColumnInfo.Name.String()
}

// TableInfo is the simplified table info passed to the sink
type TableInfo struct {
	// db name
	Schema string
	// table name
	Table string
	// unique identifier for the current table schema.
	UpdateTs   uint64
	ColumnInfo []*ColumnInfo
}

// DDLEvent represents a DDL event
type DDLEvent struct {
	StartTs   uint64
	CommitTs  uint64
	Schema    string
	Table     string
	TableInfo *TableInfo
	Query     string
	Type      model.ActionType
}

// FromJob fills the values of DDLEvent from DDL job
func (e *DDLEvent) FromJob(job *model.Job) {
	if job.BinlogInfo.TableInfo != nil {
		tableName := job.BinlogInfo.TableInfo.Name.String()
		tableInfo := job.BinlogInfo.TableInfo
		e.TableInfo = new(TableInfo)
		e.TableInfo.ColumnInfo = make([]*ColumnInfo, len(tableInfo.Columns))

		for i, colInfo := range tableInfo.Columns {
			e.TableInfo.ColumnInfo[i] = new(ColumnInfo)
			e.TableInfo.ColumnInfo[i].FromTiColumnInfo(colInfo)
		}

		e.TableInfo.Schema = job.SchemaName
		e.TableInfo.Table = tableName
		e.TableInfo.UpdateTs = tableInfo.UpdateTS
		e.Table = tableName
	}
	e.StartTs = job.StartTS
	e.CommitTs = job.BinlogInfo.FinishedTS
	e.Query = job.Query
	e.Schema = job.SchemaName
	e.Type = job.Type
}

// Txn represents a transaction which includes many row events
type Txn struct {
	StartTs   uint64
	CommitTs  uint64
	Rows      []*RowChangedEvent
	Keys      []string
	ReplicaID uint64
}

// Append adds a row changed event into Txn
func (t *Txn) Append(row *RowChangedEvent) {
	if row.StartTs != t.StartTs || row.CommitTs != t.CommitTs {
		log.Fatal("unexpected row change event",
			zap.Uint64("startTs of txn", t.StartTs),
			zap.Uint64("commitTs of txn", t.CommitTs),
			zap.Uint64("startTs of row", row.StartTs),
			zap.Uint64("commitTs of row", row.CommitTs))
	}
	t.Rows = append(t.Rows, row)
	if len(row.Keys) == 0 {
		if len(t.Keys) == 0 {
			t.Keys = []string{QuoteSchema(row.Table.Schema, row.Table.Table)}
		}
	} else {
		t.Keys = append(t.Keys, row.Keys...)
	}
}
