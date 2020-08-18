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
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/pkg/util"
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

// ColumnFlagType is for encapsulating the flag operations for different flags.
type ColumnFlagType util.Flag

const (
	// BinaryFlag means the column charset is binary
	BinaryFlag ColumnFlagType = 1 << ColumnFlagType(iota)
	// HandleKeyFlag means the column is selected as the handle key
	HandleKeyFlag
	// GeneratedColumnFlag means the column is a generated column
	GeneratedColumnFlag
	// PrimaryKeyFlag means the column is primary key
	PrimaryKeyFlag
	// UniqueKeyFlag means the column is unique key
	UniqueKeyFlag
	// MultipleKeyFlag means the column is multiple key
	MultipleKeyFlag
	// NullableFlag means the column is nullable
	NullableFlag
)

//SetIsBinary set BinaryFlag
func (b *ColumnFlagType) SetIsBinary() {
	(*util.Flag)(b).Add(util.Flag(BinaryFlag))
}

//UnsetIsBinary unset BinaryFlag
func (b *ColumnFlagType) UnsetIsBinary() {
	(*util.Flag)(b).Remove(util.Flag(BinaryFlag))
}

//IsBinary show whether BinaryFlag is set
func (b *ColumnFlagType) IsBinary() bool {
	return (*util.Flag)(b).HasAll(util.Flag(BinaryFlag))
}

//SetIsHandleKey set HandleKey
func (b *ColumnFlagType) SetIsHandleKey() {
	(*util.Flag)(b).Add(util.Flag(HandleKeyFlag))
}

//UnsetIsHandleKey unset HandleKey
func (b *ColumnFlagType) UnsetIsHandleKey() {
	(*util.Flag)(b).Remove(util.Flag(HandleKeyFlag))
}

//IsHandleKey show whether HandleKey is set
func (b *ColumnFlagType) IsHandleKey() bool {
	return (*util.Flag)(b).HasAll(util.Flag(HandleKeyFlag))
}

//SetIsGeneratedColumn set GeneratedColumn
func (b *ColumnFlagType) SetIsGeneratedColumn() {
	(*util.Flag)(b).Add(util.Flag(GeneratedColumnFlag))
}

//UnsetIsGeneratedColumn unset GeneratedColumn
func (b *ColumnFlagType) UnsetIsGeneratedColumn() {
	(*util.Flag)(b).Remove(util.Flag(GeneratedColumnFlag))
}

//IsGeneratedColumn show whether GeneratedColumn is set
func (b *ColumnFlagType) IsGeneratedColumn() bool {
	return (*util.Flag)(b).HasAll(util.Flag(GeneratedColumnFlag))
}

//SetIsPrimaryKey set PrimaryKeyFlag
func (b *ColumnFlagType) SetIsPrimaryKey() {
	(*util.Flag)(b).Add(util.Flag(PrimaryKeyFlag))
}

//UnsetIsPrimaryKey unset PrimaryKeyFlag
func (b *ColumnFlagType) UnsetIsPrimaryKey() {
	(*util.Flag)(b).Remove(util.Flag(PrimaryKeyFlag))
}

//IsPrimaryKey show whether PrimaryKeyFlag is set
func (b *ColumnFlagType) IsPrimaryKey() bool {
	return (*util.Flag)(b).HasAll(util.Flag(PrimaryKeyFlag))
}

//SetIsUniqueKey set UniqueKeyFlag
func (b *ColumnFlagType) SetIsUniqueKey() {
	(*util.Flag)(b).Add(util.Flag(UniqueKeyFlag))
}

//UnsetIsUniqueKey unset UniqueKeyFlag
func (b *ColumnFlagType) UnsetIsUniqueKey() {
	(*util.Flag)(b).Remove(util.Flag(UniqueKeyFlag))
}

//IsUniqueKey show whether UniqueKeyFlag is set
func (b *ColumnFlagType) IsUniqueKey() bool {
	return (*util.Flag)(b).HasAll(util.Flag(UniqueKeyFlag))
}

//IsMultipleKey show whether MultipleKeyFlag is set
func (b *ColumnFlagType) IsMultipleKey() bool {
	return (*util.Flag)(b).HasAll(util.Flag(MultipleKeyFlag))
}

//SetIsMultipleKey set MultipleKeyFlag
func (b *ColumnFlagType) SetIsMultipleKey() {
	(*util.Flag)(b).Add(util.Flag(MultipleKeyFlag))
}

//UnsetIsMultipleKey unset MultipleKeyFlag
func (b *ColumnFlagType) UnsetIsMultipleKey() {
	(*util.Flag)(b).Remove(util.Flag(MultipleKeyFlag))
}

//IsNullable show whether NullableFlag is set
func (b *ColumnFlagType) IsNullable() bool {
	return (*util.Flag)(b).HasAll(util.Flag(NullableFlag))
}

//SetIsNullable set NullableFlag
func (b *ColumnFlagType) SetIsNullable() {
	(*util.Flag)(b).Add(util.Flag(NullableFlag))
}

//UnsetIsNullable unset NullableFlag
func (b *ColumnFlagType) UnsetIsNullable() {
	(*util.Flag)(b).Remove(util.Flag(NullableFlag))
}

// TableName represents name of a table, includes table name and schema name.
type TableName struct {
	Schema      string `toml:"db-name" json:"db-name"`
	Table       string `toml:"tbl-name" json:"tbl-name"`
	TableID     int64  `toml:"tbl-id" json:"tbl-"`
	IsPartition bool   `toml:"is-partition" json:"is-partition"`
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

// GetTableID returns table ID.
func (t *TableName) GetTableID() int64 {
	return t.TableID
}

// RowChangedEvent represents a row changed event
type RowChangedEvent struct {
	StartTs  uint64 `json:"start-ts"`
	CommitTs uint64 `json:"commit-ts"`

	RowID int64 `json:"row-id"`

	Table *TableName `json:"table"`

	TableInfoVersion uint64 `json:"table-info-version,omitempty"`

	Columns      []*Column `json:"columns"`
	PreColumns   []*Column `json:"pre-columns"`
	IndexColumns [][]int

	// approximate size of this event, calculate by tikv proto bytes size
	ApproximateSize int64
}

// IsDelete returns true if the row is a delete event
func (r *RowChangedEvent) IsDelete() bool {
	return len(r.PreColumns) != 0 && len(r.Columns) == 0
}

// Column represents a column value in row changed event
type Column struct {
	Name  string         `json:"name"`
	Type  byte           `json:"type"`
	Flag  ColumnFlagType `json:"flag"`
	Value interface{}    `json:"value"`
}

// ColumnValueString returns the string representation of the column value
func ColumnValueString(c interface{}) string {
	var data string
	switch v := c.(type) {
	case nil:
		data = "null"
	case bool:
		if v {
			data = "1"
		} else {
			data = "0"
		}
	case int:
		data = strconv.FormatInt(int64(v), 10)
	case int8:
		data = strconv.FormatInt(int64(v), 10)
	case int16:
		data = strconv.FormatInt(int64(v), 10)
	case int32:
		data = strconv.FormatInt(int64(v), 10)
	case int64:
		data = strconv.FormatInt(int64(v), 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(uint64(v), 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(float64(v), 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}
	return data
}

// ColumnInfo represents the name and type information passed to the sink
type ColumnInfo struct {
	Name string
	Type byte
}

// FromTiColumnInfo populates cdc's ColumnInfo from TiDB's model.ColumnInfo
func (c *ColumnInfo) FromTiColumnInfo(tiColumnInfo *model.ColumnInfo) {
	c.Type = tiColumnInfo.Tp
	c.Name = tiColumnInfo.Name.O
}

// SimpleTableInfo is the simplified table info passed to the sink
type SimpleTableInfo struct {
	// db name
	Schema string
	// table name
	Table string
	// table ID
	TableID    int64
	ColumnInfo []*ColumnInfo
}

// DDLEvent represents a DDL event
type DDLEvent struct {
	StartTs      uint64
	CommitTs     uint64
	TableInfo    *SimpleTableInfo
	PreTableInfo *SimpleTableInfo
	Query        string
	Type         model.ActionType
}

// FromJob fills the values of DDLEvent from DDL job
func (d *DDLEvent) FromJob(job *model.Job, preTableInfo *TableInfo) {
	d.TableInfo = new(SimpleTableInfo)
	d.TableInfo.Schema = job.SchemaName
	d.StartTs = job.StartTS
	d.CommitTs = job.BinlogInfo.FinishedTS
	d.Query = job.Query
	d.Type = job.Type

	if job.BinlogInfo.TableInfo != nil {
		tableName := job.BinlogInfo.TableInfo.Name.O
		tableInfo := job.BinlogInfo.TableInfo
		d.TableInfo.ColumnInfo = make([]*ColumnInfo, len(tableInfo.Columns))

		for i, colInfo := range tableInfo.Columns {
			d.TableInfo.ColumnInfo[i] = new(ColumnInfo)
			d.TableInfo.ColumnInfo[i].FromTiColumnInfo(colInfo)
		}

		d.TableInfo.Table = tableName
		d.TableInfo.TableID = job.TableID
	}
	d.fillPreTableInfo(preTableInfo)
}

func (d *DDLEvent) fillPreTableInfo(preTableInfo *TableInfo) {
	if preTableInfo == nil {
		return
	}
	d.PreTableInfo = new(SimpleTableInfo)
	d.PreTableInfo.Schema = preTableInfo.TableName.Schema
	d.PreTableInfo.Table = preTableInfo.TableName.Table
	d.PreTableInfo.TableID = preTableInfo.ID

	d.PreTableInfo.ColumnInfo = make([]*ColumnInfo, len(preTableInfo.Columns))
	for i, colInfo := range preTableInfo.Columns {
		d.PreTableInfo.ColumnInfo[i] = new(ColumnInfo)
		d.PreTableInfo.ColumnInfo[i].FromTiColumnInfo(colInfo)
	}
}

// SingleTableTxn represents a transaction which includes many row events in a single table
type SingleTableTxn struct {
	Table     *TableName
	StartTs   uint64
	CommitTs  uint64
	Rows      []*RowChangedEvent
	ReplicaID uint64
}

// Append adds a row changed event into SingleTableTxn
func (t *SingleTableTxn) Append(row *RowChangedEvent) {
	if row.StartTs != t.StartTs || row.CommitTs != t.CommitTs || row.Table.TableID != t.Table.TableID {
		log.Fatal("unexpected row change event",
			zap.Uint64("startTs of txn", t.StartTs),
			zap.Uint64("commitTs of txn", t.CommitTs),
			zap.Any("table of txn", t.Table),
			zap.Any("row", row))
	}
	t.Rows = append(t.Rows, row)
}
