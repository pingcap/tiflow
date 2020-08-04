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

	TableInfoVersion uint64 `json:"table-info-version,omitempty"`

	// if the table of this row only has one unique index(includes primary key),
	// IndieMarkCol will be set to the name of the unique index
	IndieMarkCol string             `json:"indie-mark-col"`
	Columns      map[string]*Column `json:"columns"`
	PreColumns   map[string]*Column `json:"pre-columns"`
	Keys         []string           `json:"keys"`
}

// Column represents a column value in row changed event
type Column struct {
	Type byte `json:"t"`
	// WhereHandle is deprecation
	// WhereHandle is replaced by HandleKey in Flag
	WhereHandle *bool          `json:"h,omitempty"`
	Flag        ColumnFlagType `json:"f"`
	Value       interface{}    `json:"v"`
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
	Table      string
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
func (e *DDLEvent) FromJob(job *model.Job, preTableInfo *TableInfo) {
	e.TableInfo = new(SimpleTableInfo)
	e.TableInfo.Schema = job.SchemaName
	e.StartTs = job.StartTS
	e.CommitTs = job.BinlogInfo.FinishedTS
	e.Query = job.Query
	e.Type = job.Type

	if job.BinlogInfo.TableInfo != nil {
		tableName := job.BinlogInfo.TableInfo.Name.O
		tableInfo := job.BinlogInfo.TableInfo
		e.TableInfo.ColumnInfo = make([]*ColumnInfo, len(tableInfo.Columns))

		for i, colInfo := range tableInfo.Columns {
			e.TableInfo.ColumnInfo[i] = new(ColumnInfo)
			e.TableInfo.ColumnInfo[i].FromTiColumnInfo(colInfo)
		}

		e.TableInfo.Table = tableName
	}
	e.fillPreTableInfo(preTableInfo)
}

func (e *DDLEvent) fillPreTableInfo(preTableInfo *TableInfo) {
	if preTableInfo == nil {
		return
	}
	e.PreTableInfo = new(SimpleTableInfo)
	e.PreTableInfo.Schema = preTableInfo.TableName.Schema
	e.PreTableInfo.Table = preTableInfo.TableName.Table

	e.PreTableInfo.ColumnInfo = make([]*ColumnInfo, len(preTableInfo.Columns))
	for i, colInfo := range preTableInfo.Columns {
		e.PreTableInfo.ColumnInfo[i] = new(ColumnInfo)
		e.PreTableInfo.ColumnInfo[i].FromTiColumnInfo(colInfo)
	}
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
