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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

//go:generate msgp

// MessageType is the type of message, which is used by MqSink and RedoLog.
type MessageType int

const (
	// MessageTypeUnknown is unknown type of message key
	MessageTypeUnknown MessageType = iota
	// MessageTypeRow is row type of message key
	MessageTypeRow
	// MessageTypeDDL is ddl type of message key
	MessageTypeDDL
	// MessageTypeResolved is resolved type of message key
	MessageTypeResolved
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
	// UnsignedFlag means the column stores an unsigned integer
	UnsignedFlag
)

// SetIsBinary sets BinaryFlag
func (b *ColumnFlagType) SetIsBinary() {
	(*util.Flag)(b).Add(util.Flag(BinaryFlag))
}

// UnsetIsBinary unsets BinaryFlag
func (b *ColumnFlagType) UnsetIsBinary() {
	(*util.Flag)(b).Remove(util.Flag(BinaryFlag))
}

// IsBinary shows whether BinaryFlag is set
func (b *ColumnFlagType) IsBinary() bool {
	return (*util.Flag)(b).HasAll(util.Flag(BinaryFlag))
}

// SetIsHandleKey sets HandleKey
func (b *ColumnFlagType) SetIsHandleKey() {
	(*util.Flag)(b).Add(util.Flag(HandleKeyFlag))
}

// UnsetIsHandleKey unsets HandleKey
func (b *ColumnFlagType) UnsetIsHandleKey() {
	(*util.Flag)(b).Remove(util.Flag(HandleKeyFlag))
}

// IsHandleKey shows whether HandleKey is set
func (b *ColumnFlagType) IsHandleKey() bool {
	return (*util.Flag)(b).HasAll(util.Flag(HandleKeyFlag))
}

// SetIsGeneratedColumn sets GeneratedColumn
func (b *ColumnFlagType) SetIsGeneratedColumn() {
	(*util.Flag)(b).Add(util.Flag(GeneratedColumnFlag))
}

// UnsetIsGeneratedColumn unsets GeneratedColumn
func (b *ColumnFlagType) UnsetIsGeneratedColumn() {
	(*util.Flag)(b).Remove(util.Flag(GeneratedColumnFlag))
}

// IsGeneratedColumn shows whether GeneratedColumn is set
func (b *ColumnFlagType) IsGeneratedColumn() bool {
	return (*util.Flag)(b).HasAll(util.Flag(GeneratedColumnFlag))
}

// SetIsPrimaryKey sets PrimaryKeyFlag
func (b *ColumnFlagType) SetIsPrimaryKey() {
	(*util.Flag)(b).Add(util.Flag(PrimaryKeyFlag))
}

// UnsetIsPrimaryKey unsets PrimaryKeyFlag
func (b *ColumnFlagType) UnsetIsPrimaryKey() {
	(*util.Flag)(b).Remove(util.Flag(PrimaryKeyFlag))
}

// IsPrimaryKey shows whether PrimaryKeyFlag is set
func (b *ColumnFlagType) IsPrimaryKey() bool {
	return (*util.Flag)(b).HasAll(util.Flag(PrimaryKeyFlag))
}

// SetIsUniqueKey sets UniqueKeyFlag
func (b *ColumnFlagType) SetIsUniqueKey() {
	(*util.Flag)(b).Add(util.Flag(UniqueKeyFlag))
}

// UnsetIsUniqueKey unsets UniqueKeyFlag
func (b *ColumnFlagType) UnsetIsUniqueKey() {
	(*util.Flag)(b).Remove(util.Flag(UniqueKeyFlag))
}

// IsUniqueKey shows whether UniqueKeyFlag is set
func (b *ColumnFlagType) IsUniqueKey() bool {
	return (*util.Flag)(b).HasAll(util.Flag(UniqueKeyFlag))
}

// IsMultipleKey shows whether MultipleKeyFlag is set
func (b *ColumnFlagType) IsMultipleKey() bool {
	return (*util.Flag)(b).HasAll(util.Flag(MultipleKeyFlag))
}

// SetIsMultipleKey sets MultipleKeyFlag
func (b *ColumnFlagType) SetIsMultipleKey() {
	(*util.Flag)(b).Add(util.Flag(MultipleKeyFlag))
}

// UnsetIsMultipleKey unsets MultipleKeyFlag
func (b *ColumnFlagType) UnsetIsMultipleKey() {
	(*util.Flag)(b).Remove(util.Flag(MultipleKeyFlag))
}

// IsNullable shows whether NullableFlag is set
func (b *ColumnFlagType) IsNullable() bool {
	return (*util.Flag)(b).HasAll(util.Flag(NullableFlag))
}

// SetIsNullable sets NullableFlag
func (b *ColumnFlagType) SetIsNullable() {
	(*util.Flag)(b).Add(util.Flag(NullableFlag))
}

// UnsetIsNullable unsets NullableFlag
func (b *ColumnFlagType) UnsetIsNullable() {
	(*util.Flag)(b).Remove(util.Flag(NullableFlag))
}

// IsUnsigned shows whether UnsignedFlag is set
func (b *ColumnFlagType) IsUnsigned() bool {
	return (*util.Flag)(b).HasAll(util.Flag(UnsignedFlag))
}

// SetIsUnsigned sets UnsignedFlag
func (b *ColumnFlagType) SetIsUnsigned() {
	(*util.Flag)(b).Add(util.Flag(UnsignedFlag))
}

// UnsetIsUnsigned unsets UnsignedFlag
func (b *ColumnFlagType) UnsetIsUnsigned() {
	(*util.Flag)(b).Remove(util.Flag(UnsignedFlag))
}

// TableName represents name of a table, includes table name and schema name.
type TableName struct {
	Schema      string `toml:"db-name" json:"db-name" msg:"db-name"`
	Table       string `toml:"tbl-name" json:"tbl-name" msg:"tbl-name"`
	TableID     int64  `toml:"tbl-id" json:"tbl-id" msg:"tbl-id"`
	IsPartition bool   `toml:"is-partition" json:"is-partition" msg:"is-partition"`
}

// String implements fmt.Stringer interface.
func (t TableName) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

// QuoteString returns quoted full table name
func (t TableName) QuoteString() string {
	return quotes.QuoteSchema(t.Schema, t.Table)
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

// RedoLogType is the type of log
type RedoLogType int

const (
	// RedoLogTypeUnknown is unknown type of log
	RedoLogTypeUnknown RedoLogType = iota
	// RedoLogTypeRow is row type of log
	RedoLogTypeRow
	// RedoLogTypeDDL is ddl type of log
	RedoLogTypeDDL
)

// RedoLog defines the persistent structure of redo log
// since MsgPack do not support types that are defined in another package,
// more info https://github.com/tinylib/msgp/issues/158, https://github.com/tinylib/msgp/issues/149
// so define a RedoColumn, RedoDDLEvent instead of using the Column, DDLEvent
type RedoLog struct {
	RedoRow RedoRowChangedEvent `msg:"row"`
	RedoDDL RedoDDLEvent        `msg:"ddl"`
	Type    RedoLogType         `msg:"type"`
}

// RedoRowChangedEvent represents the DML event used in RedoLog
type RedoRowChangedEvent struct {
	Row        *RowChangedEvent `msg:"row"`
	Columns    []RedoColumn     `msg:"columns"`
	PreColumns []RedoColumn     `msg:"pre-columns"`
}

// RowChangedEvent represents a row changed event
type RowChangedEvent struct {
	/*********** Immutable fields **********/
	Table        *TableName `json:"table" msg:"table"`
	Columns      []*Column  `json:"columns" msg:"columns"`
	PreColumns   []*Column  `json:"pre-columns" msg:"pre-columns"`
	IndexColumns [][]int    `json:"-" msg:"index-columns"`

	TableInfo *TableInfo         `json:"-" msg:"-"`
	ColInfos  []rowcodec.ColInfo `json:"column-infos" msg:"-"`

	/*********** Mutable fields **********/
	StartTs         uint64        `json:"start-ts" msg:"start-ts"`
	CommitTs        uint64        `json:"commit-ts" msg:"commit-ts"`
	RowID           int64         `json:"row-id" msg:"-"` // Deprecated. It is empty when the RowID comes from clustered index table.
	ColumnValues    []ColumnValue `json:"column-values" msg: "column-values"`
	PreColumnValues []ColumnValue `json:"pre-column-values" msg: "pre-column-values"`

	// ApproximateDataSize is the approximate size of protobuf binary
	// representation of this event, without any meta information.
	ApproximateDataSize int64 `json:"-" msg:"-"`

	// ApproximateMemSize is the approximate size of memory representation
	// of this event, including all meta information.
	ApproximateMemSize int `json:"-" msg:"-"`

	// SplitTxn marks this RowChangedEvent as the first line of a new txn.
	SplitTxn bool `json:"-" msg:"-"`

	// ReplicatingTs is ts when a table starts replicating events to downstream.
	ReplicatingTs Ts `json:"-" msg:"-"`
}

// GetCommitTs returns the commit timestamp of this event.
func (r *RowChangedEvent) GetCommitTs() uint64 {
	return r.CommitTs
}

// IsDelete returns true if the row is a delete event
func (r *RowChangedEvent) IsDelete() bool {
	return len(r.PreColumns) != 0 && len(r.Columns) == 0
}

// IsInsert returns true if the row is an insert event
func (r *RowChangedEvent) IsInsert() bool {
	return len(r.PreColumns) == 0 && len(r.Columns) != 0
}

// IsUpdate returns true if the row is an update event
func (r *RowChangedEvent) IsUpdate() bool {
	return len(r.PreColumns) != 0 && len(r.Columns) != 0
}

// PrimaryKeyColumnNames return all primary key's name
func (r *RowChangedEvent) PrimaryKeyColumnNames() []string {
	var result []string

	var cols []*Column
	if r.IsDelete() {
		cols = r.PreColumns
	} else {
		cols = r.Columns
	}

	result = make([]string, 0)
	for _, col := range cols {
		if col != nil && col.Flag.IsPrimaryKey() {
			result = append(result, col.Name)
		}
	}
	return result
}

// PrimaryKeyColumns returns the column(s) corresponding to the handle key(s)
func (r *RowChangedEvent) PrimaryKeyColumns() []*Column {
	pkeyCols := make([]*Column, 0)

	var cols []*Column
	if r.IsDelete() {
		cols = r.PreColumns
	} else {
		cols = r.Columns
	}

	for _, col := range cols {
		if col != nil && (col.Flag.IsPrimaryKey()) {
			pkeyCols = append(pkeyCols, col)
		}
	}

	// It is okay not to have primary keys, so the empty array is an acceptable result
	return pkeyCols
}

// HandleKeyColumns returns the column(s) corresponding to the handle key(s)
func (r *RowChangedEvent) HandleKeyColumns() []*Column {
	pkeyCols := make([]*Column, 0)

	var cols []*Column
	if r.IsDelete() {
		cols = r.PreColumns
	} else {
		cols = r.Columns
	}

	for _, col := range cols {
		if col != nil && col.Flag.IsHandleKey() {
			pkeyCols = append(pkeyCols, col)
		}
	}

	// It is okay not to have handle keys, so the empty array is an acceptable result
	return pkeyCols
}

// HandleKeyColInfos returns the column(s) and colInfo(s) corresponding to the handle key(s)
func (r *RowChangedEvent) HandleKeyColInfos() ([]*Column, []ColumnValue, []rowcodec.ColInfo) {
	pkeyCols := make([]*Column, 0)
	pkeyColVals := make([]ColumnValue, 0)
	pkeyColInfos := make([]rowcodec.ColInfo, 0)

	var cols []*Column
	var colvals []ColumnValue
	if r.IsDelete() {
		cols = r.PreColumns
		colvals = r.PreColumnValues
	} else {
		cols = r.Columns
		colvals = r.ColumnValues
	}

	for i, col := range cols {
		if col != nil && col.Flag.IsHandleKey() {
			pkeyCols = append(pkeyCols, col)
			pkeyColVals = append(pkeyColVals, colvals[i])
			pkeyColInfos = append(pkeyColInfos, r.ColInfos[i])
		}
	}

	// It is okay not to have handle keys, so the empty array is an acceptable result
	return pkeyCols, pkeyColVals, pkeyColInfos
}

// WithHandlePrimaryFlag set `HandleKeyFlag` and `PrimaryKeyFlag`
func (r *RowChangedEvent) WithHandlePrimaryFlag(colNames map[string]struct{}) {
	for _, col := range r.Columns {
		if _, ok := colNames[col.Name]; ok {
			col.Flag.SetIsHandleKey()
			col.Flag.SetIsPrimaryKey()
		}
	}
	for _, col := range r.PreColumns {
		if _, ok := colNames[col.Name]; ok {
			col.Flag.SetIsHandleKey()
			col.Flag.SetIsPrimaryKey()
		}
	}
}

// ApproximateBytes returns approximate bytes in memory consumed by the event.
func (r *RowChangedEvent) ApproximateBytes() int {
	return r.ApproximateMemSize
}

// Column represents a column definition in row changed event
type Column struct {
	Name    string         `json:"name" msg:"name"`
	Type    byte           `json:"type" msg:"type"`
	Charset string         `json:"charset" msg:"charset"`
	Flag    ColumnFlagType `json:"flag" msg:"-"`
	Default interface{}    `json:"default" msg:"-"`
}

// ColumnValue represents a column value in row changed event
type ColumnValue struct {
	Value interface{} `json:"value" msg:"value"`
}

// RedoColumn stores Column change
type RedoColumn struct {
	// Fields from Column and can't be marshaled directly in Column.
	Value interface{} `msg:"column"`
	// msgp transforms empty byte slice into nil, PTAL msgp#247.
	ValueIsEmptyBytes bool   `msg:"value-is-empty-bytes"`
	Flag              uint64 `msg:"flag"`
}

// BuildTiDBTableInfo builds a TiDB TableInfo from given information.
func BuildTiDBTableInfo(columns []*Column, indexColumns [][]int) *model.TableInfo {
	ret := &model.TableInfo{}
	// nowhere will use this field, so we set a debug message
	ret.Name = model.NewCIStr("BuildTiDBTableInfo")

	for i, col := range columns {
		columnInfo := &model.ColumnInfo{
			Offset: i,
			State:  model.StatePublic,
		}
		if col == nil {
			// by referring to datum2Column, nil is happened when
			// - !IsColCDCVisible, which means the column is a virtual generated
			//   column
			// - !exist && !fillWithDefaultValue, which means upstream does not
			//   send the column value
			// just mock for the first case
			columnInfo.Name = model.NewCIStr("omitted")
			columnInfo.GeneratedExprString = "pass_generated_check"
			columnInfo.GeneratedStored = false
			ret.Columns = append(ret.Columns, columnInfo)
			continue
		}
		columnInfo.Name = model.NewCIStr(col.Name)
		columnInfo.SetType(col.Type)
		// TiKV always use utf8mb4 to store, and collation is not recorded by CDC
		columnInfo.SetCharset(mysql.UTF8MB4Charset)
		columnInfo.SetCollate(mysql.UTF8MB4DefaultCollation)

		// inverse initColumnsFlag
		flag := col.Flag
		if flag.IsBinary() {
			columnInfo.SetCharset("binary")
		}
		if flag.IsGeneratedColumn() {
			// we do not use this field, so we set it to any non-empty string
			columnInfo.GeneratedExprString = "pass_generated_check"
			columnInfo.GeneratedStored = true
		}
		if flag.IsHandleKey() {
			columnInfo.AddFlag(mysql.PriKeyFlag)
			ret.IsCommonHandle = true
		} else if flag.IsPrimaryKey() {
			columnInfo.AddFlag(mysql.PriKeyFlag)
		}
		if flag.IsUniqueKey() {
			columnInfo.AddFlag(mysql.UniqueKeyFlag)
		}
		if !flag.IsNullable() {
			columnInfo.AddFlag(mysql.NotNullFlag)
		}
		if flag.IsMultipleKey() {
			columnInfo.AddFlag(mysql.MultipleKeyFlag)
		}
		if flag.IsUnsigned() {
			columnInfo.AddFlag(mysql.UnsignedFlag)
		}
		ret.Columns = append(ret.Columns, columnInfo)
	}

	for i, colOffsets := range indexColumns {
		indexInfo := &model.IndexInfo{
			Name:  model.NewCIStr(fmt.Sprintf("idx_%d", i)),
			State: model.StatePublic,
		}
		firstCol := columns[colOffsets[0]]
		if firstCol == nil {
			// when the referenced column is nil, we already have a handle index
			// so we can skip this index.
			// only happens for DELETE event and old value feature is disabled
			continue
		}
		if firstCol.Flag.IsPrimaryKey() {
			indexInfo.Primary = true
			indexInfo.Unique = true
		}
		if firstCol.Flag.IsUniqueKey() {
			indexInfo.Unique = true
		}

		for _, offset := range colOffsets {
			col := ret.Columns[offset]

			indexCol := &model.IndexColumn{}
			indexCol.Name = col.Name
			indexCol.Offset = offset
			indexInfo.Columns = append(indexInfo.Columns, indexCol)
		}

		// TODO: revert the "all column set index related flag" to "only the
		// first column set index related flag" if needed

		ret.Indices = append(ret.Indices, indexInfo)
	}
	return ret
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
		data = strconv.FormatInt(v, 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(v, 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}
	return data
}

// DDLEvent stores DDL event
type DDLEvent struct {
	StartTs      uint64           `msg:"start-ts"`
	CommitTs     uint64           `msg:"commit-ts"`
	Query        string           `msg:"query"`
	TableInfo    *TableInfo       `msg:"-"`
	PreTableInfo *TableInfo       `msg:"-"`
	Type         model.ActionType `msg:"-"`
	Done         bool             `msg:"-"`
}

// RedoDDLEvent represents DDL event used in redo log persistent
type RedoDDLEvent struct {
	DDL  *DDLEvent `msg:"ddl"`
	Type byte      `msg:"type"`
}

// FromJob fills the values with DDLEvent from DDL job
func (d *DDLEvent) FromJob(job *model.Job, preTableInfo *TableInfo, tableInfo *TableInfo) {
	// populating DDLEvent of an `rename tables` job is handled in `FromRenameTablesJob()`
	if d.Type == model.ActionRenameTables {
		return
	}

	// The query for "DROP TABLE" and "DROP VIEW" statements need
	// to be rebuilt. The reason is elaborated as follows:
	// for a DDL statement like "DROP TABLE test1.table1, test2.table2",
	// two DDL jobs will be generated. These two jobs can be differentiated
	// from job.BinlogInfo.TableInfo whereas the job.Query are identical.
	rebuildQuery := func() {
		switch d.Type {
		case model.ActionDropTable:
			d.Query = fmt.Sprintf("DROP TABLE `%s`.`%s`", d.TableInfo.TableName.Schema, d.TableInfo.TableName.Table)
		case model.ActionDropView:
			d.Query = fmt.Sprintf("DROP VIEW `%s`.`%s`", d.TableInfo.TableName.Schema, d.TableInfo.TableName.Table)
		default:
			d.Query = job.Query
		}
	}

	d.StartTs = job.StartTS
	d.CommitTs = job.BinlogInfo.FinishedTS
	d.Type = job.Type
	d.PreTableInfo = preTableInfo
	d.TableInfo = tableInfo
	// rebuild the query if necessary
	rebuildQuery()
}

// FromRenameTablesJob fills the values of DDLEvent from a rename tables DDL job
func (d *DDLEvent) FromRenameTablesJob(job *model.Job,
	oldSchemaName, newSchemaName string,
	preTableInfo *TableInfo, tableInfo *TableInfo,
) {
	if job.Type != model.ActionRenameTables {
		return
	}

	d.StartTs = job.StartTS
	d.CommitTs = job.BinlogInfo.FinishedTS
	oldTableName := preTableInfo.Name.O
	newTableName := tableInfo.Name.O
	d.Query = fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`",
		oldSchemaName, oldTableName, newSchemaName, newTableName)
	d.Type = model.ActionRenameTable
	d.PreTableInfo = preTableInfo
	d.TableInfo = tableInfo
}

// SingleTableTxn represents a transaction which includes many row events in a single table
//
//msgp:ignore SingleTableTxn
type SingleTableTxn struct {
	// data fields of SingleTableTxn
	Table     *TableName
	TableInfo *TableInfo
	StartTs   uint64
	CommitTs  uint64
	Rows      []*RowChangedEvent

	// control fields of SingleTableTxn
	// FinishWg is a barrier txn, after this txn is received, the worker must
	// flush cached txns and call FinishWg.Done() to mark txns have been flushed.
	FinishWg *sync.WaitGroup
}

// GetCommitTs returns the commit timestamp of the transaction.
func (t *SingleTableTxn) GetCommitTs() uint64 {
	return t.CommitTs
}

// Append adds a row changed event into SingleTableTxn
func (t *SingleTableTxn) Append(row *RowChangedEvent) {
	if row.StartTs != t.StartTs || row.CommitTs != t.CommitTs || row.Table.TableID != t.Table.TableID {
		log.Panic("unexpected row change event",
			zap.Uint64("startTs", t.StartTs),
			zap.Uint64("commitTs", t.CommitTs),
			zap.Any("table", t.Table),
			zap.Any("row", row))
	}
	t.Rows = append(t.Rows, row)
}

// ToWaitFlush indicates whether to wait flushing after the txn is processed or not.
func (t *SingleTableTxn) ToWaitFlush() bool {
	return t.FinishWg != nil
}

// BoundedColumn is a combination of Column and ColumnValue. Generally used in tests.
type BoundedColumn struct {
	Name    string
	Type    byte
	Charset string
	Flag    ColumnFlagType
	Default interface{}
	Value   interface{}
}
