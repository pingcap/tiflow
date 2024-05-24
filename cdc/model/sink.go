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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/sink"
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

const (
	// the RowChangedEvent order in the same transaction
	typeDelete = iota + 1
	typeUpdate
	typeInsert
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
	// ZerofillFlag means the column is zerofill
	ZerofillFlag
)

// SetZeroFill sets ZerofillFlag
func (b *ColumnFlagType) SetZeroFill() {
	(*util.Flag)(b).Add(util.Flag(ZerofillFlag))
}

// IsZerofill shows whether ZerofillFlag is set
func (b *ColumnFlagType) IsZerofill() bool {
	return (*util.Flag)(b).HasAll(util.Flag(ZerofillFlag))
}

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
	RedoRow *RedoRowChangedEvent `msg:"row"`
	RedoDDL *RedoDDLEvent        `msg:"ddl"`
	Type    RedoLogType          `msg:"type"`
}

// GetCommitTs returns the commit ts of the redo log.
func (r *RedoLog) GetCommitTs() Ts {
	switch r.Type {
	case RedoLogTypeRow:
		return r.RedoRow.Row.CommitTs
	case RedoLogTypeDDL:
		return r.RedoDDL.DDL.CommitTs
	default:
		log.Panic("invalid redo log type", zap.Any("type", r.Type))
	}
	return 0
}

// TrySplitAndSortUpdateEvent redo log do nothing
func (r *RedoLog) TrySplitAndSortUpdateEvent(sinkScheme string) error {
	return nil
}

// RedoRowChangedEvent represents the DML event used in RedoLog
type RedoRowChangedEvent struct {
	Row        *RowChangedEvent `msg:"row"`
	PreColumns []*RedoColumn    `msg:"pre-columns"`
	Columns    []*RedoColumn    `msg:"columns"`
}

// RedoDDLEvent represents DDL event used in redo log persistent
type RedoDDLEvent struct {
	DDL       *DDLEvent `msg:"ddl"`
	Type      byte      `msg:"type"`
	TableName TableName `msg:"table-name"`
}

// ToRedoLog converts row changed event to redo log
func (row *RowChangedEvent) ToRedoLog() *RedoLog {
	return &RedoLog{RedoRow: RowToRedo(row), Type: RedoLogTypeRow}
}

// ToRedoLog converts ddl event to redo log
func (ddl *DDLEvent) ToRedoLog() *RedoLog {
	return &RedoLog{RedoDDL: DDLToRedo(ddl), Type: RedoLogTypeDDL}
}

// RowChangedEvent represents a row changed event
type RowChangedEvent struct {
	StartTs  uint64 `json:"start-ts" msg:"start-ts"`
	CommitTs uint64 `json:"commit-ts" msg:"commit-ts"`

	RowID int64 `json:"row-id" msg:"-"` // Deprecated. It is empty when the RowID comes from clustered index table.

	// Table contains the table name and table ID.
	// NOTICE: We store the physical table ID here, not the logical table ID.
	Table    *TableName         `json:"table" msg:"table"`
	ColInfos []rowcodec.ColInfo `json:"column-infos" msg:"-"`
	// NOTICE: We probably store the logical ID inside TableInfo's TableName,
	// not the physical ID.
	// For normal table, there is only one ID, which is the physical ID.
	// AKA TIDB_TABLE_ID.
	// For partitioned table, there are two kinds of ID:
	// 1. TIDB_PARTITION_ID is the physical ID of the partition.
	// 2. TIDB_TABLE_ID is the logical ID of the table.
	// In general, we always use the physical ID to represent a table, but we
	// record the logical ID from the DDL event(job.BinlogInfo.TableInfo).
	// So be careful when using the TableInfo.
	TableInfo *TableInfo `json:"-" msg:"-"`

	Columns      []*Column `json:"columns" msg:"-"`
	PreColumns   []*Column `json:"pre-columns" msg:"-"`
	IndexColumns [][]int   `json:"-" msg:"index-columns"`

	// ApproximateDataSize is the approximate size of protobuf binary
	// representation of this event.
	ApproximateDataSize int64 `json:"-" msg:"-"`

	// SplitTxn marks this RowChangedEvent as the first line of a new txn.
	SplitTxn bool `json:"-" msg:"-"`
	// ReplicatingTs is ts when a table starts replicating events to downstream.
	ReplicatingTs Ts `json:"-" msg:"-"`
}

// txnRows represents a set of events that belong to the same transaction.
type txnRows []*RowChangedEvent

// Len is the number of elements in the collection.
func (e txnRows) Len() int {
	return len(e)
}

// Less sort the events base on the order of event type delete<update<insert
func (e txnRows) Less(i, j int) bool {
	return getDMLOrder(e[i]) < getDMLOrder(e[j])
}

// getDMLOrder returns the order of the dml types: delete<update<insert
func getDMLOrder(event *RowChangedEvent) int {
	if event.IsDelete() {
		return typeDelete
	} else if event.IsUpdate() {
		return typeUpdate
	}
	return typeInsert
}

func (e txnRows) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// GetCommitTs returns the commit timestamp of this event.
func (r *RowChangedEvent) GetCommitTs() uint64 {
	return r.CommitTs
}

// TrySplitAndSortUpdateEvent do nothing
func (r *RowChangedEvent) TrySplitAndSortUpdateEvent(sinkScheme string) error {
	return nil
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
func (r *RowChangedEvent) HandleKeyColInfos() ([]*Column, []rowcodec.ColInfo) {
	pkeyCols := make([]*Column, 0)
	pkeyColInfos := make([]rowcodec.ColInfo, 0)

	var cols []*Column
	if r.IsDelete() {
		cols = r.PreColumns
	} else {
		cols = r.Columns
	}

	for i, col := range cols {
		if col != nil && col.Flag.IsHandleKey() {
			pkeyCols = append(pkeyCols, col)
			pkeyColInfos = append(pkeyColInfos, r.ColInfos[i])
		}
	}

	// It is okay not to have handle keys, so the empty array is an acceptable result
	return pkeyCols, pkeyColInfos
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
	const sizeOfRowEvent = int(unsafe.Sizeof(*r))
	const sizeOfTable = int(unsafe.Sizeof(*r.Table))
	const sizeOfIndexes = int(unsafe.Sizeof(r.IndexColumns[0]))
	const sizeOfInt = int(unsafe.Sizeof(int(0)))

	// Size of table name
	size := len(r.Table.Schema) + len(r.Table.Table) + sizeOfTable
	// Size of cols
	for i := range r.Columns {
		size += r.Columns[i].ApproximateBytes
	}
	// Size of pre cols
	for i := range r.PreColumns {
		if r.PreColumns[i] != nil {
			size += r.PreColumns[i].ApproximateBytes
		}
	}
	// Size of index columns
	for i := range r.IndexColumns {
		size += len(r.IndexColumns[i]) * sizeOfInt
		size += sizeOfIndexes
	}
	// Size of an empty row event
	size += sizeOfRowEvent
	return size
}

// Column represents a column value in row changed event
type Column struct {
	Name      string         `json:"name" msg:"name"`
	Type      byte           `json:"type" msg:"type"`
	Charset   string         `json:"charset" msg:"charset"`
	Collation string         `json:"collation" msg:"collation"`
	Flag      ColumnFlagType `json:"flag" msg:"-"`
	Value     interface{}    `json:"value" msg:"value"`
	Default   interface{}    `json:"default" msg:"-"`

	// ApproximateBytes is approximate bytes consumed by the column.
	ApproximateBytes int `json:"-" msg:"-"`
}

// RedoColumn stores Column change
type RedoColumn struct {
	Column *Column `msg:"column"`
	Flag   uint64  `msg:"flag"`
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
			indexInfo.Unique = true
		}
		if firstCol.Flag.IsUniqueKey() {
			indexInfo.Unique = true
		}

		isPrimary := true
		for _, offset := range colOffsets {
			col := columns[offset]
			// When only all columns in the index are primary key, then the index is primary key.
			if col == nil || !col.Flag.IsPrimaryKey() {
				isPrimary = false
			}

			tiCol := ret.Columns[offset]
			indexCol := &model.IndexColumn{}
			indexCol.Name = tiCol.Name
			indexCol.Offset = offset
			indexInfo.Columns = append(indexInfo.Columns, indexCol)
			indexInfo.Primary = isPrimary
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
	Done         atomic.Bool      `msg:"-"`
	Charset      string           `msg:"-"`
	Collate      string           `msg:"-"`
}

// FromJob fills the values with DDLEvent from DDL job
func (d *DDLEvent) FromJob(job *model.Job, preTableInfo *TableInfo, tableInfo *TableInfo) {
	d.FromJobWithArgs(job, preTableInfo, tableInfo, "", "")
}

// FromJobWithArgs fills the values with DDLEvent from DDL job
func (d *DDLEvent) FromJobWithArgs(
	job *model.Job,
	preTableInfo, tableInfo *TableInfo,
	oldSchemaName, newSchemaName string,
) {
	d.StartTs = job.StartTS
	d.CommitTs = job.BinlogInfo.FinishedTS
	d.Type = job.Type
	d.PreTableInfo = preTableInfo
	d.TableInfo = tableInfo
	d.Charset = job.Charset
	d.Collate = job.Collate

	switch d.Type {
	// The query for "DROP TABLE" and "DROP VIEW" statements need
	// to be rebuilt. The reason is elaborated as follows:
	// for a DDL statement like "DROP TABLE test1.table1, test2.table2",
	// two DDL jobs will be generated. These two jobs can be differentiated
	// from job.BinlogInfo.TableInfo whereas the job.Query are identical.
	case model.ActionDropTable:
		d.Query = fmt.Sprintf("DROP TABLE `%s`.`%s`",
			d.TableInfo.TableName.Schema, d.TableInfo.TableName.Table)
	case model.ActionDropView:
		d.Query = fmt.Sprintf("DROP VIEW `%s`.`%s`",
			d.TableInfo.TableName.Schema, d.TableInfo.TableName.Table)
	case model.ActionRenameTables:
		oldTableName := preTableInfo.Name.O
		newTableName := tableInfo.Name.O
		d.Query = fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`",
			oldSchemaName, oldTableName, newSchemaName, newTableName)
		// Note that type is ActionRenameTable, not ActionRenameTables.
		d.Type = model.ActionRenameTable
	case model.ActionExchangeTablePartition:
		// Parse idx of partition name from query.
		upperQuery := strings.ToUpper(job.Query)
		idx1 := strings.Index(upperQuery, "EXCHANGE PARTITION") + len("EXCHANGE PARTITION")
		idx2 := strings.Index(upperQuery, "WITH TABLE")

		// Note that partition name should be parsed from original query, not the upperQuery.
		partName := strings.TrimSpace(job.Query[idx1:idx2])
		// The tableInfo is the partition table, preTableInfo is non partition table.
		d.Query = fmt.Sprintf("ALTER TABLE `%s`.`%s` EXCHANGE PARTITION `%s` WITH TABLE `%s`.`%s`",
			tableInfo.TableName.Schema, tableInfo.TableName.Table, partName,
			preTableInfo.TableName.Schema, preTableInfo.TableName.Table)

		if strings.HasSuffix(upperQuery, "WITHOUT VALIDATION") {
			d.Query += " WITHOUT VALIDATION"
		}
	default:
		d.Query = job.Query
	}
}

// SingleTableTxn represents a transaction which includes many row events in a single table
//
//msgp:ignore SingleTableTxn
type SingleTableTxn struct {
	Table     *TableName
	TableInfo *TableInfo
	// TableInfoVersion is the version of the table info, it is used to generate data path
	// in storage sink. Generally, TableInfoVersion equals to `SingleTableTxn.TableInfo.Version`.
	// Besides, if one table is just scheduled to a new processor, the TableInfoVersion should be
	// greater than or equal to the startTs of table sink.
	TableInfoVersion uint64

	StartTs  uint64
	CommitTs uint64
	Rows     []*RowChangedEvent

	// control fields of SingleTableTxn
	// FinishWg is a barrier txn, after this txn is received, the worker must
	// flush cached txns and call FinishWg.Done() to mark txns have been flushed.
	FinishWg *sync.WaitGroup
}

// GetCommitTs returns the commit timestamp of the transaction.
func (t *SingleTableTxn) GetCommitTs() uint64 {
	return t.CommitTs
}

// TrySplitAndSortUpdateEvent split update events if unique key is updated
func (t *SingleTableTxn) TrySplitAndSortUpdateEvent(sinkScheme string) error {
	if !t.shouldSplitUpdateEvent(sinkScheme) {
		return nil
	}

	newRows, err := trySplitAndSortUpdateEvent(t.Rows)
	if err != nil {
		return errors.Trace(err)
	}
	t.Rows = newRows
	return nil
}

// Whether split a single update event into delete and insert events？
//
// For the MySQL Sink, we don't split any update event.
// This may cause error like "duplicate entry" when sink to the downstream.
// This kind of error will cause the changefeed to restart,
// and then the related update rows will be splitted to insert and delete at puller side.
//
// For the Kafka and Storage sink, always split a single unique key changed update event, since:
// 1. Avro and CSV does not output the previous column values for the update event, so it would
// cause consumer missing data if the unique key changed event is not split.
// 2. Index-Value Dispatcher cannot work correctly if the unique key changed event isn't split.
func (t *SingleTableTxn) shouldSplitUpdateEvent(sinkScheme string) bool {
	return !sink.IsMySQLCompatibleScheme(sinkScheme)
}

// trySplitAndSortUpdateEvent try to split update events if unique key is updated
// returns true if some updated events is split
func trySplitAndSortUpdateEvent(
	events []*RowChangedEvent,
) ([]*RowChangedEvent, error) {
	rowChangedEvents := make([]*RowChangedEvent, 0, len(events))
	split := false
	for _, e := range events {
		if e == nil {
			log.Warn("skip emit nil event",
				zap.Any("event", e))
			continue
		}

		colLen := len(e.Columns)
		preColLen := len(e.PreColumns)
		// Some transactions could generate empty row change event, such as
		// begin; insert into t (id) values (1); delete from t where id=1; commit;
		// Just ignore these row changed events.
		if colLen == 0 && preColLen == 0 {
			log.Warn("skip emit empty row event",
				zap.Any("event", e))
			continue
		}

		// This indicates that it is an update event. if the pk or uk is updated,
		// we need to split it into two events (delete and insert).
		if e.IsUpdate() && ShouldSplitUpdateEvent(e) {
			deleteEvent, insertEvent, err := SplitUpdateEvent(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			split = true
			rowChangedEvents = append(rowChangedEvents, deleteEvent, insertEvent)
		} else {
			rowChangedEvents = append(rowChangedEvents, e)
		}
	}
	// some updated events is split, need to sort
	if split {
		sort.Sort(txnRows(rowChangedEvents))
	}
	return rowChangedEvents, nil
}

// ShouldSplitUpdateEvent determines if the split event is needed to align the old format based on
// whether the handle key column or unique key has been modified.
// If  is modified, we need to use splitUpdateEvent to split the update event into a delete and an insert event.
func ShouldSplitUpdateEvent(updateEvent *RowChangedEvent) bool {
	// nil event will never be split.
	if updateEvent == nil {
		return false
	}

	for i := range updateEvent.Columns {
		col := updateEvent.Columns[i]
		preCol := updateEvent.PreColumns[i]
		if col != nil && (col.Flag.IsUniqueKey() || col.Flag.IsHandleKey()) &&
			preCol != nil && (preCol.Flag.IsUniqueKey() || preCol.Flag.IsHandleKey()) {
			colValueString := ColumnValueString(col.Value)
			preColValueString := ColumnValueString(preCol.Value)
			// If one unique key columns is updated, we need to split the event row.
			if colValueString != preColValueString {
				return true
			}
		}
	}
	return false
}

// SplitUpdateEvent splits an update event into a delete and an insert event.
func SplitUpdateEvent(
	updateEvent *RowChangedEvent,
) (*RowChangedEvent, *RowChangedEvent, error) {
	if updateEvent == nil {
		return nil, nil, errors.New("nil event cannot be split")
	}

	// If there is an update to handle key columns,
	// we need to split the event into two events to be compatible with the old format.
	// NOTICE: Here we don't need a full deep copy because
	// our two events need Columns and PreColumns respectively,
	// so it won't have an impact and no more full deep copy wastes memory.
	deleteEvent := *updateEvent
	deleteEvent.Columns = nil

	insertEvent := *updateEvent
	// NOTICE: clean up pre cols for insert event.
	insertEvent.PreColumns = nil

	return &deleteEvent, &insertEvent, nil
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
