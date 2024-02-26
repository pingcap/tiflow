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
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/integrity"
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
	// The handleKey is chosen by the following rules in the order:
	// 1. if the table has primary key, it's the handle key.
	// 2. If the table has not null unique key, it's the handle key.
	// 3. If the table has no primary key and no not null unique key, it has no handleKey.
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
	Schema      string `toml:"db-name" msg:"db-name"`
	Table       string `toml:"tbl-name" msg:"tbl-name"`
	TableID     int64  `toml:"tbl-id" msg:"tbl-id"`
	IsPartition bool   `toml:"is-partition" msg:"is-partition"`
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
func (r *RedoLog) TrySplitAndSortUpdateEvent(_ string) error {
	return nil
}

// RedoRowChangedEvent represents the DML event used in RedoLog
type RedoRowChangedEvent struct {
	Row        *RowChangedEventInRedoLog `msg:"row"`
	Columns    []RedoColumn              `msg:"columns"`
	PreColumns []RedoColumn              `msg:"pre-columns"`
}

// RedoDDLEvent represents DDL event used in redo log persistent
type RedoDDLEvent struct {
	DDL       *DDLEvent `msg:"ddl"`
	Type      byte      `msg:"type"`
	TableName TableName `msg:"table-name"`
}

// ToRedoLog converts row changed event to redo log
func (r *RowChangedEvent) ToRedoLog() *RedoLog {
	rowInRedoLog := &RowChangedEventInRedoLog{
		StartTs:  r.StartTs,
		CommitTs: r.CommitTs,
		Table: &TableName{
			Schema:      r.TableInfo.GetSchemaName(),
			Table:       r.TableInfo.GetTableName(),
			TableID:     r.PhysicalTableID,
			IsPartition: r.TableInfo.IsPartitionTable(),
		},
		Columns:      r.GetColumns(),
		PreColumns:   r.GetPreColumns(),
		IndexColumns: r.TableInfo.IndexColumnsOffset,
	}
	return &RedoLog{
		RedoRow: RedoRowChangedEvent{Row: rowInRedoLog},
		Type:    RedoLogTypeRow,
	}
}

// ToRedoLog converts ddl event to redo log
func (d *DDLEvent) ToRedoLog() *RedoLog {
	return &RedoLog{
		RedoDDL: RedoDDLEvent{DDL: d},
		Type:    RedoLogTypeDDL,
	}
}

// RowChangedEvent represents a row changed event
//
//msgp:ignore RowChangedEvent
type RowChangedEvent struct {
	StartTs  uint64
	CommitTs uint64

	RowID int64 // Deprecated. It is empty when the RowID comes from clustered index table.

	PhysicalTableID int64

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
	TableInfo *TableInfo

	Columns    []*ColumnData
	PreColumns []*ColumnData

	// Checksum for the event, only not nil if the upstream TiDB enable the row level checksum
	// and TiCDC set the integrity check level to the correctness.
	Checksum *integrity.Checksum

	// ApproximateDataSize is the approximate size of protobuf binary
	// representation of this event.
	ApproximateDataSize int64

	// SplitTxn marks this RowChangedEvent as the first line of a new txn.
	SplitTxn bool
	// ReplicatingTs is ts when a table starts replicating events to downstream.
	ReplicatingTs Ts
	// HandleKey is the key of the row changed event.
	// It can be used to identify the row changed event.
	// It can be one of three : common_handle, int_handle or _tidb_rowid based on the table definitions
	// 1. primary key is the clustered index, and key is not int type, then we use `CommonHandle`
	// 2. primary key is int type(including different types of int, such as bigint, TINYINT), then we use IntHandle
	// 3. when the table doesn't have the primary key and clustered index,
	//    tidb will make a hidden column called "_tidb_rowid" as the handle.
	//    due to the type of "_tidb_rowid" is int, so we also use IntHandle to represent.
	HandleKey kv.Handle
}

// RowChangedEventInRedoLog is used to store RowChangedEvent in redo log v2 format
type RowChangedEventInRedoLog struct {
	StartTs  uint64 `msg:"start-ts"`
	CommitTs uint64 `msg:"commit-ts"`

	// Table contains the table name and table ID.
	// NOTICE: We store the physical table ID here, not the logical table ID.
	Table *TableName `msg:"table"`

	Columns      []*Column `msg:"columns"`
	PreColumns   []*Column `msg:"pre-columns"`
	IndexColumns [][]int   `msg:"index-columns"`
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
func (r *RowChangedEvent) TrySplitAndSortUpdateEvent(_ string) error {
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

func columnData2Column(col *ColumnData, tableInfo *TableInfo) *Column {
	colID := col.ColumnID
	offset, ok := tableInfo.columnsOffset[colID]
	if !ok {
		log.Panic("invalid column id",
			zap.Int64("columnID", colID),
			zap.Any("tableInfo", tableInfo))
	}
	colInfo := tableInfo.Columns[offset]
	return &Column{
		Name:      colInfo.Name.O,
		Type:      colInfo.GetType(),
		Charset:   colInfo.GetCharset(),
		Collation: colInfo.GetCollate(),
		Flag:      tableInfo.ColumnsFlag[colID],
		Value:     col.Value,
		Default:   GetColumnDefaultValue(colInfo),
	}
}

func columnDatas2Columns(cols []*ColumnData, tableInfo *TableInfo) []*Column {
	if cols == nil {
		return nil
	}
	columns := make([]*Column, len(cols))
	for i, colData := range cols {
		if colData == nil {
			log.Warn("meet nil column data, should not happened in production env",
				zap.Any("cols", cols),
				zap.Any("tableInfo", tableInfo))
			continue
		}
		columns[i] = columnData2Column(colData, tableInfo)
	}
	return columns
}

// GetColumns returns the columns of the event
func (r *RowChangedEvent) GetColumns() []*Column {
	return columnDatas2Columns(r.Columns, r.TableInfo)
}

// GetPreColumns returns the pre columns of the event
func (r *RowChangedEvent) GetPreColumns() []*Column {
	return columnDatas2Columns(r.PreColumns, r.TableInfo)
}

// PrimaryKeyColumnNames return all primary key's name
func (r *RowChangedEvent) PrimaryKeyColumnNames() []string {
	var result []string

	var cols []*ColumnData
	if r.IsDelete() {
		cols = r.PreColumns
	} else {
		cols = r.Columns
	}

	result = make([]string, 0)
	tableInfo := r.TableInfo
	for _, col := range cols {
		if col != nil && tableInfo.ForceGetColumnFlagType(col.ColumnID).IsPrimaryKey() {
			result = append(result, tableInfo.ForceGetColumnName(col.ColumnID))
		}
	}
	return result
}

// GetHandleKeyColumnValues returns all handle key's column values
func (r *RowChangedEvent) GetHandleKeyColumnValues() []string {
	var result []string

	var cols []*ColumnData
	if r.IsDelete() {
		cols = r.PreColumns
	} else {
		cols = r.Columns
	}

	result = make([]string, 0)
	tableInfo := r.TableInfo
	for _, col := range cols {
		if col != nil && tableInfo.ForceGetColumnFlagType(col.ColumnID).IsHandleKey() {
			result = append(result, ColumnValueString(col.Value))
		}
	}
	return result
}

// HandleKeyColInfos returns the column(s) and colInfo(s) corresponding to the handle key(s)
func (r *RowChangedEvent) HandleKeyColInfos() ([]*Column, []rowcodec.ColInfo) {
	pkeyCols := make([]*Column, 0)
	pkeyColInfos := make([]rowcodec.ColInfo, 0)

	var cols []*ColumnData
	if r.IsDelete() {
		cols = r.PreColumns
	} else {
		cols = r.Columns
	}

	tableInfo := r.TableInfo
	colInfos := tableInfo.GetColInfosForRowChangedEvent()
	for i, col := range cols {
		if col != nil && tableInfo.ForceGetColumnFlagType(col.ColumnID).IsHandleKey() {
			pkeyCols = append(pkeyCols, columnData2Column(col, tableInfo))
			pkeyColInfos = append(pkeyColInfos, colInfos[i])
		}
	}

	// It is okay not to have handle keys, so the empty array is an acceptable result
	return pkeyCols, pkeyColInfos
}

// ApproximateBytes returns approximate bytes in memory consumed by the event.
func (r *RowChangedEvent) ApproximateBytes() int {
	const sizeOfRowEvent = int(unsafe.Sizeof(*r))

	size := 0
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
	// Size of an empty row event
	size += sizeOfRowEvent
	return size
}

// Columns2ColumnDatas convert `Column`s to `ColumnData`s
func Columns2ColumnDatas(cols []*Column, tableInfo *TableInfo) []*ColumnData {
	if cols == nil {
		return nil
	}
	columns := make([]*ColumnData, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		colID := tableInfo.ForceGetColumnIDByName(col.Name)
		columns[i] = &ColumnData{
			ColumnID: colID,
			Value:    col.Value,
		}
	}
	return columns
}

// Column represents a column value and its schema info
type Column struct {
	Name      string         `msg:"name"`
	Type      byte           `msg:"type"`
	Charset   string         `msg:"charset"`
	Collation string         `msg:"collation"`
	Flag      ColumnFlagType `msg:"-"`
	Value     interface{}    `msg:"-"`
	Default   interface{}    `msg:"-"`

	// ApproximateBytes is approximate bytes consumed by the column.
	ApproximateBytes int `msg:"-"`
}

// ColumnData represents a column value in row changed event
type ColumnData struct {
	// ColumnID may be just a mock id, because we don't store it in redo log.
	// So after restore from redo log, we need to give every a column a mock id.
	// The only guarantee is that the column id is unique in a RowChangedEvent
	ColumnID int64       `json:"column_id" msg:"column_id"`
	Value    interface{} `json:"value" msg:"-"`

	// ApproximateBytes is approximate bytes consumed by the column.
	ApproximateBytes int `json:"-" msg:"-"`
}

// RedoColumn stores Column change
type RedoColumn struct {
	// Fields from Column and can't be marshaled directly in Column.
	Value interface{} `msg:"column"`
	// msgp transforms empty byte slice into nil, PTAL msgp#247.
	ValueIsEmptyBytes bool   `msg:"value-is-empty-bytes"`
	Flag              uint64 `msg:"flag"`
}

// ColumnIDAllocator represents the interface to allocate column id for tableInfo
type ColumnIDAllocator interface {
	// GetColumnID return the column id according to the column name
	GetColumnID(name string) int64
}

// IncrementalColumnIDAllocator allocates column id in an incremental way.
// At most of the time, it is the default implementation when you don't care the column id's concrete value.
//
//msgp:ignore IncrementalColumnIDAllocator
type IncrementalColumnIDAllocator struct {
	nextColID int64
}

// NewIncrementalColumnIDAllocator creates a new IncrementalColumnIDAllocator
func NewIncrementalColumnIDAllocator() *IncrementalColumnIDAllocator {
	return &IncrementalColumnIDAllocator{
		nextColID: 100, // 100 is an arbitrary number
	}
}

// GetColumnID return the next mock column id
func (d *IncrementalColumnIDAllocator) GetColumnID(name string) int64 {
	result := d.nextColID
	d.nextColID += 1
	return result
}

// NameBasedColumnIDAllocator allocates column id using an prefined map from column name to id
//
//msgp:ignore NameBasedColumnIDAllocator
type NameBasedColumnIDAllocator struct {
	nameToIDMap map[string]int64
}

// NewNameBasedColumnIDAllocator creates a new NameBasedColumnIDAllocator
func NewNameBasedColumnIDAllocator(nameToIDMap map[string]int64) *NameBasedColumnIDAllocator {
	return &NameBasedColumnIDAllocator{
		nameToIDMap: nameToIDMap,
	}
}

// GetColumnID return the column id of the name
func (n *NameBasedColumnIDAllocator) GetColumnID(name string) int64 {
	colID, ok := n.nameToIDMap[name]
	if !ok {
		log.Panic("column not found",
			zap.String("name", name),
			zap.Any("nameToIDMap", n.nameToIDMap))
	}
	return colID
}

// BuildTableInfo builds a table info from given information.
// Note that some fields of the result TableInfo may just be mocked.
// The only guarantee is that we can use the result to reconstrut the information in `Column`.
// The main use cases of this function it to build TableInfo from redo log and in tests.
func BuildTableInfo(schemaName, tableName string, columns []*Column, indexColumns [][]int) *TableInfo {
	tidbTableInfo := BuildTiDBTableInfo(tableName, columns, indexColumns)
	return WrapTableInfo(100 /* not used */, schemaName, 1000 /* not used */, tidbTableInfo)
}

// BuildTableInfoWithPKNames4Test builds a table info from given information.
func BuildTableInfoWithPKNames4Test(schemaName, tableName string, columns []*Column, pkNames map[string]struct{}) *TableInfo {
	if len(pkNames) == 0 {
		return BuildTableInfo(schemaName, tableName, columns, nil)
	}
	indexColumns := make([][]int, 1)
	indexColumns[0] = make([]int, 0)
	for i, col := range columns {
		if _, ok := pkNames[col.Name]; ok {
			indexColumns[0] = append(indexColumns[0], i)
			col.Flag.SetIsHandleKey()
			col.Flag.SetIsPrimaryKey()
		}
	}
	if len(indexColumns[0]) != len(pkNames) {
		log.Panic("cannot find all pks",
			zap.Any("indexColumns", indexColumns),
			zap.Any("pkNames", pkNames))
	}
	return BuildTableInfo(schemaName, tableName, columns, indexColumns)
}

// AddExtraColumnInfo is used to add some extra column info to the table info.
// Just use it in test.
func AddExtraColumnInfo(tableInfo *model.TableInfo, extraColInfos []rowcodec.ColInfo) {
	for i, colInfo := range extraColInfos {
		tableInfo.Columns[i].SetElems(colInfo.Ft.GetElems())
		tableInfo.Columns[i].SetFlen(colInfo.Ft.GetFlen())
	}
}

// GetHandleAndUniqueIndexOffsets4Test is used to get the offsets of handle columns and other unique index columns in test
func GetHandleAndUniqueIndexOffsets4Test(cols []*Column) [][]int {
	result := make([][]int, 0)
	handleColumns := make([]int, 0)
	for i, col := range cols {
		if col.Flag.IsHandleKey() {
			handleColumns = append(handleColumns, i)
		} else if col.Flag.IsUniqueKey() {
			// When there is a unique key which is not handle key,
			// we cannot get the accurate index info for this key.
			// So just be aggressive to make each unique column a unique index
			// to make sure there is no write conflict when syncing data in tests.
			result = append(result, []int{i})
		}
	}
	if len(handleColumns) != 0 {
		result = append(result, handleColumns)
	}
	return result
}

// BuildTiDBTableInfoWithoutVirtualColumns build a TableInfo without virual columns from the source table info
func BuildTiDBTableInfoWithoutVirtualColumns(source *model.TableInfo) *model.TableInfo {
	ret := source.Clone()
	ret.Columns = make([]*model.ColumnInfo, 0, len(source.Columns))
	rowColumnsCurrentOffset := 0
	columnsOffset := make(map[string]int, len(source.Columns))
	for _, srcCol := range source.Columns {
		if !IsColCDCVisible(srcCol) {
			continue
		}
		colInfo := srcCol.Clone()
		colInfo.Offset = rowColumnsCurrentOffset
		ret.Columns = append(ret.Columns, colInfo)
		columnsOffset[colInfo.Name.O] = rowColumnsCurrentOffset
		rowColumnsCurrentOffset += 1
	}
	// Keep all the index info even if it contains virtual columns for simplicity
	for _, indexInfo := range ret.Indices {
		for _, col := range indexInfo.Columns {
			col.Offset = columnsOffset[col.Name.O]
		}
	}

	return ret
}

// BuildTiDBTableInfo is a simple wrapper over BuildTiDBTableInfoImpl which create a default ColumnIDAllocator.
func BuildTiDBTableInfo(tableName string, columns []*Column, indexColumns [][]int) *model.TableInfo {
	return BuildTiDBTableInfoImpl(tableName, columns, indexColumns, NewIncrementalColumnIDAllocator())
}

// BuildTiDBTableInfoImpl builds a TiDB TableInfo from given information.
// Note the result TableInfo may not be same as the original TableInfo in tidb.
// The only guarantee is that you can restore the `Name`, `Type`, `Charset`, `Collation`
// and `Flag` field of `Column` using the result TableInfo.
// The precondition required for calling this function:
//  1. There must be at least one handle key in `columns`;
//  2. The handle key must either be a primary key or a non null unique key;
//  3. The index that is selected as the handle must be provided in `indexColumns`;
func BuildTiDBTableInfoImpl(
	tableName string,
	columns []*Column,
	indexColumns [][]int,
	columnIDAllocator ColumnIDAllocator,
) *model.TableInfo {
	ret := &model.TableInfo{}
	ret.Name = model.NewCIStr(tableName)

	hasPrimaryKeyColumn := false
	for i, col := range columns {
		columnInfo := &model.ColumnInfo{
			Offset: i,
			State:  model.StatePublic,
		}
		if col == nil {
			// actually, col should never be nil according to `datum2Column` and `WrapTableInfo` in prod env
			// we mock it as generated column just for test
			columnInfo.Name = model.NewCIStr("omitted")
			columnInfo.GeneratedExprString = "pass_generated_check"
			columnInfo.GeneratedStored = false
			ret.Columns = append(ret.Columns, columnInfo)
			continue
		}
		// add a mock id to identify columns inside cdc
		columnInfo.ID = columnIDAllocator.GetColumnID(col.Name)
		columnInfo.Name = model.NewCIStr(col.Name)
		columnInfo.SetType(col.Type)

		if col.Collation != "" {
			columnInfo.SetCollate(col.Collation)
		} else {
			// collation is not stored, give it a default value
			columnInfo.SetCollate(mysql.UTF8MB4DefaultCollation)
		}

		// inverse initColumnsFlag
		flag := col.Flag
		if col.Charset != "" {
			columnInfo.SetCharset(col.Charset)
		} else if flag.IsBinary() {
			columnInfo.SetCharset("binary")
		} else {
			// charset is not stored, give it a default value
			columnInfo.SetCharset(mysql.UTF8MB4Charset)
		}
		if flag.IsGeneratedColumn() {
			// we do not use this field, so we set it to any non-empty string
			columnInfo.GeneratedExprString = "pass_generated_check"
			columnInfo.GeneratedStored = true
		}
		if flag.IsPrimaryKey() {
			columnInfo.AddFlag(mysql.PriKeyFlag)
			hasPrimaryKeyColumn = true
			if !flag.IsHandleKey() {
				log.Panic("Primary key must be handle key",
					zap.String("table", tableName),
					zap.Any("columns", columns),
					zap.Any("indexColumns", indexColumns))
			}
			// just set it for test compatibility,
			// actually we cannot deduce the value of IsCommonHandle from the provided args.
			ret.IsCommonHandle = true
		}
		if flag.IsUniqueKey() {
			columnInfo.AddFlag(mysql.UniqueKeyFlag)
		}
		if flag.IsHandleKey() {
			if !flag.IsPrimaryKey() && !flag.IsUniqueKey() {
				log.Panic("Handle key must either be primary key or unique key",
					zap.String("table", tableName),
					zap.Any("columns", columns),
					zap.Any("indexColumns", indexColumns))
			}
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

	hasPrimaryKeyIndex := false
	hasHandleIndex := false
	// TiCDC handles columns according to the following rules:
	// 1. If a primary key (PK) exists, it is chosen.
	// 2. If there is no PK, TiCDC looks for a not null unique key (UK) with the least number of columns and the smallest index ID.
	// So we assign the smallest index id to the index which is selected as handle to mock this behavior.
	minIndexID := int64(1)
	nextMockIndexID := minIndexID + 1
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
		isAllColumnsHandle := true
		for _, offset := range colOffsets {
			col := columns[offset]
			// When only all columns in the index are primary key, then the index is primary key.
			if col == nil || !col.Flag.IsPrimaryKey() {
				isPrimary = false
			}
			if col == nil || !col.Flag.IsHandleKey() {
				isAllColumnsHandle = false
			}

			tiCol := ret.Columns[offset]
			indexCol := &model.IndexColumn{}
			indexCol.Name = tiCol.Name
			indexCol.Offset = offset
			indexInfo.Columns = append(indexInfo.Columns, indexCol)
			indexInfo.Primary = isPrimary
		}
		hasPrimaryKeyIndex = hasPrimaryKeyIndex || isPrimary
		if isAllColumnsHandle {
			// If there is no primary index, only one index will contain columns which are all handles.
			// If there is a primary index, the primary index must be the handle.
			// And there may be another index which is a subset of the primary index. So we skip this check.
			if hasHandleIndex && !hasPrimaryKeyColumn {
				log.Panic("Multiple handle index found",
					zap.String("table", tableName),
					zap.Any("colOffsets", colOffsets),
					zap.String("indexName", indexInfo.Name.O),
					zap.Any("columns", columns),
					zap.Any("indexColumns", indexColumns))
			}
			hasHandleIndex = true
		}
		// If there is no primary column, we need allocate the min index id to the one selected as handle.
		// In other cases, we don't care the concrete value of index id.
		if isAllColumnsHandle && !hasPrimaryKeyColumn {
			indexInfo.ID = minIndexID
		} else {
			indexInfo.ID = nextMockIndexID
			nextMockIndexID += 1
		}

		// TODO: revert the "all column set index related flag" to "only the
		// first column set index related flag" if needed

		ret.Indices = append(ret.Indices, indexInfo)
	}
	if hasPrimaryKeyColumn != hasPrimaryKeyIndex {
		log.Panic("Primary key column and primary key index is not consistent",
			zap.String("table", tableName),
			zap.Any("columns", columns),
			zap.Any("indexColumns", indexColumns),
			zap.Bool("hasPrimaryKeyColumn", hasPrimaryKeyColumn),
			zap.Bool("hasPrimaryKeyIndex", hasPrimaryKeyIndex))
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
	IsBootstrap  bool             `msg:"-"`
	// BDRRole is the role of the TiDB cluster, it is used to determine whether
	// the DDL is executed by the primary cluster.
	BDRRole string        `msg:"-"`
	SQLMode mysql.SQLMode `msg:"-"`
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
	d.BDRRole = job.BDRRole
	d.SQLMode = job.SQLMode
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
	default:
		d.Query = job.Query
	}
}

// NewBootstrapDDLEvent returns a bootstrap DDL event.
// We set Bootstrap DDL event's startTs and commitTs to 0.
// Because it is generated by the TiCDC, not from the upstream TiDB.
// And they ere useless for a bootstrap DDL event.
func NewBootstrapDDLEvent(tableInfo *TableInfo) *DDLEvent {
	return &DDLEvent{
		StartTs:     0,
		CommitTs:    0,
		TableInfo:   tableInfo,
		IsBootstrap: true,
	}
}

// SingleTableTxn represents a transaction which includes many row events in a single table
//
//msgp:ignore SingleTableTxn
type SingleTableTxn struct {
	PhysicalTableID int64
	TableInfo       *TableInfo
	// TableInfoVersion is the version of the table info, it is used to generate data path
	// in storage sink. Generally, TableInfoVersion equals to `SingleTableTxn.TableInfo.Version`.
	// Besides, if one table is just scheduled to a new processor, the TableInfoVersion should be
	// greater than or equal to the startTs of table sink.
	TableInfoVersion uint64

	StartTs  uint64
	CommitTs uint64
	Rows     []*RowChangedEvent
}

// GetCommitTs returns the commit timestamp of the transaction.
func (t *SingleTableTxn) GetCommitTs() uint64 {
	return t.CommitTs
}

// GetPhysicalTableID returns the physical table id of the table in the transaction
func (t *SingleTableTxn) GetPhysicalTableID() int64 {
	return t.PhysicalTableID
}

// TrySplitAndSortUpdateEvent split update events if unique key is updated
func (t *SingleTableTxn) TrySplitAndSortUpdateEvent(scheme string) error {
	if !t.shouldSplitUpdateEvent(scheme) {
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
// For the MySQL Sink, there is no need to split a single unique key changed update event, this
// is also to keep the backward compatibility, the same behavior as before.
//
// For the Kafka and Storage sink, always split a single unique key changed update event, since:
// 1. Avro and CSV does not output the previous column values for the update event, so it would
// cause consumer missing data if the unique key changed event is not split.
// 2. Index-Value Dispatcher cannot work correctly if the unique key changed event isn't split.
func (t *SingleTableTxn) shouldSplitUpdateEvent(sinkScheme string) bool {
	if len(t.Rows) < 2 && sink.IsMySQLCompatibleScheme(sinkScheme) {
		return false
	}
	return true
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
		if e.IsUpdate() && shouldSplitUpdateEvent(e) {
			deleteEvent, insertEvent, err := splitUpdateEvent(e)
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

func isNonEmptyUniqueOrHandleCol(col *ColumnData, tableInfo *TableInfo) bool {
	if col != nil {
		colFlag := tableInfo.ForceGetColumnFlagType(col.ColumnID)
		return colFlag.IsUniqueKey() || colFlag.IsHandleKey()
	}
	return false
}

// shouldSplitUpdateEvent determines if the split event is needed to align the old format based on
// whether the handle key column or unique key has been modified.
// If  is modified, we need to use splitUpdateEvent to split the update event into a delete and an insert event.
func shouldSplitUpdateEvent(updateEvent *RowChangedEvent) bool {
	// nil event will never be split.
	if updateEvent == nil {
		return false
	}

	tableInfo := updateEvent.TableInfo
	for i := range updateEvent.Columns {
		col := updateEvent.Columns[i]
		preCol := updateEvent.PreColumns[i]
		if isNonEmptyUniqueOrHandleCol(col, tableInfo) && isNonEmptyUniqueOrHandleCol(preCol, tableInfo) {
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

// splitUpdateEvent splits an update event into a delete and an insert event.
func splitUpdateEvent(
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
	if row.StartTs != t.StartTs || row.CommitTs != t.CommitTs || row.PhysicalTableID != t.GetPhysicalTableID() {
		log.Panic("unexpected row change event",
			zap.Uint64("startTs", t.StartTs),
			zap.Uint64("commitTs", t.CommitTs),
			zap.Any("table", t.GetPhysicalTableID()),
			zap.Any("row", row))
	}
	t.Rows = append(t.Rows, row)
}

// TopicPartitionKey contains the topic and partition key of the message.
type TopicPartitionKey struct {
	Topic          string
	Partition      int32
	PartitionKey   string
	TotalPartition int32
}
