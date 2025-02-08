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

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/rowcodec"
)

const (
	// HandleIndexPKIsHandle represents that the handle index is the pk and the pk is the handle
	HandleIndexPKIsHandle = -1
	// HandleIndexTableIneligible represents that the table is ineligible
	HandleIndexTableIneligible = -2
)

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	*model.TableInfo
	SchemaID int64
	// NOTICE: We probably store the logical ID inside TableName,
	// not the physical ID.
	// For normal table, there is only one ID, which is the physical ID.
	// AKA TIDB_TABLE_ID.
	// For partitioned table, there are two kinds of ID:
	// 1. TIDB_PARTITION_ID is the physical ID of the partition.
	// 2. TIDB_TABLE_ID is the logical ID of the table.
	// In general, we always use the physical ID to represent a table, but we
	// record the logical ID from the DDL event(job.BinlogInfo.TableInfo).
	// So be careful when using the TableInfo.
	TableName TableName
	// Version record the tso of create the table info.
	Version       uint64
	columnsOffset map[int64]int
	indicesOffset map[int64]int
	uniqueColumns map[int64]struct{}

	// It's a mapping from ColumnID to the offset of the columns in row changed events.
	RowColumnsOffset map[int64]int

	ColumnsFlag map[int64]ColumnFlagType

	// only for new row format decoder
	handleColID []int64

	// the mounter will choose this index to output delete events
	// special value:
	// HandleIndexPKIsHandle(-1) : pk is handle
	// HandleIndexTableIneligible(-2) : the table is not eligible
	HandleIndexID int64

	IndexColumnsOffset [][]int
	// rowColInfos extend the model.ColumnInfo with some extra information
	// it's the same length and order with the model.TableInfo.Columns
	rowColInfos    []rowcodec.ColInfo
	rowColFieldTps map[int64]*types.FieldType
}

// WrapTableInfo creates a TableInfo from a timodel.TableInfo
func WrapTableInfo(schemaID int64, schemaName string, version uint64, info *model.TableInfo) *TableInfo {
	ti := &TableInfo{
		TableInfo: info,
		SchemaID:  schemaID,
		TableName: TableName{
			Schema:      schemaName,
			Table:       info.Name.O,
			TableID:     info.ID,
			IsPartition: info.GetPartitionInfo() != nil,
		},
		Version:          version,
		columnsOffset:    make(map[int64]int, len(info.Columns)),
		indicesOffset:    make(map[int64]int, len(info.Indices)),
		uniqueColumns:    make(map[int64]struct{}),
		RowColumnsOffset: make(map[int64]int, len(info.Columns)),
		ColumnsFlag:      make(map[int64]ColumnFlagType, len(info.Columns)),
		handleColID:      []int64{-1},
		HandleIndexID:    HandleIndexTableIneligible,
		rowColInfos:      make([]rowcodec.ColInfo, len(info.Columns)),
		rowColFieldTps:   make(map[int64]*types.FieldType, len(info.Columns)),
	}

	rowColumnsCurrentOffset := 0

	for i, col := range ti.Columns {
		ti.columnsOffset[col.ID] = i
		pkIsHandle := false
		if IsColCDCVisible(col) {
			ti.RowColumnsOffset[col.ID] = rowColumnsCurrentOffset
			rowColumnsCurrentOffset++
			pkIsHandle = (ti.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag())) || col.ID == model.ExtraHandleID
			if pkIsHandle {
				// pk is handle
				ti.handleColID = []int64{col.ID}
				ti.HandleIndexID = HandleIndexPKIsHandle
				ti.uniqueColumns[col.ID] = struct{}{}
				ti.IndexColumnsOffset = append(ti.IndexColumnsOffset, []int{ti.RowColumnsOffset[col.ID]})
			} else if ti.IsCommonHandle {
				ti.HandleIndexID = HandleIndexPKIsHandle
				ti.handleColID = ti.handleColID[:0]
				pkIdx := tables.FindPrimaryIndex(info)
				for _, pkCol := range pkIdx.Columns {
					id := info.Columns[pkCol.Offset].ID
					ti.handleColID = append(ti.handleColID, id)
				}
			}

		}
		ti.rowColInfos[i] = rowcodec.ColInfo{
			ID:            col.ID,
			IsPKHandle:    pkIsHandle,
			Ft:            col.FieldType.Clone(),
			VirtualGenCol: col.IsGenerated(),
		}
		ti.rowColFieldTps[col.ID] = ti.rowColInfos[i].Ft
	}

	for i, idx := range ti.Indices {
		ti.indicesOffset[idx.ID] = i
		if ti.IsIndexUnique(idx) {
			for _, col := range idx.Columns {
				ti.uniqueColumns[ti.Columns[col.Offset].ID] = struct{}{}
			}
		}
		if idx.Primary || idx.Unique {
			indexColOffset := make([]int, 0, len(idx.Columns))
			for _, idxCol := range idx.Columns {
				colInfo := ti.Columns[idxCol.Offset]
				if IsColCDCVisible(colInfo) {
					indexColOffset = append(indexColOffset, ti.RowColumnsOffset[colInfo.ID])
				}
			}
			if len(indexColOffset) > 0 {
				ti.IndexColumnsOffset = append(ti.IndexColumnsOffset, indexColOffset)
			}
		}
	}

	ti.findHandleIndex()
	ti.initColumnsFlag()
	return ti
}

func (ti *TableInfo) findHandleIndex() {
	if ti.HandleIndexID == HandleIndexPKIsHandle {
		// pk is handle
		return
	}
	handleIndexOffset := -1
	for i, idx := range ti.Indices {
		if !ti.IsIndexUnique(idx) {
			continue
		}
		if idx.Primary {
			handleIndexOffset = i
			break
		}
		if handleIndexOffset < 0 {
			handleIndexOffset = i
		} else {
			if len(ti.Indices[handleIndexOffset].Columns) > len(ti.Indices[i].Columns) ||
				(len(ti.Indices[handleIndexOffset].Columns) == len(ti.Indices[i].Columns) &&
					ti.Indices[handleIndexOffset].ID > ti.Indices[i].ID) {
				handleIndexOffset = i
			}
		}
	}
	if handleIndexOffset >= 0 {
		ti.HandleIndexID = ti.Indices[handleIndexOffset].ID
	}
}

func (ti *TableInfo) initColumnsFlag() {
	for _, colInfo := range ti.Columns {
		var flag ColumnFlagType
		if colInfo.GetCharset() == "binary" {
			flag.SetIsBinary()
		}
		if colInfo.IsGenerated() {
			flag.SetIsGeneratedColumn()
		}
		if mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			flag.SetIsPrimaryKey()
			if ti.HandleIndexID == HandleIndexPKIsHandle {
				flag.SetIsHandleKey()
			}
		}
		if mysql.HasUniKeyFlag(colInfo.GetFlag()) {
			flag.SetIsUniqueKey()
		}
		if !mysql.HasNotNullFlag(colInfo.GetFlag()) {
			flag.SetIsNullable()
		}
		if mysql.HasMultipleKeyFlag(colInfo.GetFlag()) {
			flag.SetIsMultipleKey()
		}
		if mysql.HasUnsignedFlag(colInfo.GetFlag()) {
			flag.SetIsUnsigned()
		}
		if mysql.HasZerofillFlag(colInfo.GetFlag()) {
			flag.SetZeroFill()
		}
		ti.ColumnsFlag[colInfo.ID] = flag
	}

	// In TiDB, just as in MySQL, only the first column of an index can be marked as "multiple key" or "unique key",
	// and only the first column of a unique index may be marked as "unique key".
	// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html.
	// Yet if an index has multiple columns, we would like to easily determine that all those columns are indexed,
	// which is crucial for the completeness of the information we pass to the downstream.
	// Therefore, instead of using the MySQL standard,
	// we made our own decision to mark all columns in an index with the appropriate flag(s).
	for _, idxInfo := range ti.Indices {
		for _, idxCol := range idxInfo.Columns {
			colInfo := ti.Columns[idxCol.Offset]
			flag := ti.ColumnsFlag[colInfo.ID]
			if idxInfo.Primary {
				flag.SetIsPrimaryKey()
			}
			if idxInfo.Unique {
				flag.SetIsUniqueKey()
			}
			if len(idxInfo.Columns) > 1 {
				flag.SetIsMultipleKey()
			}
			if idxInfo.ID == ti.HandleIndexID && ti.HandleIndexID >= 0 {
				flag.SetIsHandleKey()
			}
			ti.ColumnsFlag[colInfo.ID] = flag
		}
	}
}

// GetColumnInfo returns the column info by ID
func (ti *TableInfo) GetColumnInfo(colID int64) (info *model.ColumnInfo, exist bool) {
	colOffset, exist := ti.columnsOffset[colID]
	if !exist {
		return nil, false
	}
	return ti.Columns[colOffset], true
}

func (ti *TableInfo) String() string {
	return fmt.Sprintf("TableInfo, ID: %d, Name:%s, ColNum: %d, IdxNum: %d, PKIsHandle: %t", ti.ID, ti.TableName, len(ti.Columns), len(ti.Indices), ti.PKIsHandle)
}

// GetRowColInfos returns all column infos for rowcodec
func (ti *TableInfo) GetRowColInfos() ([]int64, map[int64]*types.FieldType, []rowcodec.ColInfo) {
	return ti.handleColID, ti.rowColFieldTps, ti.rowColInfos
}

// IsColCDCVisible returns whether the col is visible for CDC
func IsColCDCVisible(col *model.ColumnInfo) bool {
	// this column is a virtual generated column
	if col.IsGenerated() && !col.GeneratedStored {
		return false
	}
	return col.State == model.StatePublic
}

// ExistTableUniqueColumn returns whether the table has a unique column
func (ti *TableInfo) ExistTableUniqueColumn() bool {
	return len(ti.uniqueColumns) != 0
}

// IsEligible returns whether the table is a eligible table
func (ti *TableInfo) IsEligible(forceReplicate bool) bool {
	// Sequence is not supported yet, TiCDC needs to filter all sequence tables.
	// See https://github.com/pingcap/tiflow/issues/4559
	if ti.IsSequence() {
		return false
	}
	if forceReplicate {
		return true
	}
	if ti.IsView() {
		return true
	}
	return ti.ExistTableUniqueColumn()
}

// IsIndexUnique returns whether the index is unique
func (ti *TableInfo) IsIndexUnique(indexInfo *model.IndexInfo) bool {
	if indexInfo.Primary {
		return true
	}
	if indexInfo.Unique {
		for _, col := range indexInfo.Columns {
			colInfo := ti.Columns[col.Offset]
			if !mysql.HasNotNullFlag(colInfo.GetFlag()) {
				return false
			}
			// this column is a virtual generated column
			if colInfo.IsGenerated() && !colInfo.GeneratedStored {
				return false
			}
		}
		return true
	}
	return false
}

// Clone clones the TableInfo
func (ti *TableInfo) Clone() *TableInfo {
	return WrapTableInfo(ti.SchemaID, ti.TableName.Schema, ti.Version, ti.TableInfo.Clone())
}
