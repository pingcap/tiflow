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

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
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
	SchemaID         int64
	TableName        TableName
	TableInfoVersion uint64
	columnsOffset    map[int64]int
	indicesOffset    map[int64]int
	uniqueColumns    map[int64]struct{}

	// It's a mapping from ColumnID to the offset of columns in row changed events.
	RowColumnsOffset map[int64]int

	ColumnsFlag map[int64]ColumnFlagType

	// only for new row format decoder
	handleColID int64

	// the mounter will choose this index to output delete events
	// special value:
	// HandleIndexPKIsHandle(-1) : pk is handle
	// HandleIndexTableIneligible(-2) : the table is not eligible
	HandleIndexID int64

	// if the table of this row only has one unique index(includes primary key),
	// IndieMarkCol will be set to the name of the unique index
	IndieMarkCol       string
	IndexColumnsOffset [][]int
	rowColInfos        []rowcodec.ColInfo
}

// WrapTableInfo creates a TableInfo from a timodel.TableInfo
func WrapTableInfo(schemaID int64, schemaName string, version uint64, info *model.TableInfo) *TableInfo {
	ti := &TableInfo{
		TableInfo:        info,
		SchemaID:         schemaID,
		TableName:        TableName{Schema: schemaName, Table: info.Name.O},
		TableInfoVersion: version,
		columnsOffset:    make(map[int64]int, len(info.Columns)),
		indicesOffset:    make(map[int64]int, len(info.Indices)),
		uniqueColumns:    make(map[int64]struct{}),
		RowColumnsOffset: make(map[int64]int, len(info.Columns)),
		ColumnsFlag:      make(map[int64]ColumnFlagType, len(info.Columns)),
		handleColID:      -1,
		HandleIndexID:    HandleIndexTableIneligible,
		rowColInfos:      make([]rowcodec.ColInfo, len(info.Columns)),
	}

	uniqueIndexNum := 0
	rowColumnsCurrentOffset := 0

	for i, col := range ti.Columns {
		ti.columnsOffset[col.ID] = i
		isPK := false
		if ti.IsColCDCVisible(col) {
			ti.RowColumnsOffset[col.ID] = rowColumnsCurrentOffset
			rowColumnsCurrentOffset++
			isPK = (ti.PKIsHandle && mysql.HasPriKeyFlag(col.Flag)) || col.ID == model.ExtraHandleID
			if isPK {
				// pk is handle
				ti.handleColID = col.ID
				ti.HandleIndexID = HandleIndexPKIsHandle
				ti.uniqueColumns[col.ID] = struct{}{}
				ti.IndexColumnsOffset = append(ti.IndexColumnsOffset, []int{ti.RowColumnsOffset[col.ID]})
				uniqueIndexNum++
			}
		}
		ti.rowColInfos[i] = rowcodec.ColInfo{
			ID:         col.ID,
			IsPKHandle: isPK,
			Ft:         rowcodec.FieldTypeFromModelColumn(col),
		}
	}

	for i, idx := range ti.Indices {
		ti.indicesOffset[idx.ID] = i
		if ti.IsIndexUnique(idx, false) {
			for _, col := range idx.Columns {
				ti.uniqueColumns[ti.Columns[col.Offset].ID] = struct{}{}
			}
		}
		if ti.IsIndexUnique(idx, true) {
			indexColOffset := make([]int, 0, len(idx.Columns))
			for _, idxCol := range idx.Columns {
				colInfo := ti.Columns[idxCol.Offset]
				if ti.IsColCDCVisible(colInfo) {
					indexColOffset = append(indexColOffset, ti.RowColumnsOffset[colInfo.ID])
				}
			}
			if len(indexColOffset) > 0 {
				ti.IndexColumnsOffset = append(ti.IndexColumnsOffset, indexColOffset)
			}
			uniqueIndexNum++
		}
	}

	// this table has only one unique column
	if uniqueIndexNum == 1 && len(ti.uniqueColumns) == 1 {
		for col := range ti.uniqueColumns {
			info, _ := ti.GetColumnInfo(col)
			if !info.IsGenerated() {
				ti.IndieMarkCol = info.Name.O
			}
		}
	}
	ti.findHandleIndex()
	log.Debug("select the handle index", zap.Int64("tableID", ti.ID), zap.Int64("HandleIndexID", ti.HandleIndexID))
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
		if !ti.IsIndexUnique(idx, false) {
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
		switch {
		case colInfo.Charset == "binary":
			flag.SetIsBinary()
		case colInfo.IsGenerated():
			flag.SetIsGeneratedColumn()
		case mysql.HasPriKeyFlag(colInfo.Flag):
			flag.SetIsPrimaryKey()
			if ti.HandleIndexID == HandleIndexPKIsHandle {
				flag.SetIsHandleKey()
			}
		case mysql.HasUniKeyFlag(colInfo.Flag):
			flag.SetIsUniqueKey()
		case !mysql.HasNotNullFlag(colInfo.Flag):
			flag.SetIsNullable()
		case mysql.HasMultipleKeyFlag(colInfo.Flag):
			flag.SetIsMultipleKey()
		}
		ti.ColumnsFlag[colInfo.ID] = flag
	}

	// In TiDB, Only the first column can be set multiple key,
	// if unique index has multi columns,
	// the flag should be MultipleKeyFlag.
	// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
	// However, that's not what we want, we would like to express here a flag that is as complete as possible.
	for _, idxInfo := range ti.Indices {
		for _, idxCol := range idxInfo.Columns {
			colInfo := ti.Columns[idxCol.Offset]
			flag := ti.ColumnsFlag[colInfo.ID]
			switch {
			case idxInfo.Primary:
				flag.SetIsPrimaryKey()
			case idxInfo.Unique:
				flag.SetIsUniqueKey()
			case len(idxInfo.Columns) > 1:
				flag.SetIsMultipleKey()
			case idxInfo.ID == ti.HandleIndexID && ti.HandleIndexID >= 0:
				flag.SetIsHandleKey()
			}
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

// GetIndexInfo returns the index info by ID
func (ti *TableInfo) GetIndexInfo(indexID int64) (info *model.IndexInfo, exist bool) {
	indexOffset, exist := ti.indicesOffset[indexID]
	if !exist {
		return nil, false
	}
	return ti.Indices[indexOffset], true
}

// GetRowColInfos returns all column infos for rowcodec
func (ti *TableInfo) GetRowColInfos() (int64, []rowcodec.ColInfo) {
	return ti.handleColID, ti.rowColInfos
}

// IsColCDCVisible returns whether the col is visible for CDC
func (ti *TableInfo) IsColCDCVisible(col *model.ColumnInfo) bool {
	// this column is a virtual generated column
	if col.IsGenerated() && !col.GeneratedStored {
		return false
	}
	return col.State == model.StatePublic
}

// GetUniqueKeys returns all unique keys of the table as a slice of column names
func (ti *TableInfo) GetUniqueKeys() [][]string {
	var uniqueKeys [][]string
	if ti.PKIsHandle {
		for _, col := range ti.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				// Prepend to make sure the primary key ends up at the front
				uniqueKeys = [][]string{{col.Name.O}}
				break
			}
		}
	}
	for _, idx := range ti.Indices {
		if ti.IsIndexUnique(idx, false) {
			colNames := make([]string, 0, len(idx.Columns))
			for _, col := range idx.Columns {
				colNames = append(colNames, col.Name.O)
			}
			if idx.Primary {
				uniqueKeys = append([][]string{colNames}, uniqueKeys...)
			} else {
				uniqueKeys = append(uniqueKeys, colNames)
			}
		}
	}
	return uniqueKeys
}

// IsColumnUnique returns whether the column is unique
func (ti *TableInfo) IsColumnUnique(colID int64) bool {
	_, exist := ti.uniqueColumns[colID]
	return exist
}

// ExistTableUniqueColumn returns whether the table has a unique column
func (ti *TableInfo) ExistTableUniqueColumn() bool {
	return len(ti.uniqueColumns) != 0
}

// IsEligible returns whether the table is a eligible table
func (ti *TableInfo) IsEligible() bool {
	if ti.IsView() {
		return true
	}
	return ti.ExistTableUniqueColumn()
}

// IsIndexUnique returns whether the index is unique
func (ti *TableInfo) IsIndexUnique(indexInfo *model.IndexInfo, allowColumnNullable bool) bool {
	if indexInfo.Primary {
		return true
	}
	if indexInfo.Unique {
		if allowColumnNullable {
			return true
		}
		for _, col := range indexInfo.Columns {
			colInfo := ti.Columns[col.Offset]
			if !mysql.HasNotNullFlag(colInfo.Flag) {
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
	return WrapTableInfo(ti.SchemaID, ti.TableName.Schema, ti.TableInfoVersion, ti.TableInfo.Clone())
}
