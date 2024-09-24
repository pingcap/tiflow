// Copyright 2022 PingCAP, Inc.
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

package sqlmodel

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// WhereHandle is used to generate a WHERE clause in SQL.
type WhereHandle struct {
	UniqueNotNullIdx *model.IndexInfo
	// If the index and columns have no NOT NULL constraint, but all data is NOT
	// NULL, we can still use it.
	// every index that is UNIQUE should be added to UniqueIdxs, even for
	// PK and NOT NULL.
	UniqueIdxs []*model.IndexInfo
}

// GetWhereHandle calculates a WhereHandle by source/target TableInfo's indices,
// columns and state. Other component can cache the result.
func GetWhereHandle(source, target *model.TableInfo) *WhereHandle {
	ret := WhereHandle{}
	indices := make([]*model.IndexInfo, 0, len(target.Indices)+1)
	indices = append(indices, target.Indices...)
	if idx := getPKIsHandleIdx(target); target.PKIsHandle && idx != nil {
		indices = append(indices, idx)
	}

	for _, idx := range indices {
		if !idx.Unique {
			continue
		}
		// when the tableInfo is from CDC, it may contain some index that is
		// creating.
		if idx.State != model.StatePublic {
			continue
		}

		rewritten := rewriteColsOffset(idx, source)
		if rewritten == nil {
			continue
		}
		ret.UniqueIdxs = append(ret.UniqueIdxs, rewritten)

		if rewritten.Primary {
			// PK is prior to UNIQUE NOT NULL for better performance
			ret.UniqueNotNullIdx = rewritten
			continue
		}
		// use downstream columns to check NOT NULL constraint
		if ret.UniqueNotNullIdx == nil && allColsNotNull(idx, target.Columns) {
			ret.UniqueNotNullIdx = rewritten
			continue
		}
	}
	return &ret
}

// rewriteColsOffset rewrites index columns offset to those from source table.
// Returns nil when any column does not represent in source.
func rewriteColsOffset(index *model.IndexInfo, source *model.TableInfo) *model.IndexInfo {
	if index == nil || source == nil {
		return nil
	}

	columns := make([]*model.IndexColumn, 0, len(index.Columns))
	for _, key := range index.Columns {
		sourceColumn := model.FindColumnInfo(source.Columns, key.Name.L)
		if sourceColumn == nil {
			return nil
		}
		column := &model.IndexColumn{
			Name:   key.Name,
			Offset: sourceColumn.Offset,
			Length: key.Length,
		}
		columns = append(columns, column)
	}
	clone := *index
	clone.Columns = columns
	return &clone
}

func getPKIsHandleIdx(ti *model.TableInfo) *model.IndexInfo {
	if pk := ti.GetPkColInfo(); pk != nil {
		return &model.IndexInfo{
			Table:   ti.Name,
			Unique:  true,
			Primary: true,
			State:   model.StatePublic,
			Tp:      pmodel.IndexTypeBtree,
			Columns: []*model.IndexColumn{{
				Name:   pk.Name,
				Offset: pk.Offset,
				Length: types.UnspecifiedLength,
			}},
		}
	}
	return nil
}

func allColsNotNull(idx *model.IndexInfo, cols []*model.ColumnInfo) bool {
	for _, idxCol := range idx.Columns {
		col := cols[idxCol.Offset]
		if !mysql.HasNotNullFlag(col.GetFlag()) {
			return false
		}
	}
	return true
}

// getWhereIdxByData returns the index that is identical to a row change, it
// may be
// - a PK, or
// - an UNIQUE index whose columns are all NOT NULL, or
// - an UNIQUE index and the data are all NOT NULL.
// For the last case, last used index is swapped to front.
func (h *WhereHandle) getWhereIdxByData(data []interface{}) *model.IndexInfo {
	if h == nil {
		log.L().DPanic("WhereHandle is nil")
		return nil
	}
	if h.UniqueNotNullIdx != nil {
		return h.UniqueNotNullIdx
	}
	for i, idx := range h.UniqueIdxs {
		ok := true
		for _, idxCol := range idx.Columns {
			if data[idxCol.Offset] == nil {
				ok = false
				break
			}
		}
		if ok {
			h.UniqueIdxs[0], h.UniqueIdxs[i] = h.UniqueIdxs[i], h.UniqueIdxs[0]
			return idx
		}
	}
	return nil
}
