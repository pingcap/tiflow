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
	"slices"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"go.uber.org/zap"
)

// WhereHandle is used to generate a WHERE clause in SQL and select causality
// keys. It also caches generated-column expressions needed to materialize
// hidden columns backing unique expression indexes.
type WhereHandle struct {
	UniqueNotNullIdx *model.IndexInfo
	// UniqueIdxs contains WHERE-eligible unique indexes. Expression indexes
	// backed by hidden generated columns are tracked in causalityIdxs instead.
	UniqueIdxs []*model.IndexInfo

	// causalityIdxs is a superset of UniqueIdxs and also includes expression indexes.
	causalityIdxs                  []*model.IndexInfo
	hiddenGeneratedColumnExprCache *generatedColumnExprCache
	rowMapper                      rowValueMapper
}

type rowValueMapper struct {
	columns                     []*model.ColumnInfo
	visibleColumns              []*model.ColumnInfo
	visibleOffsetByColumnOffset []int
}

func newRowValueMapper(columns []*model.ColumnInfo) rowValueMapper {
	visibleColumns := make([]*model.ColumnInfo, 0, len(columns))
	visibleOffsetByColumnOffset := make([]int, len(columns))
	for i := range visibleOffsetByColumnOffset {
		visibleOffsetByColumnOffset[i] = -1
	}
	for _, column := range columns {
		if column.Hidden {
			continue
		}
		visibleOffsetByColumnOffset[column.Offset] = len(visibleColumns)
		visibleColumns = append(visibleColumns, column)
	}
	return rowValueMapper{
		columns:                     columns,
		visibleColumns:              visibleColumns,
		visibleOffsetByColumnOffset: visibleOffsetByColumnOffset,
	}
}

func (m rowValueMapper) isFullValues(values []any) bool {
	return len(values) == len(m.visibleOffsetByColumnOffset)
}

func (m rowValueMapper) columnsForValues(values []any) []*model.ColumnInfo {
	if m.isFullValues(values) {
		return m.columns
	}
	return m.visibleColumns
}

func (m rowValueMapper) columnsAndValuesByIndex(
	indexInfo *model.IndexInfo,
	values []any,
) ([]*model.ColumnInfo, []any) {
	cols := make([]*model.ColumnInfo, 0, len(indexInfo.Columns))
	vals := make([]any, 0, len(indexInfo.Columns))
	for _, column := range indexInfo.Columns {
		offset := m.valueOffset(column.Offset, values)
		cols = append(cols, m.columns[column.Offset])
		vals = append(vals, values[offset])
	}
	return cols, vals
}

func (m rowValueMapper) valuesByIndex(indexInfo *model.IndexInfo, values []any) []any {
	ret := make([]any, 0, len(indexInfo.Columns))
	if values == nil {
		return ret
	}
	for _, column := range indexInfo.Columns {
		offset := m.valueOffset(column.Offset, values)
		ret = append(ret, values[offset])
	}
	return ret
}

func (m rowValueMapper) valueOffset(columnOffset int, values []any) int {
	if m.isFullValues(values) {
		return columnOffset
	}
	return m.visibleOffsetByColumnOffset[columnOffset]
}

type generatedColumnExprCache struct {
	sourceTableInfo *model.TableInfo
	columns         []*model.ColumnInfo
	once            sync.Once
	// ExprContext is cached with the per-table WhereHandle. The handle is rebuilt
	// after DDL invalidates the schema cache. Within one Syncer lifetime, DM uses
	// a fixed downstream apply SQL mode/timezone for expression-index evaluation.
	exprCtx *exprstatic.ExprContext
	exprs   map[int]expression.Expression
	ok      bool
}

func newGeneratedColumnExprCache(source *model.TableInfo) *generatedColumnExprCache {
	cols := make([]*model.ColumnInfo, 0)
	for _, col := range source.Columns {
		if col.Hidden && col.IsGenerated() {
			cols = append(cols, col)
		}
	}
	return &generatedColumnExprCache{
		sourceTableInfo: source,
		columns:         cols,
	}
}

// getOrBuildExprs uses tiSessionCtx only on the first cache build.
func (c *generatedColumnExprCache) getOrBuildExprs(
	tiSessionCtx sessionctx.Context,
) (map[int]expression.Expression, *exprstatic.ExprContext, bool) {
	c.once.Do(func() {
		c.exprCtx = generatedColumnExprContext(tiSessionCtx)
		exprs := make(map[int]expression.Expression)
		for _, col := range c.columns {
			e, err := expression.ParseSimpleExprWithTableInfo(c.exprCtx, col.GeneratedExprString, c.sourceTableInfo)
			if err != nil {
				// Current causality callers cannot surface this error to the
				// scheduler, so keep the existing degraded behavior: skip the
				// table's hidden-column indexes. This can reduce causality, but
				// after DM has tracked the DDL, a build failure usually means a
				// schema/session mismatch that may also fail downstream DML.
				log.Warn("cannot build generated column expression, hidden-column indexes will be skipped for causality",
					zap.String("column", col.Name.O), zap.Error(err))
				return
			}
			exprs[col.Offset] = e
		}
		c.exprs = exprs
		c.ok = true
	})
	return c.exprs, c.exprCtx, c.ok
}

func generatedColumnExprContext(tiSessionCtx sessionctx.Context) *exprstatic.ExprContext {
	vars := tiSessionCtx.GetSessionVars()
	charset, collation := vars.GetCharsetInfo()
	// TODO(joechenrh): Carry downstream charset/collation for collation-sensitive expression indexes.
	// Remove this once DM initializes these session settings from downstream.
	evalCtx := exprstatic.NewEvalContext(
		exprstatic.WithLocation(vars.Location()),
		exprstatic.WithSQLMode(vars.SQLMode),
		exprstatic.WithWarnHandler(contextutil.IgnoreWarn),
	)
	planCacheTracker := contextutil.NewPlanCacheTracker(contextutil.IgnoreWarn)
	return exprstatic.NewExprContext(
		exprstatic.WithCharset(charset, collation),
		exprstatic.WithEvalCtx(evalCtx),
		exprstatic.WithPlanCacheTracker(&planCacheTracker),
	)
}

// GetWhereHandle calculates a WhereHandle by source/target TableInfo's indices,
// columns and state. Other component can cache the result.
func GetWhereHandle(source, target *model.TableInfo) *WhereHandle {
	ret := WhereHandle{
		rowMapper: newRowValueMapper(source.Columns),
	}
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

		ret.causalityIdxs = append(ret.causalityIdxs, rewritten)
		if indexHasHiddenColumn(rewritten, source) {
			if ret.hiddenGeneratedColumnExprCache == nil {
				ret.hiddenGeneratedColumnExprCache = newGeneratedColumnExprCache(source)
			}
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

func indexHasHiddenColumn(index *model.IndexInfo, source *model.TableInfo) bool {
	return slices.ContainsFunc(
		[]*model.IndexColumn(index.Columns),
		func(key *model.IndexColumn) bool {
			return key.Offset < len(source.Columns) && source.Columns[key.Offset].Hidden
		},
	)
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
		values := h.rowMapper.valuesByIndex(idx, data)
		for _, value := range values {
			if value == nil {
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
