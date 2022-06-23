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

package filter

import (
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/expression"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	tifilter "github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

// fizz: 负责通过 sql 表达式来过滤行变更事件
type dmlExprFilter struct {
	filterGroup *syncer.ExprFilterGroup
	tables      map[string]*timodel.TableInfo
}

func newExprFilter(timezone string, cfg *config.FilterConfig) (*dmlExprFilter, error) {
	// fizz: we need to pass the timezone
	vars := map[string]string{
		"time_zone": timezone,
	}
	filterGroupRules, err := configToExpressionRules(cfg)
	if err != nil {
		return nil, err
	}
	sessCtx := utils.NewSessionCtx(vars)
	filterGroup := syncer.NewExprFilterGroup(sessCtx, filterGroupRules)
	log.Info("fizz: new expression filter successfully.")
	return &dmlExprFilter{filterGroup: filterGroup, tables: make(map[string]*timodel.TableInfo)}, nil
}

func (f *dmlExprFilter) shouldSkipDML(row *model.RowChangedEvent, ti *timodel.TableInfo) (bool, error) {
	tableName := dbutil.TableName(row.Table.Schema, row.Table.Table)
	table := &tifilter.Table{Schema: row.Table.Schema, Name: row.Table.Table}

	// If tableInfo was updated, we need to reset its filter group.
	if oldTi, ok := f.tables[tableName]; ok {
		if ti.UpdateTS != oldTi.UpdateTS {
			f.tables[tableName] = ti.Clone()
			f.filterGroup.ResetExprs(table)
		}
	} else {
		f.tables[tableName] = ti.Clone()
	}

	switch {
	case row.IsInsert():
		log.Info("fizz: insert event")
		exprs, err := f.filterGroup.GetInsertExprs(table, ti)
		if err != nil {
			return false, err
		}
		log.Info("fizz: got expressions", zap.Int("expression len", len(exprs)))
		return f.skipDMLByExpression(row.RowChangedDatums.RowDatums, exprs)
	case row.IsUpdate():
		oldExprs, newExprs, err := f.filterGroup.GetUpdateExprs(table, ti)
		if err != nil {
			return false, err
		}
		filterOld, err := f.skipDMLByExpression(row.RowChangedDatums.PreRowDatums, oldExprs)
		if err != nil {
			return false, err
		}
		filterNew, err := f.skipDMLByExpression(row.RowChangedDatums.RowDatums, newExprs)
		if err != nil {
			return false, err
		}
		return filterOld || filterNew, nil
	case row.IsDelete():
		exprs, err := f.filterGroup.GetDeleteExprs(table, ti)
		if err != nil {
			return false, err
		}
		return f.skipDMLByExpression(row.RowChangedDatums.RowDatums, exprs)
	default:
		log.Warn("unknown row changed event type")
	}
	return false, nil
}

func (f *dmlExprFilter) skipDMLByExpression(rowData []types.Datum, exprs []expression.Expression) (bool, error) {
	log.Info("fizz: skipDMLByExpression", zap.Any("rowData", rowData), zap.Any("exprs", exprs))
	if len(rowData) == 0 || len(exprs) == 0 {
		return false, nil
	}

	r := chunk.MutRowFromDatums(rowData).ToRow()
	log.Info("fizz", zap.Int("row column len", r.Len()))
	log.Info("fizz", zap.Any("row ", r))

	for _, expr := range exprs {
		log.Info("fizz: filter by expr", zap.String("expr", expr.String()))
		d, err := expr.Eval(r)
		if err != nil {
			return false, err
		}
		if d.GetInt64() == 1 {
			log.Info("fizz: expr eval ", zap.Int64("eval", d.GetInt64()))
			return true, nil
		}
		log.Info("fizz: expr eval ", zap.Int64("eval", d.GetInt64()))
	}
	return false, nil
}
