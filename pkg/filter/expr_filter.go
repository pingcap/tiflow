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
	"context"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/expression"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	tfilter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	dmcontext "github.com/pingcap/tiflow/dm/pkg/context"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	// exprFilterSchema is a place holder for schema name in expr filter.
	exprFilterSchema = "exprFilterSchema"
	// exprFilterTable is a place holder for table name in expr filter.
	exprFilterTable = "exprFilterTable"
)

// dmlExprFilterRule only be used by dmlExprFilter.
type dmlExprFilterRule struct {
	mu     sync.Mutex
	tables map[string]*timodel.TableInfo

	tf tfilter.Filter
	eg *syncer.ExprFilterGroup
}

// We always use exprFilterTableName to get expression from expr filter group.
var exprFilterTableName = &tfilter.Table{Schema: exprFilterSchema, Name: exprFilterTable}

func (r *dmlExprFilterRule) shouldSkipDML(row *model.RowChangedEvent, ti *timodel.TableInfo) (bool, error) {
	table := &tfilter.Table{Schema: row.Table.Schema, Name: row.Table.Table}
	tableName := table.String()

	r.mu.Lock()
	defer r.mu.Unlock()

	// If tableInfo was updated, we need to reset its filter group.
	if oldTi, ok := r.tables[tableName]; ok {
		if ti.UpdateTS != oldTi.UpdateTS {
			r.tables[tableName] = ti.Clone()
			r.eg.ResetExprs(exprFilterTableName)
		}
	} else {
		r.tables[tableName] = ti.Clone()
	}

	switch {
	case row.IsInsert():
		log.Info("fizz: insert event")
		exprs, err := r.eg.GetInsertExprs(exprFilterTableName, ti)
		if err != nil {
			return false, err
		}
		log.Info("fizz: got expressions", zap.Int("expression len", len(exprs)))
		return r.skipDMLByExpression(row.RowChangedDatums.RowDatums, exprs)
	case row.IsUpdate():
		oldExprs, newExprs, err := r.eg.GetUpdateExprs(exprFilterTableName, ti)
		if err != nil {
			return false, err
		}
		filterOld, err := r.skipDMLByExpression(row.RowChangedDatums.PreRowDatums, oldExprs)
		if err != nil {
			return false, err
		}
		filterNew, err := r.skipDMLByExpression(row.RowChangedDatums.RowDatums, newExprs)
		if err != nil {
			return false, err
		}
		return filterOld || filterNew, nil
	case row.IsDelete():
		exprs, err := r.eg.GetDeleteExprs(exprFilterTableName, ti)
		if err != nil {
			return false, err
		}
		return r.skipDMLByExpression(row.RowChangedDatums.PreRowDatums, exprs)
	default:
		log.Warn("unknown row changed event type")
	}
	return false, nil
}

func (r *dmlExprFilterRule) skipDMLByExpression(rowData []types.Datum, exprs []expression.Expression) (bool, error) {
	log.Info("fizz: skipDMLByExpression", zap.Any("rowData", rowData), zap.Any("exprs", exprs))
	if len(rowData) == 0 || len(exprs) == 0 {
		return false, nil
	}

	row := chunk.MutRowFromDatums(rowData).ToRow()
	log.Info("fizz", zap.Int("row column len", row.Len()))
	log.Info("fizz", zap.Any("row ", row))

	for _, expr := range exprs {
		log.Info("fizz: filter by expr", zap.String("expr", expr.String()))
		d, err := expr.Eval(row)
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

// dmlExprFilter is a filter that filters DML events by SQL expression.
type dmlExprFilter struct {
	rules []*dmlExprFilterRule
}

func newExprFilter(timezone string, cfg *config.FilterConfig) (*dmlExprFilter, error) {
	res := &dmlExprFilter{}

	sessCtx := utils.NewSessionCtx(map[string]string{
		"time_zone": timezone,
	})
	dmctx := dmcontext.NewContext(context.Background(),
		dmlog.Logger{Logger: log.L()})

	for _, rule := range cfg.EventFilters {
		err := res.addRule(dmctx, sessCtx, rule)
		if err != nil {
			log.Error("fizz", zap.Error(err))
			return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
		}
	}
	log.Info("fizz: new expression filter successfully.")
	return res, nil
}

func (f *dmlExprFilter) addRule(dmctx *dmcontext.Context, sessCtx sessionctx.Context, cfg *config.EventFilterRule) error {
	tf, err := tfilter.Parse(cfg.Matcher)
	if err != nil {
		log.Error("fizz", zap.Error(err))
		return cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
	}
	rule := &dmlExprFilterRule{
		tf:     tf,
		tables: map[string]*timodel.TableInfo{},
	}
	rule.eg = syncer.NewExprFilterGroup(dmctx, sessCtx,
		[]*dmconfig.ExpressionFilter{{
			Schema:             exprFilterSchema,
			Table:              exprFilterTable,
			InsertValueExpr:    cfg.IgnoreInsertValueExpr,
			UpdateNewValueExpr: cfg.IgnoreUpdateNewValueExpr,
			UpdateOldValueExpr: cfg.IgnoreUpdateOldValueExpr,
			DeleteValueExpr:    cfg.IgnoreDeleteValueExpr,
		}})
	f.rules = append(f.rules, rule)
	log.Info("fizz: add expression filter rule successfully.", zap.String("insetExpr", cfg.IgnoreInsertValueExpr))
	return nil
}

func (f *dmlExprFilter) getRules(schema, table string) []*dmlExprFilterRule {
	res := make([]*dmlExprFilterRule, 0)
	for _, rule := range f.rules {
		if rule.tf.MatchTable(schema, table) {
			log.Info("fizz: match table", zap.String("schema", schema), zap.String("table", table))
			res = append(res, rule)
		}
		log.Info("fizz:no match table", zap.String("schema", schema), zap.String("table", table))
	}
	log.Info("fizz: getRules", zap.String("schema", schema), zap.String("table", table), zap.Int("rule len", len(res)))
	return res
}

// shouldSkipDML skips dml event by sql expression.
func (f *dmlExprFilter) shouldSkipDML(row *model.RowChangedEvent, ti *timodel.TableInfo) (bool, error) {
	// It only be nil in test.
	if ti == nil {
		return false, nil
	}
	rules := f.getRules(row.Table.Schema, row.Table.Table)
	for _, rule := range rules {
		ignore, err := rule.shouldSkipDML(row, ti)
		if err != nil {
			return false, cerror.WrapError(cerror.ErrFailedToFilterDML, err)
		}
		if ignore {
			return true, nil
		}
	}
	return false, nil
}
