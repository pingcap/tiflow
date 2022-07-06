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
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/expression"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	tfilter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// dmlExprFilterRule only be used by dmlExprFilter.
type dmlExprFilterRule struct {
	mu     sync.Mutex
	tables map[string]*timodel.TableInfo

	insertExpr    expression.Expression
	updateOldExpr expression.Expression
	updateNewExpr expression.Expression
	deleteExpr    expression.Expression

	tableMatcher tfilter.Filter
	config       *config.EventFilterRule

	sessCtx sessionctx.Context
}

func newExprFilterRule(sessCtx sessionctx.Context, cfg *config.EventFilterRule) (*dmlExprFilterRule, error) {
	tf, err := tfilter.Parse(cfg.Matcher)
	if err != nil {
		log.Error("fizz", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
	}

	ret := &dmlExprFilterRule{
		tables:       make(map[string]*timodel.TableInfo),
		config:       cfg,
		tableMatcher: tf,
		sessCtx:      sessCtx,
	}

	return ret, nil
}

// verifyAndInitRule will verify and init the rule.
// It should only be called in dmlExprFilter's verify method.
func (r *dmlExprFilterRule) verify(tableInfos []*model.TableInfo) error {
	// verify expression filter rule.
	for _, ti := range tableInfos {
		if !r.tableMatcher.MatchTable(ti.TableName.Schema, ti.TableName.Table) {
			continue
		}
		if r.config.IgnoreInsertValueExpr != "" {
			e, err := r.getSimpleExprOfTable(r.config.IgnoreInsertValueExpr, ti.TableInfo)
			if err != nil {
				return err
			}
			r.insertExpr = e
		}
		if r.config.IgnoreUpdateOldValueExpr != "" {
			e, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateOldValueExpr, ti.TableInfo)
			if err != nil {
				return err
			}
			r.updateOldExpr = e
		}
		if r.config.IgnoreUpdateNewValueExpr != "" {
			e, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateNewValueExpr, ti.TableInfo)
			if err != nil {
				return err
			}
			r.updateNewExpr = e
		}
		if r.config.IgnoreDeleteValueExpr != "" {
			e, err := r.getSimpleExprOfTable(r.config.IgnoreDeleteValueExpr, ti.TableInfo)
			if err != nil {
				return err
			}
			r.deleteExpr = e
		}
	}
	return nil
}

func (r *dmlExprFilterRule) resetExpr() {
	r.insertExpr = nil
	r.updateOldExpr = nil
	r.updateNewExpr = nil
	r.deleteExpr = nil
}

// getInsertExprs returns the expression filter to filter INSERT events.
// This function will lazy calculate expressions if not initialized.
func (r *dmlExprFilterRule) getInsertExpr(ti *timodel.TableInfo) (expression.Expression, error) {
	if r.insertExpr != nil {
		return r.insertExpr, nil
	}
	if r.config.IgnoreInsertValueExpr != "" {
		log.Info("fizz: get insert expr", zap.String("insetExpr", r.config.IgnoreInsertValueExpr))
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreInsertValueExpr, ti)
		if err != nil {
			return nil, err
		}
		r.insertExpr = expr
	}
	return r.insertExpr, nil
}

func (r *dmlExprFilterRule) getUpdateOldExpr(ti *timodel.TableInfo) (expression.Expression, error) {
	if r.updateOldExpr != nil {
		return r.updateOldExpr, nil
	}

	if r.config.IgnoreUpdateOldValueExpr != "" {
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateOldValueExpr, ti)
		if err != nil {
			return nil, err
		}
		r.updateOldExpr = expr
	}
	return r.updateOldExpr, nil
}

func (r *dmlExprFilterRule) getUpdateNewExpr(ti *timodel.TableInfo) (expression.Expression, error) {
	if r.updateNewExpr != nil {
		return r.updateNewExpr, nil
	}

	if r.config.IgnoreUpdateNewValueExpr != "" {
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateNewValueExpr, ti)
		if err != nil {
			return nil, err
		}
		r.updateNewExpr = expr
	}
	return r.updateNewExpr, nil
}

func (r *dmlExprFilterRule) getDeleteExpr(ti *timodel.TableInfo) (expression.Expression, error) {
	if r.deleteExpr != nil {
		return r.deleteExpr, nil
	}

	if r.config.IgnoreDeleteValueExpr != "" {
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreDeleteValueExpr, ti)
		if err != nil {
			return nil, err
		}
		r.deleteExpr = expr
	}
	return r.deleteExpr, nil
}

func (r *dmlExprFilterRule) getSimpleExprOfTable(expr string, ti *timodel.TableInfo) (expression.Expression, error) {
	e, err := expression.ParseSimpleExprWithTableInfo(r.sessCtx, expr, ti)
	if err != nil {
		// if expression contains an unknown column, we return an expression that skips nothing
		if core.ErrUnknownColumn.Equal(err) {
			log.Error("meet unknown column when generating expression, return a FALSE expression instead",
				zap.String("expression", expr),
				zap.Error(err))
			return nil, cerror.ErrExpressionColumnNotFound.FastGenByArgs(getColumnFromError(err), ti.Name.O, expr)
		} else {
			log.Error("failed to parse expression", zap.Error(err))
			return nil, cerror.ErrExpressionParseFailed.FastGenByArgs(err, expr)
		}
	}
	return e, nil
}

func getColumnFromError(err error) string {
	if !core.ErrUnknownColumn.Equal(err) {
		return err.Error()
	}
	column := strings.TrimSpace(strings.TrimPrefix(err.Error(), "[planner:1054]Unknown column '"))
	column = strings.TrimSuffix(column, "' in 'expression'")
	return column
}

func (r *dmlExprFilterRule) shouldSkipDML(row *model.RowChangedEvent, ti *timodel.TableInfo) (bool, error) {
	table := &tfilter.Table{Schema: row.Table.Schema, Name: row.Table.Table}
	tableName := table.String()

	r.mu.Lock()
	defer r.mu.Unlock()

	// If one table's tableInfo was updated, we need to reset this rule.
	if oldTi, ok := r.tables[tableName]; ok {
		if ti.UpdateTS != oldTi.UpdateTS {
			r.tables[tableName] = ti.Clone()
			r.resetExpr()
		}
	} else {
		r.tables[tableName] = ti.Clone()
	}

	switch {
	case row.IsInsert():
		log.Info("fizz: insert event")
		exprs, err := r.getInsertExpr(ti)
		if err != nil {
			return false, err
		}
		log.Info("fizz: got expressions", zap.Any("expressions", exprs))
		return r.skipDMLByExpression(row.RowChangedDatums.RowDatums, exprs)
	case row.IsUpdate():
		oldExprs, err := r.getUpdateOldExpr(ti)
		if err != nil {
			return false, err
		}
		newExprs, err := r.getUpdateNewExpr(ti)
		if err != nil {
			return false, err
		}
		ignoreOld, err := r.skipDMLByExpression(row.RowChangedDatums.PreRowDatums, oldExprs)
		if err != nil {
			return false, err
		}
		ignoreNew, err := r.skipDMLByExpression(row.RowChangedDatums.RowDatums, newExprs)
		if err != nil {
			return false, err
		}
		return ignoreOld || ignoreNew, nil
	case row.IsDelete():
		exprs, err := r.getDeleteExpr(ti)
		if err != nil {
			return false, err
		}
		return r.skipDMLByExpression(row.RowChangedDatums.PreRowDatums, exprs)
	default:
		log.Warn("unknown row changed event type")
	}
	return false, nil
}

func (r *dmlExprFilterRule) skipDMLByExpression(rowData []types.Datum, expr expression.Expression) (bool, error) {
	log.Info("fizz: skipDMLByExpression", zap.Any("rowData", rowData), zap.Any("exprs", expr))
	if len(rowData) == 0 || expr == nil {
		return false, nil
	}

	row := chunk.MutRowFromDatums(rowData).ToRow()
	log.Info("fizz", zap.Int("row column len", row.Len()))
	log.Info("fizz", zap.Any("row ", row))

	log.Info("fizz: filter by expr", zap.String("expr", expr.String()))
	d, err := expr.Eval(row)
	if err != nil {
		log.Error("fizz:failed to eval expression", zap.Error(err))
		return false, err
	}
	if d.GetInt64() == 1 {
		log.Info("fizz: expr eval ", zap.Int64("eval", d.GetInt64()))
		return true, nil
	}
	log.Info("fizz: expr eval ", zap.Int64("eval", d.GetInt64()))

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
	for _, rule := range cfg.EventFilters {
		err := res.addRule(sessCtx, rule)
		if err != nil {
			log.Error("fizz", zap.Error(err))
			return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
		}
	}
	log.Info("fizz: new expression filter successfully.")
	return res, nil
}

func (f *dmlExprFilter) addRule(sessCtx sessionctx.Context, cfg *config.EventFilterRule) error {
	rule, err := newExprFilterRule(sessCtx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	f.rules = append(f.rules, rule)
	log.Info("fizz: add expression filter rule successfully.", zap.String("insetExpr", cfg.IgnoreInsertValueExpr))
	return nil
}

// verify checks if all rules in this filter is valid.
func (f *dmlExprFilter) verify(tableInfos []*model.TableInfo) error {
	for _, rule := range f.rules {
		err := rule.verify(tableInfos)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (f *dmlExprFilter) getRules(schema, table string) []*dmlExprFilterRule {
	res := make([]*dmlExprFilterRule, 0)
	for _, rule := range f.rules {
		if rule.tableMatcher.MatchTable(schema, table) {
			log.Info("fizz: match table", zap.String("schema", schema), zap.String("table", table))
			res = append(res, rule)
		} else {
			log.Info("fizz:no match table", zap.String("schema", schema), zap.String("table", table))
		}
	}
	log.Info("fizz: getRules", zap.String("schema", schema), zap.String("table", table), zap.Int("rule len", len(res)))
	return res
}

// shouldSkipDML skips dml event by sql expression.
func (f *dmlExprFilter) shouldSkipDML(row *model.RowChangedEvent, ti *timodel.TableInfo) (bool, error) {
	// for defense purpose, normally the row and ti should not be nil.
	if ti == nil || row == nil {
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
