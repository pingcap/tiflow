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
	"fmt"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// dmlExprFilterRule only be used by dmlExprFilter.
// This struct is mostly a duplicate of `ExprFilterGroup` in dm/pkg/syncer,
// but have slightly changed to fit the usage of cdc.
type dmlExprFilterRule struct {
	mu sync.Mutex
	// Cache tableInfos to check if the table was changed.
	tables map[string]*model.TableInfo

	insertExprs    map[string]expression.Expression // tableName -> expr
	updateOldExprs map[string]expression.Expression // tableName -> expr
	updateNewExprs map[string]expression.Expression // tableName -> expr
	deleteExprs    map[string]expression.Expression // tableName -> expr

	tableMatcher tfilter.Filter
	// All tables in this rule share the same config.
	config *config.EventFilterRule

	sessCtx sessionctx.Context
}

func newExprFilterRule(
	sessCtx sessionctx.Context,
	cfg *config.EventFilterRule,
) (*dmlExprFilterRule, error) {
	tf, err := tfilter.Parse(cfg.Matcher)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, cfg.Matcher)
	}

	ret := &dmlExprFilterRule{
		tables:         make(map[string]*model.TableInfo),
		insertExprs:    make(map[string]expression.Expression),
		updateOldExprs: make(map[string]expression.Expression),
		updateNewExprs: make(map[string]expression.Expression),
		deleteExprs:    make(map[string]expression.Expression),
		config:         cfg,
		tableMatcher:   tf,
		sessCtx:        sessCtx,
	}
	return ret, nil
}

// verifyAndInitRule will verify and init the rule.
// It should only be called in dmlExprFilter's verify method.
func (r *dmlExprFilterRule) verify(tableInfos []*model.TableInfo, sqlMode string) error {
	// verify expression filter rule syntax.
	p := parser.New()
	mode, err := mysql.GetSQLMode(sqlMode)
	if err != nil {
		log.Error("failed to get sql mode", zap.Error(err))
		return cerror.ErrInvalidReplicaConfig.FastGenByArgs(fmt.Sprintf("invalid sqlMode %s", sqlMode))
	}
	p.SetSQLMode(mode)
	_, _, err = p.ParseSQL(completeExpression(r.config.IgnoreInsertValueExpr))
	if err != nil {
		log.Error("failed to parse expression", zap.Error(err))
		return cerror.ErrExpressionParseFailed.
			FastGenByArgs(r.config.IgnoreInsertValueExpr)
	}
	_, _, err = p.ParseSQL(completeExpression(r.config.IgnoreUpdateNewValueExpr))
	if err != nil {
		log.Error("failed to parse expression", zap.Error(err))
		return cerror.ErrExpressionParseFailed.
			FastGenByArgs(r.config.IgnoreUpdateNewValueExpr)
	}
	_, _, err = p.ParseSQL(completeExpression(r.config.IgnoreUpdateOldValueExpr))
	if err != nil {
		log.Error("failed to parse expression", zap.Error(err))
		return cerror.ErrExpressionParseFailed.
			FastGenByArgs(r.config.IgnoreUpdateOldValueExpr)
	}
	_, _, err = p.ParseSQL(completeExpression(r.config.IgnoreDeleteValueExpr))
	if err != nil {
		log.Error("failed to parse expression", zap.Error(err))
		return cerror.ErrExpressionParseFailed.
			FastGenByArgs(r.config.IgnoreDeleteValueExpr)
	}
	// verify expression filter rule.
	for _, ti := range tableInfos {
		tableName := ti.TableName.String()
		if !r.tableMatcher.MatchTable(ti.TableName.Schema, ti.TableName.Table) {
			continue
		}
		if r.config.IgnoreInsertValueExpr != "" {
			e, err := r.getSimpleExprOfTable(r.config.IgnoreInsertValueExpr, ti)
			if err != nil {
				return err
			}
			r.insertExprs[tableName] = e
		}
		if r.config.IgnoreUpdateOldValueExpr != "" {
			e, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateOldValueExpr, ti)
			if err != nil {
				return err
			}
			r.updateOldExprs[tableName] = e
		}
		if r.config.IgnoreUpdateNewValueExpr != "" {
			e, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateNewValueExpr, ti)
			if err != nil {
				return err
			}
			r.updateNewExprs[tableName] = e
		}
		if r.config.IgnoreDeleteValueExpr != "" {
			e, err := r.getSimpleExprOfTable(r.config.IgnoreDeleteValueExpr, ti)
			if err != nil {
				return err
			}
			r.deleteExprs[tableName] = e
		}
	}
	return nil
}

// The caller must hold r.mu.Lock() before calling this function.
func (r *dmlExprFilterRule) resetExpr(tableName string) {
	delete(r.insertExprs, tableName)
	delete(r.updateOldExprs, tableName)
	delete(r.updateNewExprs, tableName)
	delete(r.deleteExprs, tableName)
}

// getInsertExprs returns the expression filter to filter INSERT events.
// This function will lazy calculate expressions if not initialized.
func (r *dmlExprFilterRule) getInsertExpr(ti *model.TableInfo) (
	expression.Expression, error,
) {
	tableName := ti.TableName.String()
	if r.insertExprs[tableName] != nil {
		return r.insertExprs[tableName], nil
	}
	if r.config.IgnoreInsertValueExpr != "" {
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreInsertValueExpr, ti)
		if err != nil {
			return nil, err
		}
		r.insertExprs[tableName] = expr
	}
	return r.insertExprs[tableName], nil
}

func (r *dmlExprFilterRule) getUpdateOldExpr(ti *model.TableInfo) (
	expression.Expression, error,
) {
	tableName := ti.TableName.String()
	if r.updateOldExprs[tableName] != nil {
		return r.updateOldExprs[tableName], nil
	}

	if r.config.IgnoreUpdateOldValueExpr != "" {
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateOldValueExpr, ti)
		if err != nil {
			return nil, err
		}
		r.updateOldExprs[tableName] = expr
	}
	return r.updateOldExprs[tableName], nil
}

func (r *dmlExprFilterRule) getUpdateNewExpr(ti *model.TableInfo) (
	expression.Expression, error,
) {
	tableName := ti.TableName.String()
	if r.updateNewExprs[tableName] != nil {
		return r.updateNewExprs[tableName], nil
	}

	if r.config.IgnoreUpdateNewValueExpr != "" {
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateNewValueExpr, ti)
		if err != nil {
			return nil, err
		}
		r.updateNewExprs[tableName] = expr
	}
	return r.updateNewExprs[tableName], nil
}

func (r *dmlExprFilterRule) getDeleteExpr(ti *model.TableInfo) (
	expression.Expression, error,
) {
	tableName := ti.TableName.String()
	if r.deleteExprs[tableName] != nil {
		return r.deleteExprs[tableName], nil
	}

	if r.config.IgnoreDeleteValueExpr != "" {
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreDeleteValueExpr, ti)
		if err != nil {
			return nil, err
		}
		r.deleteExprs[tableName] = expr
	}
	return r.deleteExprs[tableName], nil
}

func (r *dmlExprFilterRule) getSimpleExprOfTable(
	expr string,
	ti *model.TableInfo,
) (expression.Expression, error) {
	e, err := expression.ParseSimpleExprWithTableInfo(r.sessCtx, expr, ti.TableInfo)
	if err != nil {
		// If an expression contains an unknown column,
		// we return an error and stop the changefeed.
		if core.ErrUnknownColumn.Equal(err) {
			log.Error("meet unknown column when generating expression",
				zap.String("expression", expr),
				zap.Error(err))
			return nil, cerror.ErrExpressionColumnNotFound.
				FastGenByArgs(getColumnFromError(err), ti.TableName.String(), expr)
		}
		log.Error("failed to parse expression", zap.Error(err))
		return nil, cerror.ErrExpressionParseFailed.FastGenByArgs(err, expr)
	}
	return e, nil
}

func (r *dmlExprFilterRule) shouldSkipDML(
	row *model.RowChangedEvent,
	rawRow model.RowChangedDatums,
	ti *model.TableInfo,
) (bool, error) {
	tableName := ti.TableName.String()

	r.mu.Lock()
	defer r.mu.Unlock()

	if oldTi, ok := r.tables[tableName]; ok {
		// If one table's tableInfo was updated, we need to reset this rule
		// and update the tableInfo in the cache.
		if ti.Version != oldTi.Version {
			r.tables[tableName] = ti.Clone()
			r.resetExpr(ti.TableName.String())
		}
	} else {
		r.tables[tableName] = ti.Clone()
	}

	switch {
	case row.IsInsert():
		exprs, err := r.getInsertExpr(ti)
		if err != nil {
			return false, err
		}
		return r.skipDMLByExpression(
			rawRow.RowDatums,
			exprs,
		)
	case row.IsUpdate():
		oldExprs, err := r.getUpdateOldExpr(ti)
		if err != nil {
			return false, err
		}
		newExprs, err := r.getUpdateNewExpr(ti)
		if err != nil {
			return false, err
		}
		ignoreOld, err := r.skipDMLByExpression(
			rawRow.PreRowDatums,
			oldExprs,
		)
		if err != nil {
			return false, err
		}
		ignoreNew, err := r.skipDMLByExpression(
			rawRow.RowDatums,
			newExprs,
		)
		if err != nil {
			return false, err
		}
		return ignoreOld || ignoreNew, nil
	case row.IsDelete():
		exprs, err := r.getDeleteExpr(ti)
		if err != nil {
			return false, err
		}
		return r.skipDMLByExpression(
			rawRow.PreRowDatums,
			exprs,
		)
	default:
		log.Warn("unknown row changed event type")
		return false, nil
	}
}

func (r *dmlExprFilterRule) skipDMLByExpression(
	rowData []types.Datum,
	expr expression.Expression,
) (bool, error) {
	if len(rowData) == 0 || expr == nil {
		return false, nil
	}

	row := chunk.MutRowFromDatums(rowData).ToRow()

	d, err := expr.Eval(r.sessCtx, row)
	if err != nil {
		log.Error("failed to eval expression", zap.Error(err))
		return false, errors.Trace(err)
	}
	if d.GetInt64() == 1 {
		return true, nil
	}
	return false, nil
}

func getColumnFromError(err error) string {
	if !core.ErrUnknownColumn.Equal(err) {
		return err.Error()
	}
	column := strings.TrimSpace(strings.TrimPrefix(err.Error(),
		"[planner:1054]Unknown column '"))
	column = strings.TrimSuffix(column, "' in 'expression'")
	return column
}

// dmlExprFilter is a filter that filters DML events by SQL expression.
type dmlExprFilter struct {
	rules   []*dmlExprFilterRule
	sqlMODE string
}

func newExprFilter(
	timezone string,
	cfg *config.FilterConfig,
	sqlMODE string,
) (*dmlExprFilter, error) {
	res := &dmlExprFilter{
		sqlMODE: sqlMODE,
	}
	sessCtx := utils.NewSessionCtx(map[string]string{
		"time_zone": timezone,
	})
	for _, rule := range cfg.EventFilters {
		err := res.addRule(sessCtx, rule)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (f *dmlExprFilter) addRule(
	sessCtx sessionctx.Context,
	cfg *config.EventFilterRule,
) error {
	rule, err := newExprFilterRule(sessCtx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	f.rules = append(f.rules, rule)
	return nil
}

// verify checks if all rules in this filter is valid.
func (f *dmlExprFilter) verify(tableInfos []*model.TableInfo) error {
	for _, rule := range f.rules {
		err := rule.verify(tableInfos, f.sqlMODE)
		if err != nil {
			log.Error("failed to verify expression filter rule", zap.Error(err))
			return errors.Trace(err)
		}
	}
	return nil
}

func (f *dmlExprFilter) getRules(schema, table string) []*dmlExprFilterRule {
	res := make([]*dmlExprFilterRule, 0)
	for _, rule := range f.rules {
		if rule.tableMatcher.MatchTable(schema, table) {
			res = append(res, rule)
		}
	}
	return res
}

// shouldSkipDML skips dml event by sql expression.
func (f *dmlExprFilter) shouldSkipDML(
	row *model.RowChangedEvent,
	rawRow model.RowChangedDatums,
	ti *model.TableInfo,
) (bool, error) {
	// for defense purpose, normally the row and ti should not be nil.
	if ti == nil || row == nil || rawRow.IsEmpty() {
		return false, nil
	}
	rules := f.getRules(row.Table.Schema, row.Table.Table)
	for _, rule := range rules {
		ignore, err := rule.shouldSkipDML(row, rawRow, ti)
		if err != nil {
			if cerror.ShouldFailChangefeed(err) {
				return false, err
			}
			return false, cerror.WrapError(cerror.ErrFailedToFilterDML, err, row)
		}
		if ignore {
			return true, nil
		}
	}
	return false, nil
}
