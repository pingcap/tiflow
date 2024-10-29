// Copyright 2021 PingCAP, Inc.
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

package syncer

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/config"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// ExprFilterGroup groups many related fields about expression filter.
type ExprFilterGroup struct {
	configs        map[string][]*config.ExpressionFilter // tableName -> raw config
	insertExprs    map[string][]expression.Expression    // tableName -> expr
	updateOldExprs map[string][]expression.Expression    // tableName -> expr
	updateNewExprs map[string][]expression.Expression    // tableName -> expr
	deleteExprs    map[string][]expression.Expression    // tableName -> expr

	hasInsertFilter map[string]struct{} // set(tableName)
	hasUpdateFilter map[string]struct{} // set(tableName)
	hasDeleteFilter map[string]struct{} // set(tableName)

	tidbCtx sessionctx.Context
	logCtx  *tcontext.Context
}

// NewExprFilterGroup creates an ExprFilterGroup.
func NewExprFilterGroup(logCtx *tcontext.Context, tidbCtx sessionctx.Context, exprConfig []*config.ExpressionFilter) *ExprFilterGroup {
	ret := &ExprFilterGroup{
		configs:         map[string][]*config.ExpressionFilter{},
		insertExprs:     map[string][]expression.Expression{},
		updateOldExprs:  map[string][]expression.Expression{},
		updateNewExprs:  map[string][]expression.Expression{},
		deleteExprs:     map[string][]expression.Expression{},
		hasInsertFilter: map[string]struct{}{},
		hasUpdateFilter: map[string]struct{}{},
		hasDeleteFilter: map[string]struct{}{},
		tidbCtx:         tidbCtx,
		logCtx:          logCtx,
	}
	for _, c := range exprConfig {
		tableName := dbutil.TableName(c.Schema, c.Table)
		ret.configs[tableName] = append(ret.configs[tableName], c)

		if c.InsertValueExpr != "" {
			ret.hasInsertFilter[tableName] = struct{}{}
		}
		if c.UpdateOldValueExpr != "" || c.UpdateNewValueExpr != "" {
			ret.hasUpdateFilter[tableName] = struct{}{}
		}
		if c.DeleteValueExpr != "" {
			ret.hasDeleteFilter[tableName] = struct{}{}
		}
	}
	return ret
}

// GetInsertExprs returns the expression filters for given table to filter INSERT events.
// This function will lazy calculate expressions if not initialized.
func (g *ExprFilterGroup) GetInsertExprs(table *filter.Table, ti *model.TableInfo) ([]expression.Expression, error) {
	tableID := utils.GenTableID(table)

	if ret, ok := g.insertExprs[tableID]; ok {
		return ret, nil
	}
	if _, ok := g.hasInsertFilter[tableID]; !ok {
		return nil, nil
	}

	for _, c := range g.configs[tableID] {
		if c.InsertValueExpr != "" {
			expr, err2 := getSimpleExprOfTable(g.tidbCtx, c.InsertValueExpr, ti, g.logCtx.L())
			if err2 != nil {
				// TODO: terror
				return nil, err2
			}
			g.insertExprs[tableID] = append(g.insertExprs[tableID], expr)
		}
	}
	return g.insertExprs[tableID], nil
}

// GetUpdateExprs returns two lists of expression filters for given table, to filter UPDATE events by old values and new
// values respectively. The two lists should have same length, and the corresponding expressions is AND logic.
// This function will lazy calculate expressions if not initialized.
func (g *ExprFilterGroup) GetUpdateExprs(table *filter.Table, ti *model.TableInfo) ([]expression.Expression, []expression.Expression, error) {
	tableID := utils.GenTableID(table)

	retOld, ok1 := g.updateOldExprs[tableID]
	retNew, ok2 := g.updateNewExprs[tableID]
	if ok1 || ok2 {
		return retOld, retNew, nil
	}

	if _, ok := g.hasUpdateFilter[tableID]; ok {
		for _, c := range g.configs[tableID] {
			if c.UpdateOldValueExpr == "" && c.UpdateNewValueExpr == "" {
				continue
			}
			if c.UpdateOldValueExpr != "" {
				expr, err := getSimpleExprOfTable(g.tidbCtx, c.UpdateOldValueExpr, ti, g.logCtx.L())
				if err != nil {
					// TODO: terror
					return nil, nil, err
				}
				g.updateOldExprs[tableID] = append(g.updateOldExprs[tableID], expr)
			} else {
				g.updateOldExprs[tableID] = append(g.updateOldExprs[tableID], expression.NewOne())
			}

			if c.UpdateNewValueExpr != "" {
				expr, err := getSimpleExprOfTable(g.tidbCtx, c.UpdateNewValueExpr, ti, g.logCtx.L())
				if err != nil {
					// TODO: terror
					return nil, nil, err
				}
				g.updateNewExprs[tableID] = append(g.updateNewExprs[tableID], expr)
			} else {
				g.updateNewExprs[tableID] = append(g.updateNewExprs[tableID], expression.NewOne())
			}
		}
	}

	return g.updateOldExprs[tableID], g.updateNewExprs[tableID], nil
}

// GetDeleteExprs returns the expression filters for given table to filter DELETE events.
// This function will lazy calculate expressions if not initialized.
func (g *ExprFilterGroup) GetDeleteExprs(table *filter.Table, ti *model.TableInfo) ([]expression.Expression, error) {
	tableID := utils.GenTableID(table)

	if ret, ok := g.deleteExprs[tableID]; ok {
		return ret, nil
	}
	if _, ok := g.hasDeleteFilter[tableID]; !ok {
		return nil, nil
	}

	for _, c := range g.configs[tableID] {
		if c.DeleteValueExpr != "" {
			expr, err2 := getSimpleExprOfTable(g.tidbCtx, c.DeleteValueExpr, ti, g.logCtx.L())
			if err2 != nil {
				// TODO: terror
				return nil, err2
			}
			g.deleteExprs[tableID] = append(g.deleteExprs[tableID], expr)
		}
	}
	return g.deleteExprs[tableID], nil
}

// ResetExprs deletes the expressions generated before. This should be called after table structure changed.
func (g *ExprFilterGroup) ResetExprs(table *filter.Table) {
	tableID := utils.GenTableID(table)
	delete(g.insertExprs, tableID)
	delete(g.updateOldExprs, tableID)
	delete(g.updateNewExprs, tableID)
	delete(g.deleteExprs, tableID)
}

// SkipDMLByExpression returns true when given row matches the expr, which means this row should be skipped.
func SkipDMLByExpression(ctx sessionctx.Context, row []interface{}, expr expression.Expression, upstreamCols []*model.ColumnInfo) (bool, error) {
	// TODO: add MetricsProxies
	data, err := utils.AdjustBinaryProtocolForDatum(ctx, row, upstreamCols)
	if err != nil {
		return false, err
	}
	r := chunk.MutRowFromDatums(data).ToRow()

	d, err := expr.Eval(ctx.GetExprCtx().GetEvalCtx(), r)
	if err != nil {
		return false, err
	}
	return d.GetInt64() == 1, nil
}

// getSimpleExprOfTable returns an expression of given `expr` string, using the table structure that is tracked before.
func getSimpleExprOfTable(ctx sessionctx.Context, expr string, ti *model.TableInfo, logger log.Logger) (expression.Expression, error) {
	// TODO: use upstream timezone?
	e, err := expression.ParseSimpleExprWithTableInfo(ctx.GetExprCtx(), expr, ti)
	if err != nil {
		// if expression contains an unknown column, we return an expression that skips nothing
		if plannererrors.ErrUnknownColumn.Equal(err) {
			logger.Warn("meet unknown column when generating expression, return a FALSE expression instead",
				zap.String("expression", expr),
				zap.Error(err))
			e = expression.NewZero()
		} else {
			return nil, err
		}
	}

	return e, nil
}
