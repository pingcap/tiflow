// Copyright 2023 PingCAP, Inc.
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

package columnselector

import (
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher/partition"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
)

type selector struct {
	tableF  filter.Filter
	columnM filter.ColumnFilter
}

func newSelector(
	rule *config.ColumnSelector, caseSensitive bool,
) (*selector, error) {
	tableM, err := filter.Parse(rule.Matcher)
	if err != nil {
		return nil, errors.WrapError(errors.ErrFilterRuleInvalid, err, rule.Matcher)
	}
	if !caseSensitive {
		tableM = filter.CaseInsensitive(tableM)
	}
	columnM, err := filter.ParseColumnFilter(rule.Columns)
	if err != nil {
		return nil, errors.WrapError(errors.ErrFilterRuleInvalid, err, rule.Columns)
	}

	return &selector{
		tableF:  tableM,
		columnM: columnM,
	}, nil
}

// Match implements Transformer interface
func (s *selector) Match(schema, table string) bool {
	return s.tableF.MatchTable(schema, table)
}

// Apply implements Transformer interface
// return error if the given event cannot match the selector, or the column cannot be filtered out.
// the caller's should make sure the given event match the selector first before apply it.
func (s *selector) Apply(event *model.RowChangedEvent) error {
	// defensive check, this should not happen.
	if !s.Match(event.Table.Schema, event.Table.Table) {
		return errors.ErrColumnSelectorFailed.GenWithStack(
			"the given event does not match the column selector, table: %v", event.Table)
	}

	for idx, column := range event.Columns {
		if s.columnM.MatchColumn(column.Name) {
			continue
		}
		if column.Flag.IsHandleKey() || column.Flag.IsUniqueKey() {
			return errors.ErrColumnSelectorFailed.GenWithStack(
				"primary key or unique key cannot be filtered out by the column selector, "+
					"table: %v, column: %s", event.Table, column.Name)
		}
		event.Columns[idx] = nil
	}

	for idx, column := range event.PreColumns {
		if s.columnM.MatchColumn(column.Name) {
			continue
		}
		if column.Flag.IsHandleKey() || column.Flag.IsUniqueKey() {
			return errors.ErrColumnSelectorFailed.GenWithStack(
				"primary key or unique key cannot be filtered out by the column selector, "+
					"table: %v, column: %s", event.Table, column.Name)
		}
		event.PreColumns[idx] = nil
	}

	return nil
}

// ColumnSelector manages an array of selectors, the first selector match the given
// event is used to select out columns.
type ColumnSelector struct {
	selectors []*selector
}

// New return a column selector
func New(cfg *config.ReplicaConfig) (*ColumnSelector, error) {
	selectors := make([]*selector, 0, len(cfg.Sink.ColumnSelectors))
	for _, r := range cfg.Sink.ColumnSelectors {
		selector, err := newSelector(r, cfg.CaseSensitive)
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, selector)
	}

	return &ColumnSelector{
		selectors: selectors,
	}, nil
}

// Apply the column selector to the given event.
func (c *ColumnSelector) Apply(event *model.RowChangedEvent) error {
	for _, s := range c.selectors {
		if s.Match(event.Table.Schema, event.Table.Table) {
			return s.Apply(event)
		}
	}
	return nil
}

// VerifyTables return the error if any given table cannot satisfy the column selector constraints.
// 1. if the column is filter out, it must not be a part of handle key or the unique key.
// 2. if the filtered out column is used in the column dispatcher, return error.
func (c *ColumnSelector) VerifyTables(
	infos []*model.TableInfo, eventRouter *dispatcher.EventRouter,
) error {
	if len(c.selectors) == 0 {
		return nil
	}

	for _, table := range infos {
		for _, s := range c.selectors {
			if !s.Match(table.TableName.Schema, table.TableName.Table) {
				continue
			}
			for columnID, flag := range table.ColumnsFlag {
				columnInfo, ok := table.GetColumnInfo(columnID)
				if !ok {
					return errors.ErrColumnSelectorFailed.GenWithStack(
						"column not found when verify the table for the column selector, table: %v, column: %s",
						table.TableName, columnInfo.Name)
				}

				if s.columnM.MatchColumn(columnInfo.Name.O) {
					continue
				}
				// the column is filter out.
				if flag.IsHandleKey() || flag.IsUniqueKey() {
					return errors.ErrColumnSelectorFailed.GenWithStack(
						"primary key or unique key cannot be filtered out by the column selector, "+
							"table: %v, column: %s", table.TableName, columnInfo.Name)
				}

				partitionDispatcher := eventRouter.GetPartitionDispatcher(table.TableName.Schema, table.TableName.Table)
				switch v := partitionDispatcher.(type) {
				case *partition.ColumnsDispatcher:
					for _, col := range v.Columns {
						if col == columnInfo.Name.O {
							return errors.ErrColumnSelectorFailed.GenWithStack(
								"the filtered out column is used in the column dispatcher, "+
									"table: %v, column: %s", table.TableName, columnInfo.Name)
						}
					}
				default:
				}
			}
		}
	}

	return nil
}

// VerifyColumn return true if the given `schema.table` column is matched.
func (c *ColumnSelector) VerifyColumn(schema, table, column string) bool {
	for _, s := range c.selectors {
		if !s.Match(schema, table) {
			continue
		}
		return s.columnM.MatchColumn(column)
	}
	return true
}
