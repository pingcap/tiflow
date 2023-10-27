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

package transformer

import (
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// Transformer is the interface for transform the event.
type Transformer interface {
	Match(table *model.TableName) bool
	Transform(event *model.RowChangedEvent) error
}

type ColumnSelectors []*columnSelector

func (c ColumnSelectors) Match(_ *model.TableName) bool {
	return true
}

func (c ColumnSelectors) Transform(event *model.RowChangedEvent) error {
	for _, selector := range c {
		if selector.Match(event.Table) {
			return selector.Transform(event)
		}
	}
	return nil
}

type columnSelector struct {
	tableF  filter.Filter
	columnM filter.ColumnFilter
}

func newColumnSelector(
	rule *config.ColumnSelector, caseSensitive bool,
) (*columnSelector, error) {
	tableM, err := filter.Parse(rule.Matcher)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, rule.Matcher)
	}
	if !caseSensitive {
		tableM = filter.CaseInsensitive(tableM)
	}
	columnM, err := filter.ParseColumnFilter(rule.Columns)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, rule.Columns)
	}
	return &columnSelector{
		tableF:  tableM,
		columnM: columnM,
	}, nil
}

// Match implements Transformer interface
func (s *columnSelector) Match(table *model.TableName) bool {
	return s.tableF.MatchTable(table.Schema, table.Table)
}

// Transform implements Transformer interface
func (s *columnSelector) Transform(event *model.RowChangedEvent) error {
	// the event does not match the table filter, skip it
	if !s.tableF.MatchTable(event.Table.Schema, event.Table.Table) {
		return nil
	}

	for _, column := range event.Columns {
		if !s.columnM.MatchColumn(column.Name) {
			column.Value = nil
		}
	}

	for _, column := range event.PreColumns {
		if !s.columnM.MatchColumn(column.Name) {
			column.Value = nil
		}
	}
	return nil
}

// NewColumnSelector return a column selector
func NewColumnSelector(cfg *config.ReplicaConfig) (ColumnSelectors, error) {
	result := make(ColumnSelectors, 0, len(cfg.Sink.ColumnSelectors))
	for _, r := range cfg.Sink.ColumnSelectors {
		selector, err := newColumnSelector(r, cfg.CaseSensitive)
		if err != nil {
			return nil, err
		}
		result = append(result, selector)
	}

	return result, nil
}
