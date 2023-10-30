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

package column_selector

import (
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, rule.Matcher)
	}
	if !caseSensitive {
		tableM = filter.CaseInsensitive(tableM)
	}
	columnM, err := filter.ParseColumnFilter(rule.Columns)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, rule.Columns)
	}

	return &selector{
		tableF:  tableM,
		columnM: columnM,
	}, nil
}

// Match implements Transformer interface
func (s *selector) Match(table *model.TableName) bool {
	return s.tableF.MatchTable(table.Schema, table.Table)
}

// Apply implements Transformer interface
func (s *selector) Apply(event *model.RowChangedEvent) error {
	// the event does not match the table filter, skip it
	if !s.tableF.MatchTable(event.Table.Schema, event.Table.Table) {
		return nil
	}

	for i := 0; i < len(event.Columns); i++ {
		if !s.columnM.MatchColumn(event.Columns[i].Name) {
			event.Columns[i] = nil
		}
	}

	for i := 0; i < len(event.PreColumns); i++ {
		if !s.columnM.MatchColumn(event.PreColumns[i].Name) {
			event.PreColumns[i] = nil
		}
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

func (c *ColumnSelector) Match(_ *model.TableName) bool {
	return true
}

func (c *ColumnSelector) Apply(event *model.RowChangedEvent) error {
	for _, s := range c.selectors {
		if s.Match(event.Table) {
			return s.Apply(event)
		}
	}
	return nil
}
