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
	Transform(event *model.RowChangedEvent) error
}

type columnSelector struct {
	tableF  filter.Filter
	columnM *columnMatcher
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
	columnM, err := newColumnMatcher(rule.Columns, caseSensitive)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, rule.Columns)
	}
	return &columnSelector{
		tableF:  tableM,
		columnM: columnM,
	}, nil
}

// Transform implements Transformer interface
func (s *columnSelector) Transform(event *model.RowChangedEvent) error {
	// the event does not match the table filter, skip it
	if !s.tableF.MatchTable(event.Table.Schema, event.Table.Table) {
		return nil
	}

	for _, column := range event.Columns {

	}

	for _, column := range event.PreColumns {

	}

	//// caution: after filter out columns, original columns should still keep at the same offset
	//// to prevent column dispatcher visit wrong column data.
	//return nil
}

// NewColumnSelector return a column selector
//func NewColumnSelector(cfg *config.ReplicaConfig) (*columnSelector, error) {
//	// If an event does not match any column selector rules in the config file,
//	// it won't be transformed, all columns match the *.* rule
//	ruleConfig := append(cfg.Sink.ColumnSelectors, &config.ColumnSelector{
//		Matcher: []string{"*.*"},
//		Columns: []string{"*.*"},
//	})
//
//	result := &columnSelector{
//		matchers: make([]*matcher, len(ruleConfig)),
//	}
//
//	for _, r := range ruleConfig {
//		tableM, err := filter.Parse(r.Matcher)
//		if err != nil {
//			return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, r.Matcher)
//		}
//		if !cfg.CaseSensitive {
//			tableM = filter.CaseInsensitive(tableM)
//		}
//		columnM, err := newColumnMatcher(r.Columns, cfg.CaseSensitive)
//		if err != nil {
//			return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, r.Columns)
//		}
//		result.matchers = append(result.matchers, &matcher{
//			tableF:  tableM,
//			columnM: columnM,
//		})
//	}
//
//	return result, nil
//}

type columnMatcher struct {
	matcher filter.Filter
}

func newColumnMatcher(columns []string, caseSensitive bool) (*columnMatcher, error) {
	matcher, err := filter.Parse(columns)
	if err != nil {
		return nil, err
	}
	if !caseSensitive {
		matcher = filter.CaseInsensitive(matcher)
	}
	return &columnMatcher{matcher: matcher}, nil
}

// Match return true if the column matches.
func (m *columnMatcher) Match(column string) bool {
	return m.matcher.MatchSchema(column)
}
