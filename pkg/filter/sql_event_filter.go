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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	tfilter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	// binlogFilterSchema is a place holder for schema name in binlog filter.
	binlogFilterSchema = "binlogFilterSchema"
	// binlogFilterTable is a place holder for table name in binlog filter.
	binlogFilterTable = "binlogFilterTable"
)

// sqlEventRule only be used by sqlEventFilter.
type sqlEventRule struct {
	tf tfilter.Filter
	bf *bf.BinlogEvent
}

// sqlEventFilter is a filter that filters DDL/DML event by its type or query.
type sqlEventFilter struct {
	caseSensitive bool
	rules         []*sqlEventRule
}

func newSQLEventFilter(caseSensitive bool, cfg *config.FilterConfig) (*sqlEventFilter, error) {
	res := &sqlEventFilter{caseSensitive: caseSensitive}
	for _, rule := range cfg.EventFilters {
		if err := res.addRule(rule); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return res, nil
}

func (f *sqlEventFilter) addRule(cfg *config.EventFilterRule) error {
	tf, err := tfilter.Parse(cfg.Matcher)
	if err != nil {
		log.Error("fizz", zap.Error(err))
		return cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
	}

	rule := &sqlEventRule{
		tf: tf,
	}

	bfRule := &bf.BinlogEventRule{
		SchemaPattern: binlogFilterSchema,
		TablePattern:  binlogFilterTable,
		Events:        cfg.IgnoreEvent,
		SQLPattern:    cfg.IgnoreSQL,
		Action:        bf.Ignore,
	}

	bf, err := bf.NewBinlogEvent(f.caseSensitive, []*bf.BinlogEventRule{bfRule})
	if err != nil {
		log.Error("fizz", zap.Error(err))
		return cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
	}
	rule.bf = bf
	f.rules = append(f.rules, rule)
	return nil
}

func (f *sqlEventFilter) getRules(schema, table string) []*sqlEventRule {
	res := make([]*sqlEventRule, 0)
	for _, rule := range f.rules {
		if rule.tf.MatchTable(schema, table) {
			res = append(res, rule)
		}
	}
	return res
}

// skipDDLEvent skips ddl event by its type and query.
func (f *sqlEventFilter) shouldSkipDDL(ddl *model.DDLEvent) (bool, error) {
	evenType := jobTypeToEventType(ddl.Type)
	log.Info("fizz", zap.String("event", string(evenType)))
	log.Info("fizz", zap.String("query", ddl.Query))

	rules := f.getRules(ddl.TableInfo.Schema, ddl.TableInfo.Table)
	log.Info("fizz", zap.Int("rules number", len(rules)))
	for _, rule := range rules {
		action, err := rule.bf.Filter(binlogFilterSchema, binlogFilterTable, evenType, ddl.Query)
		log.Info("fizz", zap.String("action", string(action)))
		if err != nil {
			return false, errors.Trace(err)
		}
		if action == bf.Ignore {
			return true, nil
		}
	}
	return false, nil
}

// shouldSkipDML skips dml event by its type.
func (f *sqlEventFilter) shouldSkipDML(event *model.RowChangedEvent) (bool, error) {
	var et bf.EventType
	switch {
	case event.IsInsert():
		et = bf.InsertEvent
	case event.IsUpdate():
		et = bf.UpdateEvent
	case event.IsDelete():
		et = bf.DeleteEvent
	default:
		log.Warn("unknown row changed event type")
		return false, nil
	}
	rules := f.getRules(event.Table.Schema, event.Table.Table)
	for _, rule := range rules {
		action, err := rule.bf.Filter(binlogFilterSchema, binlogFilterTable, et, "")
		if err != nil {
			return false, errors.Trace(err)
		}
		if action == bf.Ignore {
			return true, nil
		}
	}
	return false, nil
}
