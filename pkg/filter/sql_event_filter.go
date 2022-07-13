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
	"github.com/pingcap/tidb/parser"
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
	// dmlQuery is a place holder to call binlog filter to filter dml event.
	dmlQuery = ""
	// caseSensitive is use to create bf.BinlogEvent.
	caseSensitive = false
)

// sqlEventRule only be used by sqlEventFilter.
type sqlEventRule struct {
	tf tfilter.Filter
	bf *bf.BinlogEvent
}

func newSQLEventFilterRule(cfg *config.EventFilterRule) (*sqlEventRule, error) {
	tf, err := tfilter.Parse(cfg.Matcher)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, cfg.Matcher)
	}

	res := &sqlEventRule{
		tf: tf,
	}

	if err := verifyIgnoreEvents(cfg.IgnoreEvent); err != nil {
		return nil, err
	}

	bfRule := &bf.BinlogEventRule{
		SchemaPattern: binlogFilterSchema,
		TablePattern:  binlogFilterTable,
		Events:        cfg.IgnoreEvent,
		SQLPattern:    cfg.IgnoreSQL,
		Action:        bf.Ignore,
	}

	res.bf, err = bf.NewBinlogEvent(caseSensitive, []*bf.BinlogEventRule{bfRule})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, "failed to create binlog event filter")
	}

	return res, nil
}

func verifyIgnoreEvents(types []bf.EventType) error {
	typesMap := make(map[bf.EventType]struct{}, len(supportedEventTypes))
	for _, et := range supportedEventTypes {
		typesMap[et] = struct{}{}
	}
	for _, et := range types {
		if _, ok := typesMap[et]; !ok {
			return cerror.ErrInvalidIgnoreEventType.GenWithStackByArgs(string(et))
		}
	}
	return nil
}

// sqlEventFilter is a filter that filters DDL/DML event by its type or query.
type sqlEventFilter struct {
	p     *parser.Parser
	rules []*sqlEventRule
}

func newSQLEventFilter(cfg *config.FilterConfig) (*sqlEventFilter, error) {
	res := &sqlEventFilter{
		p: parser.New(),
	}
	for _, rule := range cfg.EventFilters {
		if err := res.addRule(rule); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return res, nil
}

func (f *sqlEventFilter) addRule(cfg *config.EventFilterRule) error {
	rule, err := newSQLEventFilterRule(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	f.rules = append(f.rules, rule)
	return nil
}

func (f *sqlEventFilter) getRules(schema, table string) []*sqlEventRule {
	res := make([]*sqlEventRule, 0)
	for _, rule := range f.rules {
		if len(table) == 0 {
			if rule.tf.MatchSchema(schema) {
				res = append(res, rule)
			}
		} else {
			if rule.tf.MatchTable(schema, table) {
				res = append(res, rule)
			}
		}
	}
	return res
}

// skipDDLEvent skips ddl event by its type and query.
func (f *sqlEventFilter) shouldSkipDDL(ddl *model.DDLEvent) (bool, error) {
	evenType, err := ddlToEventType(f.p, ddl.Query)
	if err != nil {
		return false, err
	}
	if evenType == bf.NullEvent {
		log.Warn("sql event filter unsupported ddl type, do nothing",
			zap.String("type", ddl.Type.String()),
			zap.String("query", ddl.Query))
		return false, nil
	}

	rules := f.getRules(ddl.TableInfo.Schema, ddl.TableInfo.Table)
	for _, rule := range rules {
		action, err := rule.bf.Filter(binlogFilterSchema, binlogFilterTable, evenType, ddl.Query)
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
		// It should never happen.
		log.Warn("unknown row changed event type")
		return false, nil
	}
	rules := f.getRules(event.Table.Schema, event.Table.Table)
	for _, rule := range rules {
		action, err := rule.bf.Filter(binlogFilterSchema, binlogFilterTable, et, dmlQuery)
		if err != nil {
			return false, cerror.WrapError(cerror.ErrFailedToFilterDML, err, event)
		}
		if action == bf.Ignore {
			return true, nil
		}
	}
	return false, nil
}

var supportedEventTypes = []bf.EventType{
	bf.AllDML,
	bf.AllDDL,

	// dml events
	bf.InsertEvent,
	bf.UpdateEvent,
	bf.DeleteEvent,

	// ddl events
	bf.CreateSchema,
	bf.CreateDatabase,
	bf.DropSchema,
	bf.DropDatabase,
	bf.CreateTable,
	bf.DropTable,
	bf.AddIndex,
	bf.CreateIndex,
	bf.DropIndex,
	bf.TruncateTable,
	bf.RenameTable,
	bf.CreateView,
	bf.DropView,
	bf.AlterTable, // not supported yet
}
