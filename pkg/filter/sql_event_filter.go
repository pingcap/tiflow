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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb/parser"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	tfilter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	// binlogFilterSchemaPlaceholder is a place holder for schema name in binlog filter.
	// Since we use table filter in rule as a matcher to match a dml/ddl event's schema and table,
	// so we don't need to care about schema name when we calling binlog filter's method,
	// we just use this place holder to call binlog filter's method whenever we need pass a schema.
	binlogFilterSchemaPlaceholder = "binlogFilterSchema"
	// binlogFilterTablePlaceholder is a place holder for table name in binlog filter.
	// The reason we need it is the same as binlogFilterSchemaPlaceholder.
	binlogFilterTablePlaceholder = "binlogFilterTable"
	// dmlQuery is a place holder to call binlog filter to filter dml event.
	dmlQuery = ""
	// caseSensitive is use to create bf.BinlogEvent.
	caseSensitive = false
)

// sqlEventRule only be used by sqlEventFilter.
type sqlEventRule struct {
	// we use table filter to match a dml/ddl event's schema and table.
	// since binlog filter does not support syntax like `!test.t1`,
	// which means not match `test.t1`.
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
		SchemaPattern: binlogFilterSchemaPlaceholder,
		TablePattern:  binlogFilterTablePlaceholder,
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
	// Please be careful, parser.Parser is not thread safe.
	pLock sync.Mutex
	// Currently, parser is only used to parse ddl query.
	// So we can use a lock to protect it.
	// If we want to use it to parse dml query in the future,
	// we should create a parser for each goroutine.
	ddlParser *parser.Parser
	rules     []*sqlEventRule
}

func newSQLEventFilter(cfg *config.FilterConfig, sqlMode string) (*sqlEventFilter, error) {
	p := parser.New()
	mode, err := mysql.GetSQLMode(sqlMode)
	if err != nil {
		log.Error("failed to get sql mode", zap.Error(err))
		return nil, cerror.ErrInvalidReplicaConfig.FastGenByArgs(fmt.Sprintf("invalid sqlMode %s", sqlMode))
	}
	p.SetSQLMode(mode)

	res := &sqlEventFilter{
		ddlParser: p,
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
func (f *sqlEventFilter) shouldSkipDDL(
	ddlType timodel.ActionType, schema, table, query string,
) (bool, error) {
	log.Info("sql event filter handle ddl event",
		zap.Any("ddlType", ddlType), zap.String("schema", schema),
		zap.String("table", table), zap.String("query", query))
	f.pLock.Lock()
	evenType, err := ddlToEventType(f.ddlParser, query, ddlType)
	f.pLock.Unlock()
	if err != nil {
		return false, err
	}
	if evenType == bf.NullEvent {
		log.Warn("sql event filter unsupported ddl type, do nothing",
			zap.String("type", ddlType.String()),
			zap.String("query", query))
		return false, nil
	}

	rules := f.getRules(schema, table)
	for _, rule := range rules {
		action, err := rule.bf.Filter(
			binlogFilterSchemaPlaceholder,
			binlogFilterTablePlaceholder,
			evenType, query)
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
		action, err := rule.bf.Filter(binlogFilterSchemaPlaceholder, binlogFilterTablePlaceholder, et, dmlQuery)
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
	bf.RenameTable,
	bf.TruncateTable,
	bf.AlterTable,
	bf.CreateView,
	bf.DropView,
	bf.AddTablePartition,
	bf.DropTablePartition,
	bf.TruncateTablePartition,
}
