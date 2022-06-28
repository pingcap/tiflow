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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type sqlEventFilter struct {
	binlogFilter *bf.BinlogEvent
}

func newSQLEventFilter(caseSensitive bool, cfg *config.FilterConfig) (*sqlEventFilter, error) {
	rule, err := configToBinlogEventRule(cfg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
	}
	binlogFilter, err := bf.NewBinlogEvent(caseSensitive, rule)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
	}
	return &sqlEventFilter{binlogFilter: binlogFilter}, nil
}

// skipDDLEvent skips ddl event by its type and query.
func (f *sqlEventFilter) skipDDLJob(ddl *model.DDLEvent) (bool, error) {
	evenType := jobTypeToEventType(ddl.Type)
	log.Info("fizz", zap.String("query", ddl.Query))
	action, err := f.binlogFilter.Filter(ddl.TableInfo.Schema, ddl.TableInfo.Table, evenType, ddl.Query)
	if err != nil {
		return false, errors.Trace(err)
	}
	return action == bf.Ignore, nil
}

// skipDMLEvent skips dml event by its type.
func (f *sqlEventFilter) skipDMLEvent(event *model.RowChangedEvent) (bool, error) {
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
	action, err := f.binlogFilter.Filter(event.Table.Schema, event.Table.Table, et, "")
	if err != nil {
		return false, errors.Trace(err)
	}
	return action == bf.Ignore, nil
}
