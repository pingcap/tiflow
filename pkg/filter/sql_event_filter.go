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
	"github.com/pingcap/log"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
)

// fizz:负责通过 ddl、dml 事件类型来过滤事件
type sqlEventFilter struct {
	binlogFilter *bf.BinlogEvent
}

func newSQLEventFilter(caseSensitive bool, cfg *config.FilterConfig) (*sqlEventFilter, error) {
	rule, err := configToBinlogEventRule(cfg)
	if err != nil {
		return nil, err
	}
	binlogFilter, err := bf.NewBinlogEvent(caseSensitive, rule)
	if err != nil {
		return nil, err
	}
	return &sqlEventFilter{binlogFilter: binlogFilter}, nil
}

// skipDDLEvent skips ddl job by its type and query
func (f *sqlEventFilter) skipDDLEvent(event *model.DDLEvent) (bool, error) {
	evenType := jobTypeToEventType(event.Type)
	action, err := f.binlogFilter.Filter(event.TableInfo.Schema, event.TableInfo.Table, evenType, event.Query)
	if err != nil {
		// fizz: complete this error
		return false, err
	}
	return action == bf.Ignore, nil
}

// skipDMLEvent skips dml event by its EventType.
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
		// fizz: complete this error
		return false, err
	}
	return action == bf.Ignore, nil
}
