// Copyright 2019 PingCAP, Inc.
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
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	onlineddl "github.com/pingcap/tiflow/dm/syncer/online-ddl-tools"
	bf "github.com/pingcap/tiflow/pkg/binlog-filter"
	"go.uber.org/zap"
)

// skipQueryEvent if skip by binlog-filter:
// * track the ddlInfo;
// * changes ddlInfo.originDDL to empty string.
func (s *Syncer) skipQueryEvent(qec *queryEventContext, ddlInfo *ddlInfo) (bool, error) {
	if utils.IsBuildInSkipDDL(qec.originSQL) {
		return true, nil
	}
	et := bf.AstToDDLEvent(ddlInfo.stmtCache)
	// get real tables before apply block-allow list
	realTables := make([]*filter.Table, 0, len(ddlInfo.sourceTables))
	for _, table := range ddlInfo.sourceTables {
		realTableName := table.Name
		if s.onlineDDL != nil {
			realTableName = s.onlineDDL.RealName(table.Name)
		}
		realTables = append(realTables, &filter.Table{
			Schema: table.Schema,
			Name:   realTableName,
		})
	}
	for _, table := range realTables {
		s.tctx.L().Debug("query event info", zap.String("event", "query"), zap.String("origin sql", qec.originSQL), zap.Stringer("table", table), zap.Stringer("ddl info", ddlInfo))
		if s.skipByTable(table) {
			s.tctx.L().Debug("skip event by balist")
			return true, nil
		}
		needSkip, err := s.skipByFilter(table, et, qec.originSQL)
		if err != nil {
			return needSkip, err
		}

		if needSkip {
			s.tctx.L().Debug("skip event by binlog filter")
			// In the case of online-ddl, if the generated table is skipped, track ddl will failed.
			err := s.trackDDL(qec.ddlSchema, ddlInfo, qec.eventContext)
			if err != nil {
				s.tctx.L().Warn("track ddl failed", zap.Stringer("ddl info", ddlInfo))
			}
			s.saveTablePoint(table, qec.lastLocation)
			s.tctx.L().Warn("track skipped ddl and return empty string", zap.String("origin sql", qec.originSQL), zap.Stringer("ddl info", ddlInfo))
			ddlInfo.originDDL = ""
			return true, nil
		}
	}
	return false, nil
}

func (s *Syncer) skipRowsEvent(table *filter.Table, eventType replication.EventType) (bool, error) {
	// skip un-realTable
	if s.onlineDDL != nil && s.onlineDDL.TableType(table.Name) != onlineddl.RealTable {
		return true, nil
	}
	if s.skipByTable(table) {
		return true, nil
	}
	var et bf.EventType
	switch eventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		et = bf.InsertEvent
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		et = bf.UpdateEvent
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		et = bf.DeleteEvent
	default:
		return false, terror.ErrSyncerUnitInvalidReplicaEvent.Generate(eventType)
	}
	return s.skipByFilter(table, et, "")
}

// skipSQLByPattern skip unsupported sql in tidb and global sql-patterns in binlog-filter config file.
func skipSQLByPattern(binlogFilter *bf.BinlogEvent, sql string) (bool, error) {
	if utils.IsBuildInSkipDDL(sql) {
		return true, nil
	}
	action, err := binlogFilter.Filter("", "", bf.NullEvent, sql)
	if err != nil {
		return false, terror.Annotatef(terror.ErrSyncerUnitBinlogEventFilter.New(err.Error()), "skip query %s", sql)
	}
	return action == bf.Ignore, nil
}

func (s *Syncer) skipByFilter(table *filter.Table, et bf.EventType, sql string) (bool, error) {
	return skipByFilter(s.binlogFilter, table, et, sql)
}

// skipByFilter returns true when
// * type of SQL doesn't pass binlog-filter.
// * pattern of SQL doesn't pass binlog-filter.
func skipByFilter(binlogFilter *bf.BinlogEvent, table *filter.Table, et bf.EventType, sql string) (bool, error) {
	if binlogFilter == nil {
		return false, nil
	}
	action, err := binlogFilter.Filter(table.Schema, table.Name, et, sql)
	if err != nil {
		return false, terror.Annotatef(terror.ErrSyncerUnitBinlogEventFilter.New(err.Error()), "skip event %s on %v", et, table)
	}
	switch action {
	case bf.Ignore:
		return true, nil
	case bf.Error:
		return false, terror.ErrSyncerUnitBinlogEventFilter.Generatef("event %s on %v", et, table)
	}
	return false, nil
}

func (s *Syncer) skipByTable(table *filter.Table) bool {
	return skipByTable(s.baList, table)
}

// skipByTable returns true when
// * any schema of table names is system schema.
// * any table name doesn't pass block-allow list.
func skipByTable(baList *filter.Filter, table *filter.Table) bool {
	if filter.IsSystemSchema(table.Schema) {
		return true
	}
	tables := baList.Apply([]*filter.Table{table})
	return len(tables) == 0
}
