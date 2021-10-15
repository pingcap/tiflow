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
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	onlineddl "github.com/pingcap/dm/syncer/online-ddl-tools"
)

func (s *Syncer) skipQueryEvent(tables []*filter.Table, stmt ast.StmtNode, sql string) (bool, error) {
	if utils.IsBuildInSkipDDL(sql) {
		return true, nil
	}
	et := bf.AstToDDLEvent(stmt)
	for _, table := range tables {
		needSkip, err := s.skipOneEvent(table, et, sql)
		if err != nil || needSkip {
			return needSkip, err
		}
	}
	return false, nil
}

func (s *Syncer) skipRowsEvent(table *filter.Table, eventType replication.EventType) (bool, error) {
	// skip un-realTable
	if s.onlineDDL != nil && s.onlineDDL.TableType(table.Name) != onlineddl.RealTable {
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
		if s.skipByTable(table) {
			return true, nil
		}
		return false, terror.ErrSyncerUnitInvalidReplicaEvent.Generate(eventType)
	}
	return s.skipOneEvent(table, et, "")
}

// skipSQLByPattern skip unsupported sql in tidb and global sql-patterns in binlog-filter config file.
func (s *Syncer) skipSQLByPattern(sql string) (bool, error) {
	if utils.IsBuildInSkipDDL(sql) {
		return true, nil
	}
	action, err := s.binlogFilter.Filter("", "", bf.NullEvent, sql)
	if err != nil {
		return false, terror.Annotatef(terror.ErrSyncerUnitBinlogEventFilter.New(err.Error()), "skip query %s", sql)
	}
	return action == bf.Ignore, nil
}

// skipOneEvent will return true when
// - any schema of table names is system schema.
// - any table name doesn't pass block-allow list.
// - type of SQL doesn't pass binlog-filter.
// - pattern of SQL doesn't pass binlog-filter.
func (s *Syncer) skipOneEvent(table *filter.Table, et bf.EventType, sql string) (bool, error) {
	if s.skipByTable(table) {
		return true, nil
	}
	if s.binlogFilter == nil {
		return false, nil
	}
	action, err := s.binlogFilter.Filter(table.Schema, table.Name, et, sql)
	if err != nil {
		return false, terror.Annotatef(terror.ErrSyncerUnitBinlogEventFilter.New(err.Error()), "skip event %s on %v", et, table)
	}
	return action == bf.Ignore, nil
}

func (s *Syncer) skipByTable(table *filter.Table) bool {
	if filter.IsSystemSchema(table.Schema) {
		return true
	}
	tables := s.baList.Apply([]*filter.Table{table})
	return len(tables) == 0
}
