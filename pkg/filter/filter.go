// Copyright 2020 PingCAP, Inc.
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
	timodel "github.com/pingcap/tidb/parser/model"
	tfilter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// All methods of Filter are safe for concurrent use.
type Filter interface {
	// ShouldIgnoreDMLEvent return true and nil if the DML event should be ignored.
	ShouldIgnoreDMLEvent(dml *model.RowChangedEvent, tableInfo *timodel.TableInfo) (bool, error)
	// ShouldIgnoreDDLEvent return true and nil if the DDL event should be ignored.
	ShouldIgnoreDDLEvent(ddl *model.DDLEvent) (bool, error)
	// ShouldDiscardDDL returns true if this DDL should be discarded.
	ShouldDiscardDDL(ddlType timodel.ActionType) bool
	// ShouldIgnoreTable return true if the table should be ignored.
	ShouldIgnoreTable(schema, table string) bool
}

// filter implements Filter.
type filter struct {
	// tableFilter is used to filter in dml/ddl event by table name.
	tableFilter tfilter.Filter
	// dmlExprFilter is used to filter out dml event by its columns value.
	dmlExprFilter *dmlExprFilter
	// sqlEventFilter is used to filter out dml/ddl event by its type or query.
	sqlEventFilter *sqlEventFilter
	// ignoreTxnStartTs is used to filter out dml/ddl event by its starsTs.
	ignoreTxnStartTs []uint64
	ddlAllowlist     []timodel.ActionType
}

// NewFilter creates a filter.
func NewFilter(cfg *config.ReplicaConfig) (Filter, error) {
	f, err := VerifyRules(cfg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
	}

	if !cfg.CaseSensitive {
		f = tfilter.CaseInsensitive(f)
	}
	// fizz: we need to set correct timezone
	// fizz: we need to check if we should new a dmlExprFilter
	dmlExprFilter, err := newExprFilter("system", cfg.Filter)
	// fizz: we need to complete the error handler
	if err != nil {
		return nil, err
	}
	sqlEventFilter, err := newSQLEventFilter(cfg.CaseSensitive, cfg.Filter)
	// fizz: we need to complete the error handler
	if err != nil {
		return nil, err
	}
	return &filter{
		tableFilter:      f,
		dmlExprFilter:    dmlExprFilter,
		sqlEventFilter:   sqlEventFilter,
		ignoreTxnStartTs: cfg.Filter.IgnoreTxnStartTs,
		ddlAllowlist:     cfg.Filter.DDLAllowlist,
	}, nil
}

// ShouldIgnoreDMLEvent checks if a DML event should be ignore by conditions below:
// 0. By startTs.
// 1. By table name.
// 2. By type.
// 3. By columns value.
func (f *filter) ShouldIgnoreDMLEvent(
	dml *model.RowChangedEvent,
	ti *timodel.TableInfo,
) (bool, error) {
	if f.shouldIgnoreStartTs(dml.StartTs) {
		return true, nil
	}

	if f.ShouldIgnoreTable(dml.Table.Schema, dml.Table.Table) {
		return true, nil
	}

	ignoreByEventType, err := f.sqlEventFilter.shouldSkipDML(dml)
	if err != nil {
		return false, err
	}
	if ignoreByEventType {
		return true, nil
	}
	return f.dmlExprFilter.shouldSkipDML(dml, ti)
}

// ShouldIgnoreDDLJob checks if a DDL Event should be ignore by conditions below:
// 0. By startTs.
// 1. By schema name.
// 2. By table name.
// 3. By type.
// 4. By query.
func (f *filter) ShouldIgnoreDDLEvent(ddl *model.DDLEvent) (bool, error) {
	if f.shouldIgnoreStartTs(ddl.StartTs) {
		return true, nil
	}

	var shouldIgnoreTableOrSchema bool
	switch ddl.Type {
	case timodel.ActionCreateSchema, timodel.ActionDropSchema,
		timodel.ActionModifySchemaCharsetAndCollate:
		shouldIgnoreTableOrSchema = !f.tableFilter.MatchSchema(ddl.TableInfo.Schema)
	default:
		shouldIgnoreTableOrSchema = f.ShouldIgnoreTable(ddl.TableInfo.Schema, ddl.TableInfo.Table)
	}
	if shouldIgnoreTableOrSchema {
		return true, nil
	}
	return f.sqlEventFilter.shouldSkipDDL(ddl)
}

// ShouldDiscardDDL returns true if this DDL should be discarded.
// If a ddl is discarded, it will not be applied to cdc't schema storage
// and sent to downstream.
func (f *filter) ShouldDiscardDDL(ddlType timodel.ActionType) bool {
	if !f.shouldDiscardByBuiltInDDLAllowlist(ddlType) {
		return false
	}
	for _, allowDDLType := range f.ddlAllowlist {
		if allowDDLType == ddlType {
			return false
		}
	}
	return true
}

func (f *filter) shouldIgnoreStartTs(ts uint64) bool {
	for _, ignoreTs := range f.ignoreTxnStartTs {
		if ignoreTs == ts {
			return true
		}
	}
	return false
}

// ShouldIgnoreTable returns true if the specified table should be ignored by this change feed.
// NOTICE: Set `tbl` to an empty string to test against the whole database.
func (f *filter) ShouldIgnoreTable(db, tbl string) bool {
	if isSysSchema(db) {
		return true
	}
	return !f.tableFilter.MatchTable(db, tbl)
}

func (f *filter) shouldDiscardByBuiltInDDLAllowlist(ddlType timodel.ActionType) bool {
	/* The following DDL will be filter:
	ActionAddForeignKey                 ActionType = 9   1  这些都是当前 cdc 白名单直接过滤掉的 ddl
	ActionDropForeignKey                ActionType = 10  1
	ActionRebaseAutoID                  ActionType = 13  1
	ActionShardRowID                    ActionType = 16  1
	ActionLockTable                     ActionType = 27  1
	ActionUnlockTable                   ActionType = 28  1
	ActionRepairTable                   ActionType = 29  1
	ActionSetTiFlashReplica             ActionType = 30  1
	ActionUpdateTiFlashReplicaStatus    ActionType = 31  1
	ActionCreateSequence                ActionType = 34  1
	ActionAlterSequence                 ActionType = 35  1
	ActionDropSequence                  ActionType = 36  1
	ActionModifyTableAutoIdCache        ActionType = 39
	ActionRebaseAutoRandomBase          ActionType = 40
	ActionAlterIndexVisibility          ActionType = 41
	ActionExchangeTablePartition        ActionType = 42
	ActionAddCheckConstraint            ActionType = 43
	ActionDropCheckConstraint           ActionType = 44
	ActionAlterCheckConstraint          ActionType = 45
	ActionAlterTableAlterPartition      ActionType = 46

	... Any Action which of value is greater than 46 ...
	*/
	switch ddlType {
	case timodel.ActionCreateSchema,
		timodel.ActionDropSchema,
		timodel.ActionCreateTable,
		timodel.ActionDropTable,
		timodel.ActionAddColumn,
		timodel.ActionDropColumn,
		timodel.ActionAddIndex,
		timodel.ActionDropIndex,
		timodel.ActionTruncateTable,
		timodel.ActionModifyColumn,
		timodel.ActionRenameTable,
		timodel.ActionRenameTables, // 0
		timodel.ActionSetDefaultValue,
		timodel.ActionModifyTableComment,
		timodel.ActionRenameIndex,
		timodel.ActionAddTablePartition,
		timodel.ActionDropTablePartition,
		timodel.ActionCreateView,
		timodel.ActionModifyTableCharsetAndCollate,
		timodel.ActionTruncateTablePartition,
		timodel.ActionDropView,
		timodel.ActionRecoverTable,
		timodel.ActionModifySchemaCharsetAndCollate,
		timodel.ActionAddPrimaryKey,
		timodel.ActionDropPrimaryKey,
		timodel.ActionAddColumns,  // 0
		timodel.ActionDropColumns: // 0  这些都是 spec 未定义的 ddl
		return false
	}
	return true
}
