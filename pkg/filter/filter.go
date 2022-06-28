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
	filterV1 "github.com/pingcap/tidb/util/filter"
	filterV2 "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// Filter is an event filter implementation.
type Filter struct {
	// tableFilter is used to filter row event by table name.
	tableFilter      filterV2.Filter
	ignoreTxnStartTs []uint64
	ddlAllowlist     []timodel.ActionType
}

// VerifyRules checks the filter rules in the configuration
// and returns an invalid rule error if the verification fails, otherwise it will return the parsed filter.
func VerifyRules(cfg *config.ReplicaConfig) (filterV2.Filter, error) {
	var f filterV2.Filter
	var err error
	if len(cfg.Filter.Rules) == 0 && cfg.Filter.MySQLReplicationRules != nil {
		f, err = filterV2.ParseMySQLReplicationRules(cfg.Filter.MySQLReplicationRules)
	} else {
		rules := cfg.Filter.Rules
		if len(rules) == 0 {
			rules = []string{"*.*"}
		}
		f, err = filterV2.Parse(rules)
	}
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
	}

	return f, nil
}

// NewFilter creates a filter.
func NewFilter(cfg *config.ReplicaConfig) (*Filter, error) {
	f, err := VerifyRules(cfg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
	}

	if !cfg.CaseSensitive {
		f = filterV2.CaseInsensitive(f)
	}
	return &Filter{
		tableFilter:      f,
		ignoreTxnStartTs: cfg.Filter.IgnoreTxnStartTs,
		ddlAllowlist:     cfg.Filter.DDLAllowlist,
	}, nil
}

func (f *Filter) shouldIgnoreStartTs(ts uint64) bool {
	for _, ignoreTs := range f.ignoreTxnStartTs {
		if ignoreTs == ts {
			return true
		}
	}
	return false
}

// ShouldIgnoreTable returns true if the specified table should be ignored by this change feed.
// NOTICE: Set `tbl` to an empty string to test against the whole database.
func (f *Filter) ShouldIgnoreTable(db, tbl string) bool {
	if isSysSchema(db) {
		return true
	}
	return !f.tableFilter.MatchTable(db, tbl)
}

// ShouldIgnoreDMLEvent removes DMLs that's not wanted by this changefeed.
// CDC only supports filtering by database/table now.
func (f *Filter) ShouldIgnoreDMLEvent(ts uint64, schema, table string) bool {
	return f.shouldIgnoreStartTs(ts) || f.ShouldIgnoreTable(schema, table)
}

// ShouldIgnoreDDLEvent removes DDLs that's not wanted by this changefeed.
// CDC only supports filtering by database/table now.
func (f *Filter) ShouldIgnoreDDLEvent(ddl *model.DDLEvent) bool {
	var shouldIgnoreTableOrSchema bool
	switch ddl.Type {
	case timodel.ActionCreateSchema, timodel.ActionDropSchema,
		timodel.ActionModifySchemaCharsetAndCollate:
		shouldIgnoreTableOrSchema = !f.tableFilter.MatchSchema(ddl.TableInfo.Schema)
	default:
		shouldIgnoreTableOrSchema = f.ShouldIgnoreTable(ddl.TableInfo.Schema, ddl.TableInfo.Table)
	}

	return f.shouldIgnoreStartTs(ddl.StartTs) || shouldIgnoreTableOrSchema
}

// ShouldDiscardDDL returns true if this DDL should be discarded.
func (f *Filter) ShouldDiscardDDL(ddlType timodel.ActionType) bool {
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

func (f *Filter) shouldDiscardByBuiltInDDLAllowlist(ddlType timodel.ActionType) bool {
	/* The following DDL will be filter:
	ActionAddForeignKey                 ActionType = 9
	ActionDropForeignKey                ActionType = 10
	ActionRebaseAutoID                  ActionType = 13
	ActionShardRowID                    ActionType = 16
	ActionLockTable                     ActionType = 27
	ActionUnlockTable                   ActionType = 28
	ActionRepairTable                   ActionType = 29
	ActionSetTiFlashReplica             ActionType = 30
	ActionUpdateTiFlashReplicaStatus    ActionType = 31
	ActionCreateSequence                ActionType = 34
	ActionAlterSequence                 ActionType = 35
	ActionDropSequence                  ActionType = 36
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
		timodel.ActionRenameTables,
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
		timodel.ActionAddColumns,
		timodel.ActionDropColumns:
		return false
	}
	return true
}

// isSysSchema returns true if the given schema is a system schema
func isSysSchema(db string) bool {
	return filterV1.IsSystemSchema(db)
}
