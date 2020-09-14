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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"

	filterV1 "github.com/pingcap/tidb-tools/pkg/filter"
	filterV2 "github.com/pingcap/tidb-tools/pkg/table-filter"

	cerror "github.com/pingcap/ticdc/pkg/errors"
)

// Filter is a event filter implementation
type Filter struct {
	filter           filterV2.Filter
	ignoreTxnStartTs []uint64
	ddlAllowlist     []model.ActionType
	isCyclicEnabled  bool
}

// NewFilter creates a filter
func NewFilter(cfg *config.ReplicaConfig) (*Filter, error) {
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
	if !cfg.CaseSensitive {
		f = filterV2.CaseInsensitive(f)
	}

	return &Filter{
		filter:           f,
		ignoreTxnStartTs: cfg.Filter.IgnoreTxnStartTs,
		ddlAllowlist:     cfg.Filter.DDLAllowlist,
		isCyclicEnabled:  cfg.Cyclic.IsEnabled(),
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
// Set `tbl` to an empty string to test against the whole database.
func (f *Filter) ShouldIgnoreTable(db, tbl string) bool {
	if IsSysSchema(db) {
		return true
	}
	if f.isCyclicEnabled && mark.IsMarkTable(db, tbl) {
		// Always replicate mark tables.
		return false
	}
	return !f.filter.MatchTable(db, tbl)
}

// ShouldIgnoreDMLEvent removes DMLs that's not wanted by this change feed.
// CDC only supports filtering by database/table now.
func (f *Filter) ShouldIgnoreDMLEvent(ts uint64, schema, table string) bool {
	return f.shouldIgnoreStartTs(ts) || f.ShouldIgnoreTable(schema, table)
}

// ShouldIgnoreDDLEvent removes DDLs that's not wanted by this change feed.
// CDC only supports filtering by database/table now.
func (f *Filter) ShouldIgnoreDDLEvent(ts uint64, ddlType model.ActionType, schema, table string) bool {
	var shouldIgnoreTableOrSchema bool
	switch ddlType {
	case model.ActionCreateSchema, model.ActionDropSchema,
		model.ActionModifySchemaCharsetAndCollate:
		shouldIgnoreTableOrSchema = !f.filter.MatchSchema(schema)
	default:
		shouldIgnoreTableOrSchema = f.ShouldIgnoreTable(schema, table)
	}
	return f.shouldIgnoreStartTs(ts) || shouldIgnoreTableOrSchema
}

// ShouldDiscardDDL returns true if this DDL should be discarded
func (f *Filter) ShouldDiscardDDL(ddlType model.ActionType) bool {
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

func (f *Filter) shouldDiscardByBuiltInDDLAllowlist(ddlType model.ActionType) bool {
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
	*/
	switch ddlType {
	case model.ActionCreateSchema,
		model.ActionDropSchema,
		model.ActionCreateTable,
		model.ActionDropTable,
		model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionTruncateTable,
		model.ActionModifyColumn,
		model.ActionRenameTable,
		model.ActionSetDefaultValue,
		model.ActionModifyTableComment,
		model.ActionRenameIndex,
		model.ActionAddTablePartition,
		model.ActionDropTablePartition,
		model.ActionCreateView,
		model.ActionModifyTableCharsetAndCollate,
		model.ActionTruncateTablePartition,
		model.ActionDropView,
		model.ActionRecoverTable,
		model.ActionModifySchemaCharsetAndCollate,
		model.ActionAddPrimaryKey,
		model.ActionDropPrimaryKey,
		model.ActionAddColumns,
		model.ActionDropColumns:
		return false
	}
	return true
}

// IsSysSchema returns true if the given schema is a system schema
func IsSysSchema(db string) bool {
	return filterV1.IsSystemSchema(db)
}
