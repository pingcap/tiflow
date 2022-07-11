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
	"strings"

	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	timodel "github.com/pingcap/tidb/parser/model"
	tifilter "github.com/pingcap/tidb/util/filter"
	tfilter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// isSysSchema returns true if the given schema is a system schema
func isSysSchema(db string) bool {
	return tifilter.IsSystemSchema(db)
}

// VerifyTableRules checks the table filter rules in the configuration
// and returns an invalid rule error if the verification fails,
// otherwise it will return a table filter.
func VerifyTableRules(cfg *config.ReplicaConfig) (tfilter.Filter, error) {
	var f tfilter.Filter
	var err error
	if len(cfg.Filter.Rules) == 0 && cfg.Filter.MySQLReplicationRules != nil {
		f, err = tfilter.ParseMySQLReplicationRules(cfg.Filter.MySQLReplicationRules)
	} else {
		rules := cfg.Filter.Rules
		if len(rules) == 0 {
			rules = []string{"*.*"}
		}
		f, err = tfilter.Parse(rules)
	}
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, cfg.Filter)
	}

	return f, nil
}

// jobTypeToEventType converts ddl job action type to binlog filter event type
func jobTypeToEventType(t timodel.ActionType) bf.EventType {
	switch t {
	case timodel.ActionCreateSchema:
		return bf.CreateDatabase
	case timodel.ActionDropSchema:
		return bf.DropDatabase
	case timodel.ActionCreateTable:
		return bf.CreateTable
	case timodel.ActionDropTable:
		return bf.DropTable
	case timodel.ActionTruncateTable:
		return bf.TruncateTable
	case timodel.ActionRenameTable, timodel.ActionRenameTables:
		return bf.RenameTable
	case timodel.ActionAddIndex:
		return bf.CreateIndex
	case timodel.ActionDropIndex:
		return bf.DropIndex
	case timodel.ActionCreateView:
		return bf.CreateView
	case timodel.ActionDropView:
		return bf.DropView
	case timodel.ActionAddColumn, timodel.ActionAddColumns:
		return bf.AddColumn
	case timodel.ActionDropColumn, timodel.ActionDropColumns:
		return bf.DropColumn
	case timodel.ActionModifyColumn:
		return bf.ModifyColumn
	case timodel.ActionSetDefaultValue:
		return bf.SetDefaultValue
	case timodel.ActionModifyTableComment:
		return bf.ModifyTableComment
	case timodel.ActionRenameIndex:
		return bf.RenameIndex
	case timodel.ActionAddTablePartition:
		return bf.AddTablePartition
	case timodel.ActionDropTablePartition:
		return bf.DropTablePartition
	case timodel.ActionTruncateTablePartition:
		return bf.TruncateTablePartition
	case timodel.ActionModifyTableCharsetAndCollate:
		return bf.ModifyTableCharsetAndCollate
	case timodel.ActionModifySchemaCharsetAndCollate:
		return bf.ModifySchemaCharsetAndCollate
	case timodel.ActionRecoverTable:
		return bf.RecoverTable
	case timodel.ActionAddPrimaryKey:
		return bf.AddPrimaryKey
	case timodel.ActionDropPrimaryKey:
		return bf.DropPrimaryKey
	default:
		return bf.NullEvent
	}

	// Alter table:
	// 	timodel.ActionDropColumn,
	// 	timodel.ActionAddIndex,
	// 	timodel.ActionDropIndex,
	// 	timodel.ActionModifyColumn,
	// 	timodel.ActionRenameTable,
	// 	timodel.ActionSetDefaultValue,
	// 	timodel.ActionModifyTableComment,
	// 	timodel.ActionRenameIndex,
	// 	timodel.ActionAddTablePartition,
	// 	timodel.ActionDropTablePartition,
	// 	timodel.ActionCreateView,
	// 	timodel.ActionDropView,
	// 	timodel.ActionModifyTableCharsetAndCollate,
	// 	timodel.ActionTruncateTablePartition,
	// 	timodel.ActionAddPrimaryKey,
	// 	timodel.ActionDropPrimaryKey,
	// 	timodel.ActionAddColumns,
	// 	timodel.ActionDropColumns,
	// 	timodel.ActionRenameTables,
	// 	timodel.ActionDropIndexes:
	// 	return bf.AlertTable
}

// SupportedEventWarnMessage returns the supported event types warning message
// for API or Cli use.
func SupportedEventWarnMessage() string {
	eventTypesStr := make([]string, 0, len(supportedEventTypes))
	for _, eventType := range supportedEventTypes {
		eventTypesStr = append(eventTypesStr, string(eventType))
	}
	return fmt.Sprintf("Invalid input, 'ignore-event' parameters can only accept [%s]",
		strings.Join(eventTypesStr, ", "))
}

func shouldDiscardByBuiltInDDLAllowlist(ddlType timodel.ActionType) bool {
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
