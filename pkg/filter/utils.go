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
	timodel "github.com/pingcap/tidb/parser/model"
	tifilter "github.com/pingcap/tidb/util/filter"
	tfilter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// isSysSchema returns true if the given schema is a system schema
func isSysSchema(db string) bool {
	return tifilter.IsSystemSchema(db)
}

// VerifyRules checks the filter rules in the configuration
// and returns an invalid rule error if the verification fails,
// otherwise it will return a table filter.
func VerifyRules(cfg *config.ReplicaConfig) (tfilter.Filter, error) {
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
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
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
	case timodel.ActionDropIndex,
		timodel.ActionDropIndexes:
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
	case timodel.ActionUpdateTiFlashReplicaStatus:
		return bf.UpdateTiFlashReplicaStatus
	case timodel.ActionAddPrimaryKey:
		return bf.AddPrimaryKey
	case timodel.ActionDropPrimaryKey:
		return bf.DropPrimaryKey
	}

	switch t {
	case timodel.ActionAddColumn,
		timodel.ActionDropColumn,
		timodel.ActionAddIndex,
		timodel.ActionDropIndex,
		timodel.ActionModifyColumn,
		timodel.ActionRenameTable,
		timodel.ActionSetDefaultValue,
		timodel.ActionModifyTableComment,
		timodel.ActionRenameIndex,
		timodel.ActionAddTablePartition,
		timodel.ActionDropTablePartition,
		timodel.ActionCreateView,
		timodel.ActionModifyTableCharsetAndCollate,
		timodel.ActionTruncateTablePartition,
		timodel.ActionAddPrimaryKey,
		timodel.ActionDropPrimaryKey,
		timodel.ActionAddColumns,
		timodel.ActionDropColumns,
		timodel.ActionRenameTables,
		timodel.ActionDropIndexes:
		return bf.AlertTable
	default:
		log.Info("binlog filter unsupported ddl type", zap.String("type", t.String()))
		return bf.NullEvent
	}
}

// fizz: for cli and api used.
// getSupportedDDLJobTypes returns the supported event types for sqlEventFilter
func getSupportEventType() []bf.EventType {
	return []bf.EventType{
		bf.AllDML,
		bf.AllDDL,

		// dml events
		bf.InsertEvent,
		bf.UpdateEvent,
		bf.DeleteEvent,

		// ddl events share by dm and ticdc
		bf.CreateSchema,
		bf.DropSchema,
		bf.CreateTable,
		bf.DropTable,
		bf.TruncateTable,
		bf.RenameTable,
		bf.AddIndex,
		bf.DropIndex,
		bf.CreateView,
		bf.DropView,

		// ddl events used by ticdc only
		bf.AddColumn,
		bf.DropColumn,
		bf.ModifyColumn,
		bf.ModifyTableComment,
		bf.RenameIndex,
		bf.AddTablePartition,
		bf.DropTablePartition,
		bf.TruncateTablePartition,
		bf.ModifyTableCharsetAndCollate,
		bf.ModifySchemaCharsetAndCollate,
		bf.RecoverTable,
		bf.UpdateTiFlashReplicaStatus,
		bf.AddPrimaryKey,
		bf.DropPrimaryKey,
		// bf.AlertTable,
	}
}
