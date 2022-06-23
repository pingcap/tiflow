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
	"github.com/pingcap/tidb/util/filter"
	tfilter "github.com/pingcap/tidb/util/table-filter"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

func configToBinlogEventRule(cfg *config.FilterConfig) ([]*bf.BinlogEventRule, error) {
	return cfg.EventRules, nil
}

func configToExpressionRules(cfg *config.FilterConfig) ([]*dmconfig.ExpressionFilter, error) {
	return cfg.ExpressionRules, nil
}

// isSysSchema returns true if the given schema is a system schema
func isSysSchema(db string) bool {
	return filter.IsSystemSchema(db)
}

// VerifyRules checks the filter rules in the configuration
// and returns an invalid rule error if the verification fails, otherwise it will return the parsed filter.
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
	case timodel.ActionRenameTable:
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
	}

	switch t {
	case timodel.ActionAddColumn,
		timodel.ActionDropColumn,
		timodel.ActionAddIndex,
		timodel.ActionDropIndex,
		timodel.ActionAddForeignKey,
		timodel.ActionDropForeignKey,
		timodel.ActionModifyColumn,
		timodel.ActionRebaseAutoID,
		timodel.ActionRenameTable,
		timodel.ActionSetDefaultValue,
		timodel.ActionShardRowID,
		timodel.ActionModifyTableComment,
		timodel.ActionRenameIndex,
		timodel.ActionAddTablePartition,
		timodel.ActionDropTablePartition,
		timodel.ActionCreateView,
		timodel.ActionModifyTableCharsetAndCollate,
		timodel.ActionTruncateTablePartition,
		timodel.ActionLockTable,
		timodel.ActionUnlockTable,
		timodel.ActionSetTiFlashReplica,
		timodel.ActionAddPrimaryKey,
		timodel.ActionDropPrimaryKey,
		timodel.ActionAddColumns,
		timodel.ActionDropColumns,
		timodel.ActionModifyTableAutoIdCache,
		timodel.ActionRebaseAutoRandomBase,
		timodel.ActionAlterIndexVisibility,
		timodel.ActionExchangeTablePartition,
		timodel.ActionAddCheckConstraint,
		timodel.ActionDropCheckConstraint,
		timodel.ActionAlterCheckConstraint,
		timodel.ActionRenameTables,
		timodel.ActionDropIndexes,
		timodel.ActionAlterTableAttributes,
		timodel.ActionAlterTablePartitionAttributes,
		timodel.ActionAlterPlacementPolicy,
		timodel.ActionAlterTablePartitionPlacement,
		timodel.ActionAlterTablePlacement,
		timodel.ActionAlterCacheTable,
		timodel.ActionAlterTableStatsOptions,
		timodel.ActionAlterNoCacheTable,
		timodel.ActionMultiSchemaChange:
		return bf.AlertTable
	default:
		log.Info("binlog filter unsupported ddl type", zap.String("type", t.String()))
		return bf.NullEvent
	}
}
