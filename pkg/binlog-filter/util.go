// Copyright 2018 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// AstToDDLEvent returns filter.DDLEvent
func AstToDDLEvent(node ast.StmtNode) EventType {
	switch n := node.(type) {
	case *ast.CreateDatabaseStmt:
		return CreateDatabase
	case *ast.DropDatabaseStmt:
		return DropDatabase
	case *ast.CreateTableStmt:
		return CreateTable
	case *ast.DropTableStmt:
		if n.IsView {
			return DropView
		}
		return DropTable
	case *ast.TruncateTableStmt:
		return TruncateTable
	case *ast.RenameTableStmt:
		return RenameTable
	case *ast.CreateIndexStmt:
		return CreateIndex
	case *ast.DropIndexStmt:
		return DropIndex
	case *ast.AlterTableStmt:
		return AlterTable
	case *ast.CreateViewStmt:
		return CreateView
	case *ast.AlterDatabaseStmt:
		return AlterDatabase
	}

	return NullEvent
}

// toEventType converts event type string to EventType and check if it is valid.
func toEventType(es string) (EventType, error) {
	event := EventType(strings.ToLower(es))
	switch event {
	case AllEvent,
		AllDDL,
		AllDML,
		NullEvent,
		NoneEvent,
		NoneDDL,
		NoneDML,
		InsertEvent,
		UpdateEvent,
		DeleteEvent,
		CreateDatabase,
		DropDatabase,
		AlterDatabase,
		CreateTable,
		DropTable,
		TruncateTable,
		RenameTable,
		CreateIndex,
		DropIndex,
		CreateView,
		DropView,
		AlterTable,
		AddTablePartition,
		DropTablePartition,
		TruncateTablePartition,

		IncompatibleDDLChanges,
		ValueRangeDecrease,
		PrecisionDecrease,
		ModifyColumn,
		RenameColumn,
		RenameIndex,
		DropColumn,
		DropPrimaryKey,
		DropUniqueKey,
		ModifyDefaultValue,
		ModifyConstraint,
		ModifyColumnsOrder,
		ModifyCharset,
		ModifyCollation,
		RemoveAutoIncrement,
		ModifyStorageEngine,
		ReorganizePartition,
		RebuildPartition,
		CoalescePartition,
		SplitPartition,
		ExchangePartition,

		ModifySchemaCharsetAndCollate,
		ModifyTableCharsetAndCollate,
		ModifyTableComment,
		RecoverTable,
		AlterTablePartitioning,
		RemovePartitioning,
		AddColumn,
		SetDefaultValue,
		RebaseAutoID,
		AddPrimaryKey,
		AlterIndexVisibility,
		AlterTTLInfo,
		AlterTTLRemove,
		MultiSchemaChange:
		return event, nil
	case CreateSchema: // alias of CreateDatabase
		return CreateDatabase, nil
	case DropSchema: // alias of DropDatabase
		return DropDatabase, nil
	case AlterSchema:
		return AlterDatabase, nil
	default:
		return NullEvent, errors.NotValidf("event type %s", es)
	}
}
