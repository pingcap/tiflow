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
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
)

// ddlWhiteListMap is a map of all DDL types that can be applied to cdc's schema storage.
// It's a white list.
var ddlWhiteListMap = map[timodel.ActionType]bf.EventType{
	// schema related DDLs
	timodel.ActionCreateSchema:                  bf.CreateDatabase,
	timodel.ActionDropSchema:                    bf.DropDatabase,
	timodel.ActionModifySchemaCharsetAndCollate: bf.ModifySchemaCharsetAndCollate,

	// table related DDLs
	timodel.ActionCreateTable:                  bf.CreateTable,
	timodel.ActionDropTable:                    bf.DropTable,
	timodel.ActionTruncateTable:                bf.TruncateTable,
	timodel.ActionRenameTable:                  bf.RenameTable,
	timodel.ActionRenameTables:                 bf.RenameTable,
	timodel.ActionRecoverTable:                 bf.RecoverTable,
	timodel.ActionModifyTableComment:           bf.ModifyTableComment,
	timodel.ActionModifyTableCharsetAndCollate: bf.ModifyTableCharsetAndCollate,

	// view related DDLs
	timodel.ActionCreateView: bf.CreateView,
	timodel.ActionDropView:   bf.DropView,

	// partition related DDLs
	timodel.ActionAddTablePartition:      bf.AddTablePartition,
	timodel.ActionDropTablePartition:     bf.DropTablePartition,
	timodel.ActionTruncateTablePartition: bf.TruncateTablePartition,
	timodel.ActionExchangeTablePartition: bf.ExchangeTablePartition,
	timodel.ActionReorganizePartition:    bf.ReorganizePartition,
	timodel.ActionAlterTablePartitioning: bf.AlterTablePartitioning,
	timodel.ActionRemovePartitioning:     bf.RemovePartitioning,

	// column related DDLs
	timodel.ActionAddColumn:       bf.AddColumn,
	timodel.ActionDropColumn:      bf.DropColumn,
	timodel.ActionModifyColumn:    bf.ModifyColumn,
	timodel.ActionSetDefaultValue: bf.SetDefaultValue,

	// index related DDLs
	timodel.ActionRebaseAutoID:         bf.RebaseAutoID,
	timodel.ActionAddPrimaryKey:        bf.AddPrimaryKey,
	timodel.ActionDropPrimaryKey:       bf.DropPrimaryKey,
	timodel.ActionAddIndex:             bf.CreateIndex,
	timodel.ActionDropIndex:            bf.DropIndex,
	timodel.ActionRenameIndex:          bf.RenameIndex,
	timodel.ActionAlterIndexVisibility: bf.AlterIndexVisibility,

	// TTL related DDLs
	timodel.ActionAlterTTLInfo:   bf.AlterTTLInfo,
	timodel.ActionAlterTTLRemove: bf.AlterTTLRemove,

	// difficult to classify DDLs
	timodel.ActionMultiSchemaChange: bf.MultiSchemaChange,

	// deprecated DDLs,see https://github.com/pingcap/tidb/pull/35862.
	// DDL types below are deprecated in TiDB v6.2.0, but we still keep them here
	// In case that some users will use TiCDC to replicate data from TiDB v6.1.x.
	timodel.ActionAddColumns:  bf.AddColumn,
	timodel.ActionDropColumns: bf.DropColumn,
}

// Filter are safe for concurrent use.
// TODO: find a better way to abstract this interface.
type Filter interface {
	// ShouldIgnoreDMLEvent returns true and nil if the DML event should be ignored.
	ShouldIgnoreDMLEvent(dml *model.RowChangedEvent, rawRow model.RowChangedDatums, tableInfo *model.TableInfo) (bool, error)
	// ShouldDiscardDDL returns true if this DDL should be discarded.
	// If a ddl is discarded, it will neither be applied to cdc's schema storage
	// nor sent to downstream.
	ShouldDiscardDDL(startTs uint64, ddlType timodel.ActionType, schema, table, query string) (bool, error)
	// ShouldIgnoreTable returns true if the table should be ignored.
	ShouldIgnoreTable(schema, table string) bool
	// ShouldIgnoreSchema returns true if the schema should be ignored.
	ShouldIgnoreSchema(schema string) bool
	// Verify should only be called by create changefeed OpenAPI.
	// Its purpose is to verify the expression filter config.
	Verify(tableInfos []*model.TableInfo) error
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
}

// NewFilter creates a filter.
func NewFilter(cfg *config.ReplicaConfig, tz string) (Filter, error) {
	f, err := VerifyTableRules(cfg.Filter)
	if err != nil {
		return nil, err
	}

	if !cfg.CaseSensitive {
		f = tfilter.CaseInsensitive(f)
	}

	dmlExprFilter, err := newExprFilter(tz, cfg.Filter, cfg.SQLMode)
	if err != nil {
		return nil, err
	}
	sqlEventFilter, err := newSQLEventFilter(cfg.Filter, cfg.SQLMode)
	if err != nil {
		return nil, err
	}
	return &filter{
		tableFilter:      f,
		dmlExprFilter:    dmlExprFilter,
		sqlEventFilter:   sqlEventFilter,
		ignoreTxnStartTs: cfg.Filter.IgnoreTxnStartTs,
	}, nil
}

// ShouldIgnoreDMLEvent checks if a DML event should be ignore by conditions below:
// 0. By startTs.
// 1. By table name.
// 2. By type.
// 3. By columns value.
func (f *filter) ShouldIgnoreDMLEvent(
	dml *model.RowChangedEvent,
	rawRow model.RowChangedDatums,
	ti *model.TableInfo,
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
	return f.dmlExprFilter.shouldSkipDML(dml, rawRow, ti)
}

// ShouldDiscardDDL returns true if this DDL should be discarded.
// If a ddl is discarded, it will not be applied to cdc's schema storage
// and sent to downstream.
func (f *filter) ShouldDiscardDDL(startTs uint64, ddlType timodel.ActionType, schema, table, query string) (discard bool, err error) {
	discard = !isAllowedDDL(ddlType)
	if discard {
		return
	}

	discard = f.shouldIgnoreStartTs(startTs)
	if discard {
		return
	}

	if IsSchemaDDL(ddlType) {
		discard = !f.tableFilter.MatchSchema(schema)
	} else {
		discard = f.ShouldIgnoreTable(schema, table)
	}

	if discard {
		return
	}

	return f.sqlEventFilter.shouldSkipDDL(ddlType, schema, table, query)
}

// ShouldIgnoreTable returns true if the specified table should be ignored by this changefeed.
// NOTICE: Set `tbl` to an empty string to test against the whole database.
func (f *filter) ShouldIgnoreTable(db, tbl string) bool {
	if isSysSchema(db) {
		return true
	}
	return !f.tableFilter.MatchTable(db, tbl)
}

// ShouldIgnoreSchema returns true if the specified schema should be ignored by this changefeed.
func (f *filter) ShouldIgnoreSchema(schema string) bool {
	return isSysSchema(schema) || !f.tableFilter.MatchSchema(schema)
}

func (f *filter) Verify(tableInfos []*model.TableInfo) error {
	return f.dmlExprFilter.verify(tableInfos)
}

func (f *filter) shouldIgnoreStartTs(ts uint64) bool {
	for _, ignoreTs := range f.ignoreTxnStartTs {
		if ignoreTs == ts {
			return true
		}
	}
	return false
}

func isAllowedDDL(actionType timodel.ActionType) bool {
	_, ok := ddlWhiteListMap[actionType]
	return ok
}

// IsSchemaDDL returns true if the action type is a schema DDL.
func IsSchemaDDL(actionType timodel.ActionType) bool {
	switch actionType {
	case timodel.ActionCreateSchema, timodel.ActionDropSchema,
		timodel.ActionModifySchemaCharsetAndCollate:
		return true
	default:
		return false
	}
}
