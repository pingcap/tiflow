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
)

// allowDDLList is a list of DDL types that can be applied to cdc's schema storage.
// It's a white list.
var allowDDLList = []timodel.ActionType{
	timodel.ActionCreateSchema,
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
	timodel.ActionAddColumns,  // Removed in TiDB v6.2.0, see https://github.com/pingcap/tidb/pull/35862.
	timodel.ActionDropColumns, // Removed in TiDB v6.2.0
	timodel.ActionRebaseAutoID,
	timodel.ActionAlterIndexVisibility,
	timodel.ActionMultiSchemaChange,
	timodel.ActionExchangeTablePartition,
	timodel.ActionAlterTTLInfo,
	timodel.ActionAlterTTLRemove,
}

// Filter are safe for concurrent use.
// TODO: find a better way to abstract this interface.
type Filter interface {
	// ShouldIgnoreDMLEvent returns true and nil if the DML event should be ignored.
	ShouldIgnoreDMLEvent(dml *model.RowChangedEvent, rawRow model.RowChangedDatums, tableInfo *model.TableInfo) (bool, error)
	// ShouldIgnoreDDLEvent returns true and nil if the DDL event should be ignored.
	// If a ddl is ignored, it will applied to cdc's schema storage,
	// but not sent to downstream.
	ShouldIgnoreDDLEvent(ddl *model.DDLEvent) (bool, error)
	// ShouldDiscardDDL returns true if this DDL should be discarded.
	// If a ddl is discarded, it will neither be applied to cdc's schema storage
	// nor sent to downstream.
	ShouldDiscardDDL(ddlType timodel.ActionType, schema, table string) bool
	// ShouldIgnoreTable returns true if the table should be ignored.
	ShouldIgnoreTable(schema, table string) bool
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

	dmlExprFilter, err := newExprFilter(tz, cfg.Filter)
	if err != nil {
		return nil, err
	}
	sqlEventFilter, err := newSQLEventFilter(cfg.Filter)
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

// ShouldIgnoreDDLEvent checks if a DDL Event should be ignore by conditions below:
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
		shouldIgnoreTableOrSchema = !f.tableFilter.MatchSchema(ddl.TableInfo.TableName.Schema)
	case timodel.ActionRenameTable:
		shouldIgnoreTableOrSchema = f.ShouldIgnoreTable(ddl.PreTableInfo.TableName.Schema, ddl.PreTableInfo.TableName.Table)
	default:
		shouldIgnoreTableOrSchema = f.ShouldIgnoreTable(ddl.TableInfo.TableName.Schema, ddl.TableInfo.TableName.Table)
	}
	if shouldIgnoreTableOrSchema {
		return true, nil
	}
	return f.sqlEventFilter.shouldSkipDDL(ddl)
}

// ShouldDiscardDDL returns true if this DDL should be discarded.
// If a ddl is discarded, it will not be applied to cdc's schema storage
// and sent to downstream.
func (f *filter) ShouldDiscardDDL(ddlType timodel.ActionType, schema, table string) (discard bool) {
	discard = true

	for _, actionType := range allowDDLList {
		if ddlType == actionType {
			discard = false
			break
		}
	}

	if discard {
		return
	}

	switch ddlType {
	case timodel.ActionCreateSchema, timodel.ActionDropSchema,
		timodel.ActionModifySchemaCharsetAndCollate:
		discard = !f.tableFilter.MatchSchema(schema)
	default:
		discard = f.ShouldIgnoreTable(schema, table)
	}
	return
}

// ShouldIgnoreTable returns true if the specified table should be ignored by this change feed.
// NOTICE: Set `tbl` to an empty string to test against the whole database.
func (f *filter) ShouldIgnoreTable(db, tbl string) bool {
	if isSysSchema(db) {
		return true
	}
	return !f.tableFilter.MatchTable(db, tbl)
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
