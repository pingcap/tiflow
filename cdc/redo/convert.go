// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing pemissions and
// limitations under the License.

package redo

import (
	pmodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
)

// RowToRedo converts row changed event to redo log row
func RowToRedo(row *model.RowChangedEvent) *model.RedoRowChangedEvent {
	redoLog := &model.RedoRowChangedEvent{
		Row:        row,
		Columns:    make([]*model.RedoColumn, 0, len(row.Columns)),
		PreColumns: make([]*model.RedoColumn, 0, len(row.PreColumns)),
	}
	for _, column := range row.Columns {
		redoLog.Columns = append(
			redoLog.Columns,
			&model.RedoColumn{Column: column, Flag: uint64(column.Flag)},
		)
	}
	for _, column := range row.PreColumns {
		redoLog.PreColumns = append(
			redoLog.PreColumns,
			&model.RedoColumn{Column: column, Flag: uint64(column.Flag)},
		)
	}
	return redoLog
}

// LogToRow converts redo log row to row changed event
func LogToRow(redoLog *model.RedoRowChangedEvent) *model.RowChangedEvent {
	row := redoLog.Row
	row.Columns = make([]*model.Column, 0, len(redoLog.Columns))
	row.PreColumns = make([]*model.Column, 0, len(redoLog.PreColumns))
	for _, column := range redoLog.PreColumns {
		column.Column.Flag = model.ColumnFlagType(column.Flag)
		row.PreColumns = append(row.PreColumns, column.Column)
	}
	for _, column := range redoLog.Columns {
		column.Column.Flag = model.ColumnFlagType(column.Flag)
		row.Columns = append(row.Columns, column.Column)
	}
	return row
}

// DDLToRedo converts ddl event to redo log ddl
func DDLToRedo(ddl *model.DDLEvent) *model.RedoDDLEvent {
	redoDDL := &model.RedoDDLEvent{
		DDL:  ddl,
		Type: byte(ddl.Type),
	}
	return redoDDL
}

// LogToDDL converts redo log ddl to ddl event
func LogToDDL(redoDDL *model.RedoDDLEvent) *model.DDLEvent {
	redoDDL.DDL.Type = pmodel.ActionType(redoDDL.Type)
	return redoDDL.DDL
}
