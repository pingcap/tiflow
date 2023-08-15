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
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"bytes"

	pmodel "github.com/pingcap/tidb/parser/model"
)

// RowToRedo converts row changed event to redo log row
func RowToRedo(row *RowChangedEvent) *RedoRowChangedEvent {
	redoLog := &RedoRowChangedEvent{
		Row:        row,
		Columns:    make([]*RedoColumn, 0, len(row.Columns)),
		PreColumns: make([]*RedoColumn, 0, len(row.PreColumns)),
	}
	for _, column := range row.Columns {
		var redoColumn *RedoColumn
		if column != nil {
			// workaround msgp issue(Decode replaces empty slices with nil https://github.com/tinylib/msgp/issues/247)
			// if []byte("") send with RowChangedEvent after UnmarshalMsg,
			// the value will become nil, which is unexpected.
			switch v := column.Value.(type) {
			case []byte:
				if bytes.Equal(v, []byte("")) {
					column.Value = ""
				}
			}
			redoColumn = &RedoColumn{Column: column, Flag: uint64(column.Flag)}
		}
		redoLog.Columns = append(redoLog.Columns, redoColumn)
	}
	for _, column := range row.PreColumns {
		var redoColumn *RedoColumn
		if column != nil {
			switch v := column.Value.(type) {
			case []byte:
				if bytes.Equal(v, []byte("")) {
					column.Value = ""
				}
			}
			redoColumn = &RedoColumn{Column: column, Flag: uint64(column.Flag)}
		}
		redoLog.PreColumns = append(redoLog.PreColumns, redoColumn)
	}
	return redoLog
}

// LogToRow converts redo log row to row changed event
func LogToRow(redoLog *RedoRowChangedEvent) *RowChangedEvent {
	row := redoLog.Row
	row.Columns = make([]*Column, 0, len(redoLog.Columns))
	row.PreColumns = make([]*Column, 0, len(redoLog.PreColumns))
	for _, column := range redoLog.PreColumns {
		if column == nil {
			row.PreColumns = append(row.PreColumns, nil)
			continue
		}
		column.Column.Flag = ColumnFlagType(column.Flag)
		row.PreColumns = append(row.PreColumns, column.Column)
	}
	for _, column := range redoLog.Columns {
		if column == nil {
			row.Columns = append(row.Columns, nil)
			continue
		}
		column.Column.Flag = ColumnFlagType(column.Flag)
		row.Columns = append(row.Columns, column.Column)
	}
	return row
}

// DDLToRedo converts ddl event to redo log ddl
func DDLToRedo(ddl *DDLEvent) *RedoDDLEvent {
	redoDDL := &RedoDDLEvent{
		DDL:  ddl,
		Type: byte(ddl.Type),
	}
	if ddl.TableInfo != nil {
		redoDDL.TableName = ddl.TableInfo.TableName
	}
	return redoDDL
}

// LogToDDL converts redo log ddl to ddl event
func LogToDDL(redoDDL *RedoDDLEvent) *DDLEvent {
	redoDDL.DDL.Type = pmodel.ActionType(redoDDL.Type)
	redoDDL.DDL.TableInfo = &TableInfo{
		TableName: redoDDL.TableName,
	}
	return redoDDL.DDL
}
