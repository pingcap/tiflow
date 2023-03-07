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

package v1

import (
	"bytes"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
)

// PreMarshal should be called before marshal a RedoLog.
func PreMarshal(r *RedoLog) {
	if r.RedoRow != nil && r.RedoRow.Row != nil {
		adjustRowPreMarshal(r.RedoRow)
	}
	if r.RedoDDL != nil && r.RedoDDL.DDL != nil {
		adjustDDLPreMarshal(r.RedoDDL)
	}
}

func adjustRowPreMarshal(redoLog *RedoRowChangedEvent) {
	row := redoLog.Row
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
}

func adjustDDLPreMarshal(redoDDL *RedoDDLEvent) {
	redoDDL.Type = byte(redoDDL.DDL.Type)
}

// PostUnmarshal should be called after unmarshal a RedoLog.
func PostUnmarshal(r *RedoLog) {
	if r.RedoRow != nil && r.RedoRow.Row != nil {
		adjustRowPostUnmarshal(r.RedoRow)
	}
	if r.RedoDDL != nil && r.RedoDDL.DDL != nil {
		adjustDDLPostUnmarshal(r.RedoDDL)
	}
}

func adjustRowPostUnmarshal(redoLog *RedoRowChangedEvent) {
	row := redoLog.Row
	row.Columns = make([]*Column, 0, len(redoLog.Columns))
	row.PreColumns = make([]*Column, 0, len(redoLog.PreColumns))
	for _, column := range redoLog.PreColumns {
		if column == nil {
			row.PreColumns = append(row.PreColumns, nil)
			continue
		}
		column.Column.Flag = model.ColumnFlagType(column.Flag)
		row.PreColumns = append(row.PreColumns, column.Column)
	}
	for _, column := range redoLog.Columns {
		if column == nil {
			row.Columns = append(row.Columns, nil)
			continue
		}
		column.Column.Flag = model.ColumnFlagType(column.Flag)
		row.Columns = append(row.Columns, column.Column)
	}
}

func adjustDDLPostUnmarshal(redoDDL *RedoDDLEvent) {
	redoDDL.DDL.Type = timodel.ActionType(redoDDL.Type)
}
