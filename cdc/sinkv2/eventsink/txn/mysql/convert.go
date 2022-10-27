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

package mysql

import (
	"fmt"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
)

func convertRowChange(row *model.RowChangedEvent) *sqlmodel.RowChange {
	preValues := make([]interface{}, 0, len(row.PreColumns))
	for _, col := range row.PreColumns {
		if col == nil {
			// will not use this value, just append a dummy value
			preValues = append(preValues, "omitted value")
			continue
		}
		preValues = append(preValues, col.Value)
	}
	postValues := make([]interface{}, 0, len(row.Columns))
	for _, col := range row.Columns {
		if col == nil {
			postValues = append(postValues, "omitted value")
			continue
		}
		postValues = append(postValues, col.Value)
	}

	tableInfo := recoverTableInfo(row.PreColumns, row.Columns, row.IndexColumns)

	return sqlmodel.NewRowChange(row.Table, nil, preValues, postValues, tableInfo, nil, nil)
}

func recoverTableInfo(
	preCols,
	postCols []*model.Column,
	indexOffsetMatrix [][]int,
) *timodel.TableInfo {
	nonEmptyColumns := preCols
	if len(nonEmptyColumns) == 0 {
		nonEmptyColumns = postCols
	}

	tableInfo := &timodel.TableInfo{}
	// in fact nowhere will use this field, so we set a debug message
	tableInfo.Name = timodel.NewCIStr("generated_by_recoverTableInfo")

	for i, column := range nonEmptyColumns {
		columnInfo := &timodel.ColumnInfo{}
		if column == nil {
			// will not use this column, just add a dummy columnInfo
			columnInfo.Name = timodel.NewCIStr("omitted column")
			columnInfo.Offset = i
			columnInfo.State = timodel.StatePublic
			columnInfo.Hidden = true
			tableInfo.Columns = append(tableInfo.Columns, columnInfo)
			continue
		}
		columnInfo.Name = timodel.NewCIStr(column.Name)
		columnInfo.SetType(column.Type)
		columnInfo.Offset = i
		columnInfo.State = timodel.StatePublic

		flag := column.Flag
		if flag.IsBinary() {
			columnInfo.SetCharset("binary")
		}
		if flag.IsGeneratedColumn() {
			// we do not use this field, so we set it to any non-empty string
			columnInfo.GeneratedExprString = "generated_by_recoverTableInfo"
		}
		// now we will mark all column of an index as PriKeyFlag, however a real
		// TableInfo from TiDB only mark the first column
		// This also applies for UniqueKeyFlag, MultipleKeyFlag
		if flag.IsPrimaryKey() || flag.IsHandleKey() {
			// simply revert the TableInfo to a PK
			columnInfo.AddFlag(mysql.PriKeyFlag)
			tableInfo.PKIsHandle = true
		}
		if flag.IsUniqueKey() {
			columnInfo.AddFlag(mysql.UniqueKeyFlag)
		}
		if !flag.IsNullable() || flag.IsHandleKey() {
			columnInfo.AddFlag(mysql.NotNullFlag)
		}
		if flag.IsMultipleKey() {
			columnInfo.AddFlag(mysql.MultipleKeyFlag)
		}
		if flag.IsUnsigned() {
			columnInfo.AddFlag(mysql.UnsignedFlag)
		}

		tableInfo.Columns = append(tableInfo.Columns, columnInfo)
	}

	for i, colOffsets := range indexOffsetMatrix {
		indexInfo := &timodel.IndexInfo{}
		indexInfo.State = timodel.StatePublic
		indexInfo.Name = timodel.NewCIStr(fmt.Sprintf("idx_%d", i))

		firstCol := tableInfo.Columns[colOffsets[0]]
		if mysql.HasPriKeyFlag(firstCol.GetFlag()) {
			indexInfo.Primary = true
		}
		if mysql.HasUniKeyFlag(firstCol.GetFlag()) {
			indexInfo.Unique = true
		}

		for _, colOffset := range colOffsets {
			offsetAfterSkipped := colOffset
			col := tableInfo.Columns[offsetAfterSkipped]

			indexCol := &timodel.IndexColumn{}
			indexCol.Name = col.Name
			indexCol.Offset = offsetAfterSkipped
			indexInfo.Columns = append(indexInfo.Columns, indexCol)
		}

		// TODO: revert the "all column set index related flag" to "only the
		// first column set index related flag"

		tableInfo.Indices = append(tableInfo.Indices, indexInfo)
	}

	return tableInfo
}
