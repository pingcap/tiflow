// Copyright 2024 PingCAP, Inc.
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

package diff

import (
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"go.uber.org/zap"
)

func sliceToMap(slice []string) map[string]interface{} {
	sMap := make(map[string]interface{})
	for _, str := range slice {
		sMap[str] = struct{}{}
	}
	return sMap
}

func ignoreColumns(tableInfo *model.TableInfo, columns []string) *model.TableInfo {
	if len(columns) == 0 {
		return tableInfo
	}

	removeColMap := sliceToMap(columns)
	for i := 0; i < len(tableInfo.Indices); i++ {
		index := tableInfo.Indices[i]
		for j := 0; j < len(index.Columns); j++ {
			col := index.Columns[j]
			if _, ok := removeColMap[col.Name.O]; ok {
				tableInfo.Indices = append(tableInfo.Indices[:i], tableInfo.Indices[i+1:]...)
				i--
				break
			}
		}
	}

	for j := 0; j < len(tableInfo.Columns); j++ {
		col := tableInfo.Columns[j]
		if _, ok := removeColMap[col.Name.O]; ok {
			tableInfo.Columns = append(tableInfo.Columns[:j], tableInfo.Columns[j+1:]...)
			j--
		}
	}

	// calculate column offset
	colMap := make(map[string]int, len(tableInfo.Columns))
	for i, col := range tableInfo.Columns {
		col.Offset = i
		colMap[col.Name.O] = i
	}

	for _, index := range tableInfo.Indices {
		for _, col := range index.Columns {
			offset, ok := colMap[col.Name.O]
			if !ok {
				// this should never happened
				log.Fatal("column not exists", zap.String("column", col.Name.O))
			}
			col.Offset = offset
		}
	}

	return tableInfo
}

func getColumnsFromIndex(index *model.IndexInfo, tableInfo *model.TableInfo) []*model.ColumnInfo {
	indexColumns := make([]*model.ColumnInfo, 0, len(index.Columns))
	for _, indexColumn := range index.Columns {
		for _, column := range tableInfo.Columns {
			if column.Name.O == indexColumn.Name.O {
				indexColumns = append(indexColumns, column)
			}
		}
	}

	return indexColumns
}

func needQuotes(ft types.FieldType) bool {
	return !(dbutil.IsNumberType(ft.GetType()) || dbutil.IsFloatType(ft.GetType()))
}

func rowContainsCols(row map[string]*dbutil.ColumnData, cols []*model.ColumnInfo) bool {
	for _, col := range cols {
		if _, ok := row[col.Name.O]; !ok {
			return false
		}
	}

	return true
}

func rowToString(row map[string]*dbutil.ColumnData) string {
	var s strings.Builder
	s.WriteString("{ ")
	for key, val := range row {
		if val.IsNull {
			s.WriteString(fmt.Sprintf("%s: IsNull, ", key))
		} else {
			s.WriteString(fmt.Sprintf("%s: %s, ", key, val.Data))
		}
	}
	s.WriteString(" }")

	return s.String()
}

func stringsToInterfaces(strs []string) []interface{} {
	is := make([]interface{}, 0, len(strs))
	for _, str := range strs {
		is = append(is, str)
	}

	return is
}

func minLenInSlices(slices [][]string) int {
	min := 0
	for i, slice := range slices {
		if i == 0 || len(slice) < min {
			min = len(slice)
		}
	}

	return min
}

func equalTableInfo(tableInfo1, tableInfo2 *model.TableInfo) (bool, string) {
	// check columns
	if len(tableInfo1.Columns) != len(tableInfo2.Columns) {
		return false, fmt.Sprintf("column num not equal, one is %d another is %d",
			len(tableInfo1.Columns), len(tableInfo2.Columns))
	}

	for j, col := range tableInfo1.Columns {
		if col.Name.O != tableInfo2.Columns[j].Name.O {
			return false, fmt.Sprintf("column name not equal, one is %s another is %s",
				col.Name.O, tableInfo2.Columns[j].Name.O)
		}
		if col.GetType() != tableInfo2.Columns[j].GetType() {
			return false, fmt.Sprintf("column %s's type not equal, one is %v another is %v",
				col.Name.O, col.GetType(), tableInfo2.Columns[j].GetType())
		}
	}

	// check index
	if len(tableInfo1.Indices) != len(tableInfo2.Indices) {
		return false, fmt.Sprintf("index num not equal, one is %d another is %d",
			len(tableInfo1.Indices), len(tableInfo2.Indices))
	}

	index2Map := make(map[string]*model.IndexInfo)
	for _, index := range tableInfo2.Indices {
		index2Map[index.Name.O] = index
	}

	for _, index1 := range tableInfo1.Indices {
		index2, ok := index2Map[index1.Name.O]
		if !ok {
			return false, fmt.Sprintf("index %s not exists", index1.Name.O)
		}

		if len(index1.Columns) != len(index2.Columns) {
			return false, fmt.Sprintf("index %s's columns num not equal, one is %d another is %d",
				index1.Name.O, len(index1.Columns), len(index2.Columns))
		}
		for j, col := range index1.Columns {
			if col.Name.O != index2.Columns[j].Name.O {
				return false, fmt.Sprintf("index %s's column not equal, one has %s another has %s",
					index1.Name.O, col.Name.O, index2.Columns[j].Name.O)
			}
		}
	}

	return true, ""
}
