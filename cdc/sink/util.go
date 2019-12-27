// Copyright 2019 PingCAP, Inc.
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

package sink

import (
	rawerrors "errors"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
)

var (
	errTableNotExist = rawerrors.New("table not exist")
)

type tableInfo struct {
	columns    []string
	primaryKey *indexInfo
	// include primary key if have
	uniqueKeys []indexInfo
}

type indexInfo struct {
	name    string
	columns []string
}

func getTableInfoFromSchemaStorage(schemaStorage TableInfoGetter, schemaName, tableName string) (info *tableInfo, err error) {
	info = new(tableInfo)
	tableID, exist := schemaStorage.GetTableIDByName(schemaName, tableName)
	if !exist {
		return nil, errors.Annotatef(errTableNotExist, "schema: %s table: %s", schemaName, tableName)
	}
	tableInfoModel, exist := schemaStorage.TableByID(tableID)
	if !exist {
		return nil, errors.Annotatef(errTableNotExist, "tableID: %d", tableID)
	}
	var columns []string
	for _, col := range tableInfoModel.Columns {
		if col.GeneratedExprString != "" {
			continue
		}
		columns = append(columns, col.Name.O)
	}
	var uniques []indexInfo
	for _, idx := range tableInfoModel.Indices {
		if idx.Primary || idx.Unique {
			idxCols := make([]string, len(idx.Columns))
			for i, col := range idx.Columns {
				idxCols[i] = col.Name.O
			}
			uniques = append(uniques, indexInfo{
				name:    idx.Name.O,
				columns: idxCols,
			})
		}
	}
	if tableInfoModel.PKIsHandle {
		for _, col := range tableInfoModel.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				uniques = append(uniques, indexInfo{
					name:    "PRIMARY",
					columns: []string{col.Name.O},
				})
				break
			}
		}
	}
	info.columns = columns
	info.uniqueKeys = uniques
	// put primary key at first place
	// and set primaryKey
	for i := 0; i < len(info.uniqueKeys); i++ {
		if info.uniqueKeys[i].name == "PRIMARY" {
			info.uniqueKeys[i], info.uniqueKeys[0] = info.uniqueKeys[0], info.uniqueKeys[i]
			info.primaryKey = &info.uniqueKeys[0]
			break
		}
	}
	return
}
