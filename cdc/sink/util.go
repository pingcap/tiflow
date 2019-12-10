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
	gosql "database/sql"
	rawerrors "errors"
	"strings"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/schema"
)

var (
	errTableNotExist = rawerrors.New("table not exist")
)

const (
	colsSQL = `
SELECT column_name, extra FROM information_schema.columns
WHERE table_schema = ? AND table_name = ?;`
	uniqKeysSQL = `
SELECT non_unique, index_name, seq_in_index, column_name
FROM information_schema.statistics
WHERE table_schema = ? AND table_name = ?
ORDER BY seq_in_index ASC;`
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

// getTableInfo returns information like (non-generated) column names and
// unique keys about the specified table
func getTableInfo(db *gosql.DB, schema string, table string) (info *tableInfo, err error) {
	info = new(tableInfo)

	if info.columns, err = getColsOfTbl(db, schema, table); err != nil {
		return nil, errors.Trace(err)
	}

	if info.uniqueKeys, err = getUniqKeys(db, schema, table); err != nil {
		return nil, errors.Trace(err)
	}

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

func getTableInfoFromSchemaStorage(schemaStorage *schema.Storage, schemaName, tableName string) (info *tableInfo, err error) {
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

// getColsOfTbl returns a slice of the names of all columns,
// generated columns are excluded.
// https://dev.mysql.com/doc/mysql-infoschema-excerpt/5.7/en/columns-table.html
func getColsOfTbl(db *gosql.DB, schema, table string) ([]string, error) {
	rows, err := db.Query(colsSQL, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	cols := make([]string, 0, 1)
	for rows.Next() {
		var name, extra string
		err = rows.Scan(&name, &extra)
		if err != nil {
			return nil, errors.Trace(err)
		}
		isGenerated := strings.Contains(extra, "VIRTUAL GENERATED") || strings.Contains(extra, "STORED GENERATED")
		if isGenerated {
			continue
		}
		cols = append(cols, name)
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	// if no any columns returns, means the table not exist.
	if len(cols) == 0 {
		return nil, errors.Annotatef(errTableNotExist, "schema: %s table: %s", schema, table)
	}

	return cols, nil
}

// https://dev.mysql.com/doc/mysql-infoschema-excerpt/5.7/en/statistics-table.html
func getUniqKeys(db *gosql.DB, schema, table string) (uniqueKeys []indexInfo, err error) {
	rows, err := db.Query(uniqKeysSQL, schema, table)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	defer rows.Close()

	var nonUnique int
	var keyName string
	var columnName string
	var seqInIndex int // start at 1

	// get pk and uk
	// key for PRIMARY or other index name
	for rows.Next() {
		err = rows.Scan(&nonUnique, &keyName, &seqInIndex, &columnName)
		if err != nil {
			err = errors.Trace(err)
			return
		}

		if nonUnique == 1 {
			continue
		}

		var i int
		// Search for indexInfo with the current keyName
		for i = 0; i < len(uniqueKeys); i++ {
			if uniqueKeys[i].name == keyName {
				uniqueKeys[i].columns = append(uniqueKeys[i].columns, columnName)
				break
			}
		}
		// If we don't find the indexInfo with the loop above, create a new one
		if i == len(uniqueKeys) {
			uniqueKeys = append(uniqueKeys, indexInfo{keyName, []string{columnName}})
		}
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	return
}

func isTableChanged(ddl *model.DDL) bool {
	switch ddl.Job.Type {
	case timodel.ActionDropTable, timodel.ActionDropSchema, timodel.ActionTruncateTable, timodel.ActionCreateSchema:
		return false
	default:
		return true
	}
}
