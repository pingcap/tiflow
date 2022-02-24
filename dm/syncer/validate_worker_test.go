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

package syncer

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func Test_ValidatorWorker_getTargetRows(t *testing.T) {
	type testCase struct {
		schemaName string
		tblName    string
		creatSQL   string
		pkValues   [][]string
		allCols    []string
		rowData    [][]interface{}
		querySQL   string
	}
	testCases := []testCase{
		{
			schemaName: "test1",
			tblName:    "tbl1",
			creatSQL: `create table if not exists test1.tbl1(
				a int,
				b int,
				c int,
				primary key(a, b)
			);`,
			pkValues: [][]string{
				{"1", "2"}, {"3", "4"}, {"5", "6"},
			},
			allCols: []string{"a", "b", "c"},
			rowData: [][]interface{}{
				{"1", "2", "3"}, {"3", "4", "5"}, {"5", "6", "7"},
			},
			querySQL: "SELECT .* FROM .*test1.*",
		},
		{
			schemaName: "test2",
			tblName:    "tbl2",
			creatSQL: `create table if not exists test2.tbl2(
				a varchar(10),
				other text,
				b varbinary(100),
				c int,
				primary key(a)
			);`,
			pkValues: [][]string{
				{"a"}, {"b"}, {"c"},
			},
			allCols: []string{"a", "other", "b", "c"},
			rowData: [][]interface{}{
				{"a", "aaa", "\xdd\xcc", "1"}, {"b", "bbb", nil, "2"}, {"c", nil, nil, "3"},
			},
			querySQL: "SELECT .* FROM .*test2.*",
		},
	}
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	for i, tc := range testCases {
		var args []driver.Value
		for _, arr := range testCases[i].pkValues {
			for _, val := range arr {
				args = append(args, val)
			}
		}
		dataRows := mock.NewRows(tc.allCols)
		for j := range testCases[i].rowData {
			var args []driver.Value
			for _, val := range tc.rowData[j] {
				args = append(args, val)
			}
			dataRows = dataRows.AddRow(args...)
		}
		mock.ExpectQuery(tc.querySQL).WithArgs(args...).WillReturnRows(dataRows)
		cond := &Cond{
			Table:     genValidateTableInfo(t, tc.schemaName, tc.tblName, tc.creatSQL),
			ColumnCnt: 2,
			PkValues:  tc.pkValues,
		}
		dbConn := genDBConn(t, db, genSubtaskConfig(t))

		worker := &validateWorker{
			ctx:  context.Background(),
			conn: dbConn,
		}
		targetRows, err := worker.getTargetRows(cond)
		require.NoError(t, err)
		require.Equal(t, 3, len(targetRows))
		for i, pkVs := range tc.pkValues {
			key := genRowKey(pkVs)
			require.Contains(t, targetRows, key)
			data := targetRows[key]
			require.Equal(t, len(tc.rowData[i]), len(data))
			for j, val := range tc.rowData[i] {
				if val == nil {
					require.True(t, data[j].IsNull)
					require.Nil(t, data[j].Data)
				} else {
					require.False(t, data[j].IsNull)
					require.Equal(t, val, string(data[j].Data))
				}
			}
		}
	}
}
