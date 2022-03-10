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
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
)

// split into 3 cases, since it may be unstable when put together.
func TestValidatorWorkerRunInsertUpdate(t *testing.T) {
	tbl1 := filter.Table{Schema: "test", Name: "tbl1"}
	tbl2 := filter.Table{Schema: "test", Name: "tbl2"}
	tbl3 := filter.Table{Schema: "test", Name: "tbl3"}
	tableInfo1 := genValidateTableInfo(t, tbl1.Schema, tbl1.Name,
		"create table tbl1(a int primary key, b varchar(100))")
	tableInfo2 := genValidateTableInfo(t, tbl2.Schema, tbl2.Name,
		"create table tbl2(a varchar(100) primary key, b varchar(100))")
	tableInfo3 := genValidateTableInfo(t, tbl3.Schema, tbl3.Name,
		"create table tbl3(a varchar(100) primary key, b varchar(100))")

	cfg := genSubtaskConfig(t)
	_, mock, err := conn.InitMockDBFull()
	mock.MatchExpectationsInOrder(false)
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	syncerObj := NewSyncer(cfg, nil, nil)
	validator := NewContinuousDataValidator(cfg, syncerObj)
	validator.Start(pb.Stage_Stopped)
	defer validator.cancel()

	// insert & update same table, both row are validated failed
	worker := newValidateWorker(validator, 0)
	worker.updateRowChange(&rowChange{
		table:      tableInfo1,
		key:        "1",
		pkValues:   []string{"1"},
		data:       []interface{}{1, "a"},
		tp:         rowInsert,
		lastMeetTS: time.Now().Unix(),
	})
	worker.updateRowChange(&rowChange{
		table:      tableInfo1,
		key:        "1",
		pkValues:   []string{"1"},
		data:       []interface{}{1, "b"},
		tp:         rowUpdated,
		lastMeetTS: time.Now().Unix(),
	})
	worker.updateRowChange(&rowChange{
		table:      tableInfo1,
		key:        "2",
		pkValues:   []string{"2"},
		data:       []interface{}{2, "2b"},
		tp:         rowInsert,
		lastMeetTS: time.Now().Unix(),
	})
	mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow(2, "incorrect data"))
	require.NoError(t, worker.validateTableChange())
	require.Equal(t, int64(2), worker.pendingRowCount.Load())
	require.Len(t, worker.pendingChangesMap, 1)
	require.Contains(t, worker.pendingChangesMap, tbl1.String())
	require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 2)
	require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "1")
	require.Equal(t, rowUpdated, worker.pendingChangesMap[tbl1.String()].rows["1"].tp)
	require.Equal(t, 1, worker.pendingChangesMap[tbl1.String()].rows["1"].failedCnt)
	require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "2")
	require.Equal(t, rowInsert, worker.pendingChangesMap[tbl1.String()].rows["2"].tp)
	require.Equal(t, 1, worker.pendingChangesMap[tbl1.String()].rows["2"].failedCnt)

	// validate again, this time row with pk=2 validate success
	mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow(2, "2b"))
	require.NoError(t, worker.validateTableChange())
	require.Equal(t, int64(1), worker.pendingRowCount.Load())
	require.Len(t, worker.pendingChangesMap, 1)
	require.Contains(t, worker.pendingChangesMap, tbl1.String())
	require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 1)
	require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "1")
	require.Equal(t, rowUpdated, worker.pendingChangesMap[tbl1.String()].rows["1"].tp)
	require.Equal(t, 2, worker.pendingChangesMap[tbl1.String()].rows["1"].failedCnt)

	//
	// add 2 delete row of tbl2 and tbl3
	worker.updateRowChange(&rowChange{
		table:      tableInfo2,
		key:        "a",
		pkValues:   []string{"a"},
		data:       []interface{}{"a", "b"},
		tp:         rowDeleted,
		lastMeetTS: time.Now().Unix(),
	})
	worker.updateRowChange(&rowChange{
		table:      tableInfo3,
		key:        "aa",
		pkValues:   []string{"aa"},
		data:       []interface{}{"aa", "b"},
		tp:         rowDeleted,
		lastMeetTS: time.Now().Unix(),
	})
	mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}))
	mock.ExpectQuery("SELECT .* FROM .*tbl2.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}))
	mock.ExpectQuery("SELECT .* FROM .*tbl3.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow("aa", "b"))
	require.NoError(t, worker.validateTableChange())
	require.Equal(t, int64(2), worker.pendingRowCount.Load())
	require.Len(t, worker.pendingChangesMap, 2)
	require.Contains(t, worker.pendingChangesMap, tbl1.String())
	require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 1)
	require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "1")
	require.Equal(t, rowUpdated, worker.pendingChangesMap[tbl1.String()].rows["1"].tp)
	require.Equal(t, 3, worker.pendingChangesMap[tbl1.String()].rows["1"].failedCnt)
	require.Contains(t, worker.pendingChangesMap, tbl3.String())
	require.Len(t, worker.pendingChangesMap[tbl3.String()].rows, 1)
	require.Contains(t, worker.pendingChangesMap[tbl3.String()].rows, "aa")
	require.Equal(t, rowDeleted, worker.pendingChangesMap[tbl3.String()].rows["aa"].tp)
	require.Equal(t, 1, worker.pendingChangesMap[tbl3.String()].rows["aa"].failedCnt)

	// for tbl1, pk=1 is synced, validate success
	// for tbl3, pk=aa is synced, validate success
	mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow(1, "b"))
	mock.ExpectQuery("SELECT .* FROM .*tbl3.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}))
	require.NoError(t, worker.validateTableChange())
	require.Equal(t, int64(0), worker.pendingRowCount.Load())
	require.Len(t, worker.pendingChangesMap, 0)

	//
	// validate with batch size = 2
	worker.batchSize = 2
	worker.updateRowChange(&rowChange{
		table:      tableInfo1,
		key:        "1",
		pkValues:   []string{"1"},
		data:       []interface{}{1, "a"},
		tp:         rowInsert,
		lastMeetTS: time.Now().Unix(),
	})
	worker.updateRowChange(&rowChange{
		table:      tableInfo1,
		key:        "2",
		pkValues:   []string{"2"},
		data:       []interface{}{2, "2b"},
		tp:         rowInsert,
		lastMeetTS: time.Now().Unix(),
	})
	worker.updateRowChange(&rowChange{
		table:      tableInfo1,
		key:        "3",
		pkValues:   []string{"3"},
		data:       []interface{}{3, "3c"},
		tp:         rowInsert,
		lastMeetTS: time.Now().Unix(),
	})
	mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow(1, "a").AddRow(2, "2b"))
	mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow(1, "a").AddRow(2, "2b"))
	require.NoError(t, worker.validateTableChange())
	require.Equal(t, int64(1), worker.pendingRowCount.Load())
	require.Len(t, worker.pendingChangesMap, 1)
	require.Contains(t, worker.pendingChangesMap, tbl1.String())
	require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 1)
	require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "3")
	require.Equal(t, rowInsert, worker.pendingChangesMap[tbl1.String()].rows["3"].tp)
	require.Equal(t, 1, worker.pendingChangesMap[tbl1.String()].rows["3"].failedCnt)
}

func TestValidatorWorkerCompareData(t *testing.T) {
	worker := validateWorker{}
	eq, err := worker.compareData([]*sql.NullString{{String: "1", Valid: true}},
		[]*sql.NullString{{Valid: false}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeLong}}})
	require.NoError(t, err)
	require.False(t, eq)
	eq, err = worker.compareData([]*sql.NullString{{String: "1.1", Valid: true}},
		[]*sql.NullString{{String: "1.x", Valid: true}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeFloat}}})
	require.Error(t, err)
	require.False(t, eq)
	eq, err = worker.compareData([]*sql.NullString{{String: "1.1", Valid: true}},
		[]*sql.NullString{{String: "1.1000011", Valid: true}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeFloat}}})
	require.NoError(t, err)
	require.False(t, eq)
	eq, err = worker.compareData([]*sql.NullString{{String: "1.1", Valid: true}},
		[]*sql.NullString{{String: "1.1000001", Valid: true}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeFloat}}})
	require.NoError(t, err)
	require.True(t, eq)
	eq, err = worker.compareData([]*sql.NullString{{String: "1.1", Valid: true}},
		[]*sql.NullString{{String: "1.1000001", Valid: true}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeDouble}}})
	require.NoError(t, err)
	require.True(t, eq)
	eq, err = worker.compareData([]*sql.NullString{{String: "1", Valid: true}},
		[]*sql.NullString{{String: "1", Valid: true}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeLong}}})
	require.NoError(t, err)
	require.True(t, eq)
	eq, err = worker.compareData([]*sql.NullString{{String: "aaa", Valid: true}},
		[]*sql.NullString{{String: "aaa", Valid: true}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeVarchar}}})
	require.NoError(t, err)
	require.True(t, eq)
	eq, err = worker.compareData([]*sql.NullString{{String: "\x01\x02", Valid: true}},
		[]*sql.NullString{{String: "\x01\x02", Valid: true}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeVarString}}})
	require.NoError(t, err)
	require.True(t, eq)
}

func TestValidatorWorkerGetTargetRows(t *testing.T) {
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
			var rowData []driver.Value
			for _, val := range tc.rowData[j] {
				rowData = append(rowData, val)
			}
			dataRows = dataRows.AddRow(rowData...)
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
		targetRows, err2 := worker.getTargetRows(cond)
		require.NoError(t, err2)
		require.Equal(t, 3, len(targetRows))
		for i, pkVs := range tc.pkValues {
			key := genRowKey(pkVs)
			require.Contains(t, targetRows, key)
			data := targetRows[key]
			require.Equal(t, len(tc.rowData[i]), len(data))
			for j, val := range tc.rowData[i] {
				if val == nil {
					require.False(t, data[j].Valid)
					require.Empty(t, data[j].String)
				} else {
					require.True(t, data[j].Valid)
					require.Equal(t, val, data[j].String)
				}
			}
		}
	}

	cond := &Cond{
		Table:     genValidateTableInfo(t, "test", "tbl", "create table tbl(a int primary key)"),
		ColumnCnt: 1,
		PkValues:  [][]string{{"1"}},
	}
	worker := &validateWorker{
		ctx:  context.Background(),
		conn: genDBConn(t, db, genSubtaskConfig(t)),
	}

	// query error
	mock.ExpectQuery("SELECT .* FROM .*").WithArgs(sqlmock.AnyArg()).WillReturnError(errors.New("query"))
	_, err = worker.getTargetRows(cond)
	require.EqualError(t, errors.Cause(err), "query")
}

func TestValidatorWorkerGetSourceRowsForCompare(t *testing.T) {
	rows := getSourceRowsForCompare([]*rowChange{
		{
			key: "a",
			data: []interface{}{
				nil, 1,
			},
		},
		{
			key: "b",
			data: []interface{}{
				1, 2,
			},
		},
	})
	require.Len(t, rows, 2)
	require.Len(t, rows["a"], 2)
	require.Len(t, rows["b"], 2)
	require.False(t, rows["a"][0].Valid)
	require.Equal(t, "1", rows["a"][1].String)
	require.Equal(t, "1", rows["b"][0].String)
	require.Equal(t, "2", rows["b"][1].String)
}
