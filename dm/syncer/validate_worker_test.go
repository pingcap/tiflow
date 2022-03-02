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
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// split into 3 cases, since it may be unstable when put together.
func TestValidatorWorkerRun_insert_update(t *testing.T) {
	tbl1 := filter.Table{Schema: "test", Name: "tbl1"}
	tableInfo1 := genValidateTableInfo(t, tbl1.Schema, tbl1.Name,
		"create table tbl1(a int primary key, b varchar(100))")

	cfg := genSubtaskConfig(t)
	_, mock, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	syncerObj := NewSyncer(cfg, nil, nil)
	validator := NewContinuousDataValidator(cfg, syncerObj)
	validator.Start(pb.Stage_Stopped)
	defer validator.cancel()

	// insert & update same table, one success, one fail
	worker := newValidateWorker(validator, 0)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		worker.run()
	}()
	worker.rowChangeCh <- &rowChange{
		table:      tableInfo1,
		key:        "1",
		pkValues:   []string{"1"},
		data:       []interface{}{1, "a"},
		tp:         rowInsert,
		lastMeetTS: time.Now().Unix(),
	}
	worker.rowChangeCh <- &rowChange{
		table:      tableInfo1,
		key:        "1",
		pkValues:   []string{"1"},
		data:       []interface{}{1, "b"},
		tp:         rowUpdated,
		lastMeetTS: time.Now().Unix(),
	}
	worker.rowChangeCh <- &rowChange{
		table:      tableInfo1,
		key:        "2",
		pkValues:   []string{"2"},
		data:       []interface{}{2, "2b"},
		tp:         rowInsert,
		lastMeetTS: time.Now().Unix(),
	}
	mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow(2, "incorrect data"))
	require.True(t, utils.WaitSomething(30, time.Second, func() bool {
		return worker.receivedRowCount.Load() == 3
	}))
	require.True(t, utils.WaitSomething(30, time.Second, func() bool {
		return worker.validationCount.Load() > 0
	}))
	validator.cancel()
	wg.Wait()
	require.Equal(t, int64(3), worker.receivedRowCount.Load())
	require.Equal(t, int64(2), worker.pendingRowCount.Load())
	require.Len(t, worker.pendingChangesMap, 1)
	require.Contains(t, worker.pendingChangesMap, tbl1.String())
	require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 2)
	require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "1")
	require.Equal(t, rowUpdated, worker.pendingChangesMap[tbl1.String()].rows["1"].tp)
	require.Equal(t, int(worker.validationCount.Load()), worker.pendingChangesMap[tbl1.String()].rows["1"].failedCnt)
	require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "2")
	require.Equal(t, rowInsert, worker.pendingChangesMap[tbl1.String()].rows["2"].tp)
	require.Equal(t, int(worker.validationCount.Load()), worker.pendingChangesMap[tbl1.String()].rows["2"].failedCnt)
}

func TestValidatorWorkerRun_all_validated(t *testing.T) {
	tbl1 := filter.Table{Schema: "test", Name: "tbl1"}
	tableInfo1 := genValidateTableInfo(t, tbl1.Schema, tbl1.Name,
		"create table tbl1(a int primary key, b varchar(100))")

	cfg := genSubtaskConfig(t)
	_, mock, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	syncerObj := NewSyncer(cfg, nil, nil)
	validator := NewContinuousDataValidator(cfg, syncerObj)
	validator.Start(pb.Stage_Stopped)
	defer validator.cancel()

	// insert & update same table, one success, one fail
	worker := newValidateWorker(validator, 0)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		worker.run()
	}()
	worker.rowChangeCh <- &rowChange{
		table:      tableInfo1,
		key:        "1",
		pkValues:   []string{"1"},
		data:       []interface{}{1, "a"},
		tp:         rowInsert,
		lastMeetTS: time.Now().Unix(),
	}
	worker.rowChangeCh <- &rowChange{
		table:      tableInfo1,
		key:        "1",
		pkValues:   []string{"1"},
		data:       []interface{}{1, "b"},
		tp:         rowUpdated,
		lastMeetTS: time.Now().Unix(),
	}
	worker.rowChangeCh <- &rowChange{
		table:      tableInfo1,
		key:        "2",
		pkValues:   []string{"2"},
		data:       []interface{}{2, "2b"},
		tp:         rowInsert,
		lastMeetTS: time.Now().Unix(),
	}
	mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow(1, "b").AddRow(2, "2b"))
	require.True(t, utils.WaitSomething(30, time.Second, func() bool {
		return worker.receivedRowCount.Load() == 3
	}))
	require.True(t, utils.WaitSomething(30, time.Second, func() bool {
		return worker.validationCount.Load() > 0
	}))
	validator.cancel()
	wg.Wait()
	require.Equal(t, int64(3), worker.receivedRowCount.Load())
	require.Equal(t, int64(0), worker.pendingRowCount.Load())
	require.Len(t, worker.pendingChangesMap, 0)
}

func TestValidatorWorkerRun_delete(t *testing.T) {
	tbl2 := filter.Table{Schema: "test", Name: "tbl2"}
	tbl3 := filter.Table{Schema: "test", Name: "tbl3"}
	tableInfo2 := genValidateTableInfo(t, tbl2.Schema, tbl2.Name,
		"create table tbl2(a varchar(100) primary key, b varchar(100))")
	tableInfo3 := genValidateTableInfo(t, tbl3.Schema, tbl3.Name,
		"create table tbl3(a varchar(100) primary key, b varchar(100))")

	cfg := genSubtaskConfig(t)
	_, mock, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	syncerObj := NewSyncer(cfg, nil, nil)
	validator := NewContinuousDataValidator(cfg, syncerObj)
	validator.Start(pb.Stage_Stopped)
	defer validator.cancel()

	worker := newValidateWorker(validator, 0)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		worker.run()
	}()
	worker.rowChangeCh <- &rowChange{
		table:      tableInfo2,
		key:        "a",
		pkValues:   []string{"a"},
		data:       []interface{}{"a", "b"},
		tp:         rowDeleted,
		lastMeetTS: time.Now().Unix(),
	}
	worker.rowChangeCh <- &rowChange{
		table:      tableInfo3,
		key:        "aa",
		pkValues:   []string{"aa"},
		data:       []interface{}{"aa", "b"},
		tp:         rowDeleted,
		lastMeetTS: time.Now().Unix(),
	}
	mock.ExpectQuery("SELECT .* FROM .*tbl2.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}))
	mock.ExpectQuery("SELECT .* FROM .*tbl3.* WHERE .*").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow("aa", "b"))

	// wait all events received by worker
	require.True(t, utils.WaitSomething(30, time.Second, func() bool {
		return worker.receivedRowCount.Load() == 2
	}))
	currCnt := worker.validationCount.Load()
	require.True(t, utils.WaitSomething(30, time.Second, func() bool {
		return worker.validationCount.Load() > currCnt
	}))
	validator.cancel()
	wg.Wait()

	require.Equal(t, int64(1), worker.pendingRowCount.Load())
	require.Len(t, worker.pendingChangesMap, 1)
	require.Contains(t, worker.pendingChangesMap, tbl3.String())
	require.Len(t, worker.pendingChangesMap[tbl3.String()].rows, 1)
	require.Contains(t, worker.pendingChangesMap[tbl3.String()].rows, "aa")
	require.Equal(t, rowDeleted, worker.pendingChangesMap[tbl3.String()].rows["aa"].tp)
}

func TestValidatorWorkerCompareData(t *testing.T) {
	worker := validateWorker{}
	eq, err := worker.compareData([]*dbutil.ColumnData{{Data: []byte("1")}},
		[]*dbutil.ColumnData{{IsNull: true}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeLong}}})
	require.NoError(t, err)
	require.False(t, eq)
	eq, err = worker.compareData([]*dbutil.ColumnData{{Data: []byte("1.1")}},
		[]*dbutil.ColumnData{{Data: []byte("1.x")}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeFloat}}})
	require.Error(t, err)
	require.False(t, eq)
	eq, err = worker.compareData([]*dbutil.ColumnData{{Data: []byte("1.1")}},
		[]*dbutil.ColumnData{{Data: []byte("1.1000011")}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeFloat}}})
	require.NoError(t, err)
	require.False(t, eq)
	eq, err = worker.compareData([]*dbutil.ColumnData{{Data: []byte("1.1")}},
		[]*dbutil.ColumnData{{Data: []byte("1.1000001")}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeFloat}}})
	require.NoError(t, err)
	require.True(t, eq)
	eq, err = worker.compareData([]*dbutil.ColumnData{{Data: []byte("1.1")}},
		[]*dbutil.ColumnData{{Data: []byte("1.1000001")}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeDouble}}})
	require.NoError(t, err)
	require.True(t, eq)
	eq, err = worker.compareData([]*dbutil.ColumnData{{Data: []byte("1")}},
		[]*dbutil.ColumnData{{Data: []byte("1")}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeLong}}})
	require.NoError(t, err)
	require.True(t, eq)
	eq, err = worker.compareData([]*dbutil.ColumnData{{Data: []byte("aaa")}},
		[]*dbutil.ColumnData{{Data: []byte("aaa")}},
		[]*model.ColumnInfo{{FieldType: types.FieldType{Tp: mysql.TypeVarchar}}})
	require.NoError(t, err)
	require.True(t, eq)
	eq, err = worker.compareData([]*dbutil.ColumnData{{Data: []byte("\x01\x02")}},
		[]*dbutil.ColumnData{{Data: []byte("\x01\x02")}},
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
					require.True(t, data[j].IsNull)
					require.Nil(t, data[j].Data)
				} else {
					require.False(t, data[j].IsNull)
					require.Equal(t, val, string(data[j].Data))
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
	require.True(t, rows["a"][0].IsNull)
	require.Equal(t, []byte("1"), rows["a"][1].Data)
	require.Equal(t, []byte("1"), rows["b"][0].Data)
	require.Equal(t, []byte("2"), rows["b"][1].Data)
}
