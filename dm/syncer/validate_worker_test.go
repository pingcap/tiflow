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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/util/filter"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

func TestValidatorWorkerValidateTableChanges(t *testing.T) {
	testFunc := func(t *testing.T, mode string) {
		t.Helper()
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
		cfg.ValidatorCfg.Mode = mode
		_, mock, err := conn.InitMockDBFull()
		mock.MatchExpectationsInOrder(false)
		require.NoError(t, err)
		defer func() {
			conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
		}()
		syncerObj := NewSyncer(cfg, nil, nil)
		validator := NewContinuousDataValidator(cfg, syncerObj, false)
		validator.persistHelper.schemaInitialized.Store(true)
		validator.Start(pb.Stage_Stopped)
		defer validator.cancel()
		validator.reachedSyncer.Store(true)

		worker := newValidateWorker(validator, 0)

		checkInitStatus := func() {
			require.Zero(t, worker.pendingRowCounts[rowInsert])
			require.Zero(t, worker.pendingRowCounts[rowUpdated])
			require.Zero(t, worker.pendingRowCounts[rowDeleted])
			require.Zero(t, validator.pendingRowCounts[rowInsert].Load())
			require.Zero(t, validator.pendingRowCounts[rowUpdated].Load())
			require.Zero(t, validator.pendingRowCounts[rowDeleted].Load())
			require.Zero(t, len(worker.pendingChangesMap))
			require.Zero(t, len(worker.errorRows))
			require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_NewErr].Load())
			require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_IgnoredErr].Load())
			require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_ResolvedErr].Load())
		}

		// just created
		checkInitStatus()

		// insert & update same table, both row are validated failed
		worker.updateRowChange(&rowChange{
			table: tableInfo1,
			Key:   "1",
			Data:  []interface{}{1, "a"},
			Tp:    rowInsert,
		})
		worker.updateRowChange(&rowChange{
			table: tableInfo1,
			Key:   "1",
			Data:  []interface{}{1, "b"},
			Tp:    rowUpdated,
		})
		worker.updateRowChange(&rowChange{
			table: tableInfo1,
			Key:   "2",
			Data:  []interface{}{2, "2b"},
			Tp:    rowInsert,
		})
		mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}).AddRow(2, "incorrect data"))
		worker.validateTableChange()
		require.Zero(t, validator.result.Errors)
		require.Len(t, worker.pendingChangesMap, 1)
		require.Contains(t, worker.pendingChangesMap, tbl1.String())
		require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "1")
		require.Equal(t, rowUpdated, worker.pendingChangesMap[tbl1.String()].rows["1"].Tp)
		require.Equal(t, 1, worker.pendingChangesMap[tbl1.String()].rows["1"].FailedCnt)
		require.Len(t, worker.errorRows, 0)
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_NewErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_IgnoredErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_ResolvedErr].Load())
		if mode == config.ValidationFull {
			require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 2)
			require.Equal(t, int64(1), worker.pendingRowCounts[rowInsert])
			require.Equal(t, int64(1), worker.pendingRowCounts[rowUpdated])
			require.Equal(t, int64(0), worker.pendingRowCounts[rowDeleted])
			require.Equal(t, worker.pendingRowCounts[rowInsert], validator.pendingRowCounts[rowInsert].Load())
			require.Equal(t, worker.pendingRowCounts[rowUpdated], validator.pendingRowCounts[rowUpdated].Load())
			require.Equal(t, worker.pendingRowCounts[rowDeleted], validator.pendingRowCounts[rowDeleted].Load())
			require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "2")
			require.Equal(t, rowInsert, worker.pendingChangesMap[tbl1.String()].rows["2"].Tp)
			require.Equal(t, 1, worker.pendingChangesMap[tbl1.String()].rows["2"].FailedCnt)
		} else {
			// fast mode
			require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 1)
			require.Equal(t, int64(0), worker.pendingRowCounts[rowInsert])
			require.Equal(t, int64(1), worker.pendingRowCounts[rowUpdated])
			require.Equal(t, int64(0), worker.pendingRowCounts[rowDeleted])
			require.Equal(t, worker.pendingRowCounts[rowInsert], validator.pendingRowCounts[rowInsert].Load())
			require.Equal(t, worker.pendingRowCounts[rowUpdated], validator.pendingRowCounts[rowUpdated].Load())
			require.Equal(t, worker.pendingRowCounts[rowDeleted], validator.pendingRowCounts[rowDeleted].Load())
		}

		// validate again, this time row with pk=2 validate success
		mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}).AddRow(2, "2b"))
		worker.validateTableChange()
		require.Zero(t, validator.result.Errors)
		require.Equal(t, int64(0), worker.pendingRowCounts[rowInsert])
		require.Equal(t, int64(1), worker.pendingRowCounts[rowUpdated])
		require.Equal(t, int64(0), worker.pendingRowCounts[rowDeleted])
		require.Equal(t, worker.pendingRowCounts[rowInsert], validator.pendingRowCounts[rowInsert].Load())
		require.Equal(t, worker.pendingRowCounts[rowUpdated], validator.pendingRowCounts[rowUpdated].Load())
		require.Equal(t, worker.pendingRowCounts[rowDeleted], validator.pendingRowCounts[rowDeleted].Load())
		require.Len(t, worker.pendingChangesMap, 1)
		require.Contains(t, worker.pendingChangesMap, tbl1.String())
		require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 1)
		require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "1")
		require.Equal(t, rowUpdated, worker.pendingChangesMap[tbl1.String()].rows["1"].Tp)
		require.Equal(t, 2, worker.pendingChangesMap[tbl1.String()].rows["1"].FailedCnt)
		require.Len(t, worker.errorRows, 0)
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_NewErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_IgnoredErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_ResolvedErr].Load())

		//
		// add 2 delete row of tbl2 and tbl3
		worker.updateRowChange(&rowChange{
			table: tableInfo2,
			Key:   "a",
			Data:  []interface{}{"a", "b"},
			Tp:    rowDeleted,
		})
		worker.updateRowChange(&rowChange{
			table: tableInfo3,
			Key:   "aa",
			Data:  []interface{}{"aa", "b"},
			Tp:    rowDeleted,
		})
		mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}))
		mock.ExpectQuery("SELECT .* FROM .*tbl2.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}))
		mock.ExpectQuery("SELECT .* FROM .*tbl3.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}).AddRow("aa", "b"))
		worker.validateTableChange()
		require.Zero(t, validator.result.Errors)
		require.Equal(t, int64(0), worker.pendingRowCounts[rowInsert])
		require.Equal(t, int64(1), worker.pendingRowCounts[rowUpdated])
		require.Equal(t, int64(1), worker.pendingRowCounts[rowDeleted])
		require.Equal(t, worker.pendingRowCounts[rowInsert], validator.pendingRowCounts[rowInsert].Load())
		require.Equal(t, worker.pendingRowCounts[rowUpdated], validator.pendingRowCounts[rowUpdated].Load())
		require.Equal(t, worker.pendingRowCounts[rowDeleted], validator.pendingRowCounts[rowDeleted].Load())
		require.Len(t, worker.pendingChangesMap, 2)
		require.Contains(t, worker.pendingChangesMap, tbl1.String())
		require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 1)
		require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "1")
		require.Equal(t, rowUpdated, worker.pendingChangesMap[tbl1.String()].rows["1"].Tp)
		require.Equal(t, 3, worker.pendingChangesMap[tbl1.String()].rows["1"].FailedCnt)
		require.Contains(t, worker.pendingChangesMap, tbl3.String())
		require.Len(t, worker.pendingChangesMap[tbl3.String()].rows, 1)
		require.Contains(t, worker.pendingChangesMap[tbl3.String()].rows, "aa")
		require.Equal(t, rowDeleted, worker.pendingChangesMap[tbl3.String()].rows["aa"].Tp)
		require.Equal(t, 1, worker.pendingChangesMap[tbl3.String()].rows["aa"].FailedCnt)
		require.Len(t, worker.errorRows, 0)
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_NewErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_IgnoredErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_ResolvedErr].Load())

		// for tbl1, pk=1 is synced, validate success
		// for tbl3, pk=aa is synced, validate success
		mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}).AddRow(1, "b"))
		mock.ExpectQuery("SELECT .* FROM .*tbl3.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}))
		worker.validateTableChange()
		require.Zero(t, validator.result.Errors)
		// everything is validated successfully, no error rows
		checkInitStatus()

		//
		// validate with batch size = 2
		worker.batchSize = 2
		worker.updateRowChange(&rowChange{
			table: tableInfo1,
			Key:   "1",
			Data:  []interface{}{1, "a"},
			Tp:    rowInsert,
		})
		worker.updateRowChange(&rowChange{
			table: tableInfo1,
			Key:   "2",
			Data:  []interface{}{2, "2b"},
			Tp:    rowInsert,
		})
		worker.updateRowChange(&rowChange{
			table: tableInfo1,
			Key:   "3",
			Data:  []interface{}{3, "3c"},
			Tp:    rowInsert,
		})
		mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}).AddRow(1, "a").AddRow(2, "2b"))
		mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}).AddRow(1, "a").AddRow(2, "2b"))
		worker.validateTableChange()
		require.Zero(t, validator.result.Errors)
		require.Equal(t, int64(1), worker.pendingRowCounts[rowInsert])
		require.Equal(t, int64(0), worker.pendingRowCounts[rowUpdated])
		require.Equal(t, int64(0), worker.pendingRowCounts[rowDeleted])
		require.Equal(t, worker.pendingRowCounts[rowInsert], validator.pendingRowCounts[rowInsert].Load())
		require.Equal(t, worker.pendingRowCounts[rowUpdated], validator.pendingRowCounts[rowUpdated].Load())
		require.Equal(t, worker.pendingRowCounts[rowDeleted], validator.pendingRowCounts[rowDeleted].Load())
		require.Len(t, worker.pendingChangesMap, 1)
		require.Contains(t, worker.pendingChangesMap, tbl1.String())
		require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 1)
		require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "3")
		require.Equal(t, rowInsert, worker.pendingChangesMap[tbl1.String()].rows["3"].Tp)
		require.Equal(t, 1, worker.pendingChangesMap[tbl1.String()].rows["3"].FailedCnt)
		require.Len(t, worker.errorRows, 0)
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_NewErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_IgnoredErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_ResolvedErr].Load())

		// sync row 3 but got wrong result
		mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}).AddRow(1, "a").AddRow(2, "2b").AddRow(3, "3dd"))
		worker.validateTableChange()
		require.Zero(t, validator.result.Errors)
		if mode == config.ValidationFull {
			// remain error
			require.Equal(t, int64(1), worker.pendingRowCounts[rowInsert])
			require.Equal(t, int64(0), worker.pendingRowCounts[rowUpdated])
			require.Equal(t, int64(0), worker.pendingRowCounts[rowDeleted])
			require.Equal(t, worker.pendingRowCounts[rowInsert], validator.pendingRowCounts[rowInsert].Load())
			require.Equal(t, worker.pendingRowCounts[rowUpdated], validator.pendingRowCounts[rowUpdated].Load())
			require.Equal(t, worker.pendingRowCounts[rowDeleted], validator.pendingRowCounts[rowDeleted].Load())
			require.Len(t, worker.pendingChangesMap, 1)
			require.Contains(t, worker.pendingChangesMap, tbl1.String())
			require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 1)
			require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "3")
			require.Equal(t, rowInsert, worker.pendingChangesMap[tbl1.String()].rows["3"].Tp)
			require.Equal(t, 2, worker.pendingChangesMap[tbl1.String()].rows["3"].FailedCnt) // fail again
			require.Len(t, worker.errorRows, 0)
			require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_NewErr].Load())
			require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_IgnoredErr].Load())
			require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_ResolvedErr].Load())
		} else {
			// everything is validated successfully, no error rows
			checkInitStatus()
		}

		// reset batch size
		worker.batchSize = 100
		if mode == config.ValidationFull {
			// sync row 3 success
			mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
				sqlmock.NewRows([]string{"a", "b"}).AddRow(3, "3c"))
			worker.validateTableChange()
			require.Zero(t, validator.result.Errors)
			// everything is validated successfully, no error rows
			checkInitStatus()
		}

		// set reachedSyncer = false, there should not be any errors and failedCount=0
		validator.reachedSyncer.Store(false)
		worker.updateRowChange(&rowChange{
			table: tableInfo1,
			Key:   "1",
			Data:  []interface{}{1, "a"},
			Tp:    rowInsert,
		})
		mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}))
		worker.validateTableChange()
		require.Zero(t, validator.result.Errors)
		require.Equal(t, int64(1), worker.pendingRowCounts[rowInsert])
		require.Equal(t, int64(0), worker.pendingRowCounts[rowUpdated])
		require.Equal(t, int64(0), worker.pendingRowCounts[rowDeleted])
		require.Equal(t, worker.pendingRowCounts[rowInsert], validator.pendingRowCounts[rowInsert].Load())
		require.Equal(t, worker.pendingRowCounts[rowUpdated], validator.pendingRowCounts[rowUpdated].Load())
		require.Equal(t, worker.pendingRowCounts[rowDeleted], validator.pendingRowCounts[rowDeleted].Load())
		require.Len(t, worker.pendingChangesMap, 1)
		require.Contains(t, worker.pendingChangesMap, tbl1.String())
		require.Len(t, worker.pendingChangesMap[tbl1.String()].rows, 1)
		require.Contains(t, worker.pendingChangesMap[tbl1.String()].rows, "1")
		require.Equal(t, rowInsert, worker.pendingChangesMap[tbl1.String()].rows["1"].Tp)
		require.Zero(t, worker.pendingChangesMap[tbl1.String()].rows["1"].FailedCnt)
		require.Len(t, worker.errorRows, 0)
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_NewErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_IgnoredErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_ResolvedErr].Load())

		mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}).AddRow(1, "a"))
		worker.validateTableChange()
		require.Zero(t, validator.result.Errors)
		// everything is validated successfully, no error rows
		checkInitStatus()

		// set reachedSyncer=true, rowErrorDelayInSec = 0, failed rows became error directly
		validator.reachedSyncer.Store(true)
		worker.rowErrorDelayInSec = 0
		worker.updateRowChange(&rowChange{
			table: tableInfo1,
			Key:   "1",
			Data:  []interface{}{1, "a"},
			Tp:    rowInsert,
		})
		mock.ExpectQuery("SELECT .* FROM .*tbl1.* WHERE .*").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b"}))
		worker.validateTableChange()
		require.Zero(t, validator.result.Errors)
		require.Zero(t, worker.pendingRowCounts[rowInsert])
		require.Zero(t, worker.pendingRowCounts[rowUpdated])
		require.Zero(t, worker.pendingRowCounts[rowDeleted])
		require.Zero(t, validator.pendingRowCounts[rowInsert].Load())
		require.Zero(t, validator.pendingRowCounts[rowUpdated].Load())
		require.Zero(t, validator.pendingRowCounts[rowDeleted].Load())
		require.Zero(t, len(worker.pendingChangesMap))
		require.Len(t, worker.errorRows, 1)
		require.Equal(t, int64(1), validator.errorRowCounts[pb.ValidateErrorState_NewErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_IgnoredErr].Load())
		require.Zero(t, validator.errorRowCounts[pb.ValidateErrorState_ResolvedErr].Load())
	}
	testFunc(t, config.ValidationFast)
	testFunc(t, config.ValidationFull)
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
			L:    log.L(),
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
		L:    log.L(),
	}

	// query error
	mock.ExpectQuery("SELECT .* FROM .*").WithArgs(sqlmock.AnyArg()).WillReturnError(errors.New("query"))
	_, err = worker.getTargetRows(cond)
	require.EqualError(t, errors.Cause(err), "query")
}

func TestValidatorWorkerGetSourceRowsForCompare(t *testing.T) {
	rows := getSourceRowsForCompare([]*rowChange{
		{
			Key: "a",
			Data: []interface{}{
				nil, 1,
			},
		},
		{
			Key: "b",
			Data: []interface{}{
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
