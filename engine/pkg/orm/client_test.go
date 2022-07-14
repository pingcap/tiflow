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

package orm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/dbutil"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/engine/pkg/orm/model"
	derror "github.com/pingcap/tiflow/pkg/errors"
)

type tCase struct {
	fn     string        // function name
	inputs []interface{} // function args

	output interface{} // function output
	err    error       // function error

	mockExpectResFn func(mock sqlmock.Sqlmock) // sqlmock expectation
}

func mockGetDBConn(t *testing.T, dsnStr string) (*sql.DB, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	// common execution for orm
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows(
		[]string{"VERSION()"}).AddRow("5.7.35-log"))
	return db, mock, nil
}

type anyTime struct{}

func (a anyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}

func TestNewMetaOpsClient(t *testing.T) {
	t.Parallel()

	var store metaModel.StoreConfig
	store.SetEndpoints("127.0.0.1:3306")
	cli, err := NewClient(store, *dbutil.DefaultDBConfig())
	require.Nil(t, cli)
	require.Error(t, err)

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err = newClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)
}

// nolint: deadcode
func testInitialize(t *testing.T) {
	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	testCases := []tCase{
		{
			fn:     "Initialize",
			inputs: []interface{}{},
			// TODO: Why index sequence is not stable ??
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
					sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `project_infos` (`seq_id` bigint unsigned AUTO_INCREMENT," +
					"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL," +
					"`id` varchar(64) not null,`name` varchar(64) not null,PRIMARY KEY (`seq_id`)," +
					"UNIQUE INDEX uidx_id (`id`))")).WillReturnResult(sqlmock.NewResult(1, 1))

				mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
					sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `project_operations` (`seq_id` bigint unsigned AUTO_INCREMENT," +
					"`project_id` varchar(64) not null,`operation` varchar(16) not null,`job_id` varchar(64) not null," +
					"`created_at` datetime(3) NULL,PRIMARY KEY (`seq_id`),INDEX idx_op (`project_id`,`created_at`))")).WillReturnResult(sqlmock.NewResult(1, 1))

				mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
					sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `master_meta_kv_data` (`seq_id` bigint unsigned AUTO_INCREMENT,`created_at` datetime(3) NULL," +
					"`updated_at` datetime(3) NULL,`project_id` varchar(64) not null,`id` varchar(64) not null,`type` smallint not null COMMENT 'JobManager(1),CvsJobMaster(2),FakeJobMaster(3),DMJobMaster(4),CDCJobMaster(5)'," +
					"`status` tinyint not null COMMENT 'Uninit(1),Init(2),Finished(3),Stopped(4)',`node_id` varchar(64) not null,`address` varchar(64) not null,`epoch` bigint not null," +
					"`config` blob,`deleted` datetime(3) NULL,PRIMARY KEY (`seq_id`),INDEX idx_mst (`project_id`,`status`),UNIQUE INDEX uidx_mid (`id`))")).WillReturnResult(sqlmock.NewResult(1, 1))

				mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
					sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `worker_statuses` (`seq_id` bigint unsigned AUTO_INCREMENT," +
					"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL," +
					"`project_id` varchar(64) not null,`job_id` varchar(64) not null,`id` varchar(64) not null," +
					"`type` smallint not null COMMENT 'JobManager(1),CvsJobMaster(2),FakeJobMaster(3),DMJobMaster(4),CDCJobMaster(5),CvsTask(6),FakeTask(7),DMTask(8),CDCTask(9),WorkerDMDump(10),WorkerDMLoad(11),WorkerDMSync(12)'," +
					"`status` tinyint not null COMMENT 'Normal(1),Created(2),Init(3),Error(4),Finished(5),Stopped(6)',`errmsg` text," +
					"`ext_bytes` blob,PRIMARY KEY (`seq_id`),UNIQUE INDEX uidx_wid (`job_id`,`id`)," +
					"INDEX idx_wst (`job_id`,`status`))")).WillReturnResult(sqlmock.NewResult(1, 1))

				mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
					sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `resource_meta` (`seq_id` bigint unsigned AUTO_INCREMENT,`created_at` datetime(3) NULL," +
					"`updated_at` datetime(3) NULL,`project_id` varchar(64) not null," +
					"`id` varchar(64) not null,`job_id` varchar(64) not null,`worker_id` varchar(64) not null," +
					"`executor_id` varchar(64) not null,`gc_pending` BOOLEAN,`deleted` BOOLEAN,PRIMARY KEY (`seq_id`)," +
					"UNIQUE INDEX uidx_rid (`job_id`,`id`), INDEX idx_rei (`executor_id`,`id`))")).WillReturnResult(sqlmock.NewResult(1, 1))

				mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
					sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `logic_epoches` (`seq_id` bigint unsigned AUTO_INCREMENT,`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL,`epoch` bigint not null default 1,PRIMARY KEY (`seq_id`))")).WillReturnResult(
					sqlmock.NewResult(1, 1))
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `logic_epoches` (`created_at`,`updated_at`,`epoch`,`seq_id`) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE `seq_id`=`seq_id`")).WithArgs(
					anyTime{}, anyTime{}, 1, 1).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestProject(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []tCase{
		{
			fn: "CreateProject",
			inputs: []interface{}{
				&model.ProjectInfo{
					Model: model.Model{
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:   "p111",
					Name: "tenant1",
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `project_infos` [(]`created_at`,`updated_at`,`id`,"+
					"`name`[)]").WithArgs(createdAt, updatedAt, "p111", "tenant1").WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "CreateProject",
			inputs: []interface{}{
				&model.ProjectInfo{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:   "p111",
					Name: "tenant2",
				},
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `project_infos` [(]`created_at`,`updated_at`,`id`,"+
					"`name`,`seq_id`[)]").WithArgs(createdAt, updatedAt, "p111", "tenant2", 1).WillReturnError(errors.New("projectID is duplicated"))
			},
		},
		{
			fn: "DeleteProject",
			inputs: []interface{}{
				"p111",
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `project_infos` WHERE id").WithArgs("p111").WillReturnError(errors.New("DeleteProject error"))
			},
		},
		{
			fn: "DeleteProject",
			inputs: []interface{}{
				"p111",
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `project_infos` WHERE id").WithArgs("p111").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			fn:     "QueryProjects",
			inputs: []interface{}{},
			output: []*model.ProjectInfo{
				{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:   "p111",
					Name: "tenant1",
				},
				{
					Model: model.Model{
						SeqID:     2,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:   "p111",
					Name: "tenant2",
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_infos`").WillReturnRows(sqlmock.NewRows([]string{
					"created_at", "updated_at", "id", "name",
					"seq_id",
				}).AddRow(createdAt, updatedAt, "p111", "tenant1", 1).AddRow(createdAt, updatedAt, "p111", "tenant2", 2))
			},
		},
		{
			fn:     "QueryProjects",
			inputs: []interface{}{},
			err:    derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_infos`").WillReturnError(errors.New("QueryProjects error"))
			},
		},
		{
			// SELECT * FROM `project_infos` WHERE project_id = '111-222-333' ORDER BY `project_infos`.`id` LIMIT 1
			fn: "GetProjectByID",
			inputs: []interface{}{
				"111-222-333",
			},
			output: &model.ProjectInfo{
				Model: model.Model{
					SeqID:     2,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				ID:   "p111",
				Name: "tenant1",
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_infos` WHERE id").WithArgs("111-222-333").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "id", "name",
						"seq_id",
					}).AddRow(createdAt, updatedAt, "p111", "tenant1", 2))
			},
		},
		{
			fn: "GetProjectByID",
			inputs: []interface{}{
				"p111",
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_infos` WHERE id").WithArgs("p111").WillReturnError(
					errors.New("GetProjectByID error"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestProjectOperation(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	tm1 := tm.Add(time.Duration(1))

	testCases := []tCase{
		{
			// SELECT * FROM `project_operations` WHERE project_id = '111'
			fn: "QueryProjectOperations",
			inputs: []interface{}{
				"p111",
			},
			output: []*model.ProjectOperation{
				{
					SeqID:     1,
					ProjectID: "p111",
					Operation: "Submit",
					JobID:     "j222",
					CreatedAt: tm,
				},
				{
					SeqID:     2,
					ProjectID: "p112",
					Operation: "Drop",
					JobID:     "j222",
					CreatedAt: tm1,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_operations` WHERE project_id").WithArgs("p111").WillReturnRows(
					sqlmock.NewRows([]string{"seq_id", "project_id", "operation", "job_id", "created_at"}).AddRow(
						1, "p111", "Submit", "j222", tm).AddRow(
						2, "p112", "Drop", "j222", tm1))
			},
		},
		{
			fn: "QueryProjectOperations",
			inputs: []interface{}{
				"p111",
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_operations` WHERE project_id").WithArgs("p111").WillReturnError(errors.New("QueryProjectOperations error"))
			},
		},
		{
			// SELECT * FROM `project_operations` WHERE project_id = '111' AND created_at >= '2022-04-13 23:51:42.46' AND created_at <= '2022-04-13 23:51:42.46'
			fn: "QueryProjectOperationsByTimeRange",
			inputs: []interface{}{
				"p111",
				TimeRange{
					start: tm,
					end:   tm1,
				},
			},
			output: []*model.ProjectOperation{
				{
					SeqID:     1,
					ProjectID: "p111",
					Operation: "Submit",
					JobID:     "j222",
					CreatedAt: tm,
				},
				{
					SeqID:     2,
					ProjectID: "p112",
					Operation: "Drop",
					JobID:     "j222",
					CreatedAt: tm1,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_operations` WHERE project_id").WithArgs("p111", tm, tm1).WillReturnRows(
					sqlmock.NewRows([]string{"seq_id", "project_id", "operation", "job_id", "created_at"}).AddRow(
						1, "p111", "Submit", "j222", tm).AddRow(
						2, "p112", "Drop", "j222", tm1))
			},
		},
		{
			fn: "QueryProjectOperationsByTimeRange",
			inputs: []interface{}{
				"p111",
				TimeRange{
					start: tm,
					end:   tm1,
				},
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_operations` WHERE project_id").WithArgs("p111", tm, tm1).WillReturnError(
					errors.New("QueryProjectOperationsByTimeRange error"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestJob(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []tCase{
		{
			fn: "UpsertJob",
			inputs: []interface{}{
				&frameModel.MasterMetaKVData{
					ProjectID:  "p111",
					ID:         "j111",
					Tp:         1,
					NodeID:     "n111",
					Epoch:      1,
					StatusCode: 1,
					Addr:       "127.0.0.1",
					Config:     []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("ON DUPLICATE KEY UPDATE").WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "DeleteJob",
			inputs: []interface{}{
				"j111",
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "UPDATE `master_meta_kv_data` SET `deleted`=? WHERE id = ? AND `master_meta_kv_data`.`deleted` IS NULL"
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).WithArgs(
					anyTime{}, "j111").WillReturnError(errors.New("DeleteJob error"))
			},
		},
		{
			// DELETE FROM `master_meta_kv_data` WHERE project_id = '111-222-334' AND job_id = '111'
			fn: "DeleteJob",
			inputs: []interface{}{
				"j112",
			},
			output: &ormResult{
				rowsAffected: 1,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "UPDATE `master_meta_kv_data` SET `deleted`=? WHERE id = ? AND `master_meta_kv_data`.`deleted` IS NULL"
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).WithArgs(
					anyTime{}, "j112").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			// "UPDATE `master_meta_kv_data` SET `addr`=?,`config`=?,`epoch`=?,`id`=?,`node_id`=?,`project-id`=?,`status`=?,`type`=?,`updated_at`=? WHERE id = ?"
			fn: "UpdateJob",
			inputs: []interface{}{
				&frameModel.MasterMetaKVData{
					ProjectID:  "p111",
					ID:         "j111",
					Tp:         1,
					NodeID:     "n111",
					Epoch:      1,
					StatusCode: 1,
					Addr:       "127.0.0.1",
					Config:     []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE `master_meta_kv_data` SET").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			// SELECT * FROM `master_meta_kv_data` WHERE project_id = '111-222-333' AND job_id = '111' ORDER BY `master_meta_kv_data`.`id` LIMIT 1
			fn: "GetJobByID",
			inputs: []interface{}{
				"j111",
			},
			output: &frameModel.MasterMetaKVData{
				Model: model.Model{
					SeqID:     1,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				ProjectID:  "p111",
				ID:         "j111",
				Tp:         1,
				NodeID:     "n111",
				Epoch:      1,
				StatusCode: 1,
				Addr:       "127.0.0.1",
				Config:     []byte{0x11, 0x22},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "SELECT * FROM `master_meta_kv_data` WHERE id = ? AND `master_meta_kv_data`.`deleted` IS NULL"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).WithArgs("j111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "id",
						"type", "status", "node_id", "address", "epoch", "config", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", 1, 1, "n111", "127.0.0.1", 1, []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "GetJobByID",
			inputs: []interface{}{
				"j111",
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `master_meta_kv_data` WHERE id").WithArgs("j111").WillReturnError(
					errors.New("GetJobByID error"))
			},
		},
		{
			// SELECT * FROM `master_meta_kv_data` WHERE project_id = '111-222-333'
			fn: "QueryJobsByProjectID",
			inputs: []interface{}{
				"p111",
			},
			output: []*frameModel.MasterMetaKVData{
				{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:  "p111",
					ID:         "j111",
					Tp:         1,
					NodeID:     "n111",
					Epoch:      1,
					StatusCode: 1,
					Addr:       "1.1.1.1",
					Config:     []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `master_meta_kv_data` WHERE project_id").WithArgs("p111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "id",
						"type", "status", "node_id", "address", "epoch", "config", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", 1, 1, "n111", "1.1.1.1", 1, []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "QueryJobsByProjectID",
			inputs: []interface{}{
				"p111",
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `master_meta_kv_data` WHERE project_id").WithArgs("p111").WillReturnError(
					errors.New("QueryJobsByProjectID error"))
			},
		},
		{
			//  SELECT * FROM `master_meta_kv_data` WHERE project_id = '111-222-333' AND job_status = 1
			fn: "QueryJobsByStatus",
			inputs: []interface{}{
				"j111",
				1,
			},
			output: []*frameModel.MasterMetaKVData{
				{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:  "p111",
					ID:         "j111",
					Tp:         1,
					NodeID:     "n111",
					Epoch:      1,
					StatusCode: 1,
					Addr:       "127.0.0.1",
					Config:     []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "SELECT * FROM `master_meta_kv_data` WHERE (id = ? AND status = ?) AND `master_meta_kv_data`.`deleted` IS NULL"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).WithArgs("j111", 1).WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "id",
						"type", "status", "node_id", "address", "epoch", "config", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", 1, 1, "n111", "127.0.0.1", 1, []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "QueryJobsByStatus",
			inputs: []interface{}{
				"j111",
				1,
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "SELECT * FROM `master_meta_kv_data` WHERE (id = ? AND status = ?) AND `master_meta_kv_data`.`deleted` IS NULL"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).WithArgs("j111", 1).WillReturnError(
					errors.New("QueryJobsByStatus error"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestWorker(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []tCase{
		{
			// INSERT INTO `worker_statuses` (`created_at`,`updated_at`,`project_id`,`job_id`,`id`,`type`,`status`,`errmsg`,`ext_bytes`)
			// VALUES ('2022-04-29 18:49:40.932','2022-04-29 18:49:40.932','p111','j111','w222',1,'1','error','<binary>') ON DUPLICATE KEY
			// UPDATE `updated_at`=VALUES(`updated_at`),`project_id`=VALUES(`project_id`),`job_id`=VALUES(`job_id`),`id`=VALUES(`id`),
			// `type`=VALUES(`type`),`status`=VALUES(`status`),`errmsg`=VALUES(`errmsg`),`ext_bytes`=VALUES(`ext_bytes`)
			fn: "UpsertWorker",
			inputs: []interface{}{
				&frameModel.WorkerStatus{
					Model: model.Model{
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:    "p111",
					JobID:        "j111",
					ID:           "w222",
					Type:         1,
					Code:         1,
					ErrorMessage: "error",
					ExtBytes:     []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("ON DUPLICATE KEY UPDATE").WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "UpsertWorker",
			inputs: []interface{}{
				&frameModel.WorkerStatus{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:    "p111",
					JobID:        "j111",
					ID:           "w222",
					Type:         1,
					Code:         1,
					ErrorMessage: "error",
					ExtBytes:     []byte{0x11, 0x22},
				},
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `worker_statuses` [(]`created_at`,`updated_at`,`project_id`,`job_id`," +
					"`id`,`type`,`status`,`errmsg`,`ext_bytes`,`seq_id`[)]").WillReturnError(&mysql.MySQLError{Number: 1062, Message: "error"})
			},
		},
		{
			fn: "DeleteWorker",
			inputs: []interface{}{
				"j111",
				"w222",
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `worker_statuses` WHERE job_id").WithArgs(
					"j111", "w222").WillReturnError(errors.New("DeleteWorker error"))
			},
		},
		{
			// DELETE FROM `worker_statuses` WHERE project_id = '111-222-334' AND job_id = '111' AND worker_id = '222'
			fn: "DeleteWorker",
			inputs: []interface{}{
				"j112",
				"w223",
			},
			output: &ormResult{
				rowsAffected: 1,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `worker_statuses` WHERE job_id").WithArgs(
					"j112", "w223").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			// 'UPDATE `worker_statuses` SET `error-message`=?,`ext-bytes`=?,`id`=?,`job_id`=?,`project_id`=?,`status`=?,`type`=?,`updated_at`=? WHERE job_id = ? && id = ?'
			fn: "UpdateWorker",
			inputs: []interface{}{
				&frameModel.WorkerStatus{
					ProjectID:    "p111",
					JobID:        "j111",
					ID:           "w111",
					Type:         1,
					Code:         1,
					ErrorMessage: "error",
					ExtBytes:     []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE `worker_statuses` SET").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			// SELECT * FROM `worker_statuses` WHERE project_id = '111-222-333' AND job_id = '111' AND
			// worker_id = '222' ORDER BY `worker_statuses`.`id` LIMIT 1
			fn: "GetWorkerByID",
			inputs: []interface{}{
				"j111",
				"w222",
			},
			output: &frameModel.WorkerStatus{
				Model: model.Model{
					SeqID:     1,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				ProjectID:    "p111",
				JobID:        "j111",
				ID:           "w222",
				Type:         1,
				Code:         1,
				ErrorMessage: "error",
				ExtBytes:     []byte{0x11, 0x22},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_statuses` WHERE job_id").WithArgs("j111", "w222").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"id", "type", "status", "errmsg", "ext_bytes", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", "w222", 1, 1, "error", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "GetWorkerByID",
			inputs: []interface{}{
				"j111",
				"w222",
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_statuses` WHERE job_id").WithArgs("j111", "w222").WillReturnError(
					errors.New("GetWorkerByID error"))
			},
		},
		{
			// SELECT * FROM `worker_statuses` WHERE project_id = '111-222-333' AND job_id = '111'
			fn: "QueryWorkersByMasterID",
			inputs: []interface{}{
				"j111",
			},
			output: []*frameModel.WorkerStatus{
				{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:    "p111",
					JobID:        "j111",
					ID:           "w222",
					Type:         1,
					Code:         1,
					ErrorMessage: "error",
					ExtBytes:     []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_statuses` WHERE job_id").WithArgs("j111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"id", "type", "status", "errmsg", "ext_bytes", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", "w222", 1, 1, "error", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "QueryWorkersByMasterID",
			inputs: []interface{}{
				"j111",
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_statuses` WHERE job_id").WithArgs("j111").WillReturnError(
					errors.New("QueryWorkersByMasterID error"))
			},
		},
		{
			// SELECT * FROM `worker_statuses` WHERE project_id = '111-222-333' AND job_id = '111' AND worker_statuses = 1
			fn: "QueryWorkersByStatus",
			inputs: []interface{}{
				"j111",
				1,
			},
			output: []*frameModel.WorkerStatus{
				{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:    "p111",
					JobID:        "j111",
					ID:           "w222",
					Type:         1,
					Code:         1,
					ErrorMessage: "error",
					ExtBytes:     []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_statuses` WHERE job_id").WithArgs("j111", 1).WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"id", "type", "status", "errmsg", "ext_bytes", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", "w222", 1, 1, "error", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "QueryWorkersByStatus",
			inputs: []interface{}{
				"j111",
				1,
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_statuses` WHERE job_id").WithArgs("j111", 1).WillReturnError(
					errors.New("QueryWorkersByStatus error"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestResource(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []tCase{
		{
			fn: "CreateResource",
			inputs: []interface{}{
				&resModel.ResourceMeta{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:        "r333",
					ProjectID: "111-222-333",
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e444",
					Deleted:   false,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta("SELECT count(*) FROM `resource_meta` WHERE job_id = ? AND id = ?")).WithArgs("j111", "r333").WillReturnRows(
					sqlmock.NewRows([]string{
						"count(1)",
					}).AddRow(0))
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `resource_meta` (`created_at`,`updated_at`,`project_id`,`id`,`job_id`,"+
					"`worker_id`,`executor_id`,`gc_pending`,`deleted`,`seq_id`)")).WithArgs(
					createdAt, updatedAt, "111-222-333", "r333", "j111", "w222", "e444", false, false, 1).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			},
		},
		{
			fn: "CreateResource",
			inputs: []interface{}{
				&resModel.ResourceMeta{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:        "r333",
					ProjectID: "111-222-333",
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e444",
					Deleted:   false,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta("SELECT count(*) FROM `resource_meta` WHERE job_id = ? AND id = ?")).WithArgs("j111", "r333").WillReturnRows(
					sqlmock.NewRows([]string{
						"count(1)",
					}).AddRow(1))
				mock.ExpectRollback()
			},
			err: derror.ErrDuplicateResourceID.GenWithStackByArgs("r333"),
		},
		{
			fn: "UpsertResource",
			inputs: []interface{}{
				&resModel.ResourceMeta{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:        "r333",
					ProjectID: "111-222-333",
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e445",
					Deleted:   true,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("ON DUPLICATE KEY UPDATE").WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "UpsertResource",
			inputs: []interface{}{
				&resModel.ResourceMeta{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:        "r333",
					ProjectID: "111-222-333",
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e444",
					Deleted:   true,
				},
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `resource_meta` (`created_at`,`updated_at`,`project_id`,`id`,`job_id`,"+
					"`worker_id`,`executor_id`,`gc_pending`,`deleted`,`seq_id`)")).WithArgs(
					createdAt, updatedAt, "111-222-333", "r333", "j111", "w222", "e444", false, true, 1).WillReturnError(&mysql.MySQLError{Number: 1062, Message: "error"})
			},
		},
		{
			fn: "DeleteResource",
			inputs: []interface{}{
				ResourceKey{
					JobID: "j111",
					ID:    "r222",
				},
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `resource_meta` WHERE job_id = ? AND id = ?")).WithArgs(
					"j111", "r222").WillReturnError(errors.New("DeleteReource error"))
			},
		},
		{
			fn: "DeleteResource",
			inputs: []interface{}{
				ResourceKey{
					JobID: "j111",
					ID:    "r223",
				},
			},
			output: &ormResult{
				rowsAffected: 1,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `resource_meta` WHERE job_id = ? AND id = ?")).WithArgs(
					"j111", "r223").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			// 'UPDATE `resource_meta` SET `deleted`=?,`executor_id`=?,`id`=?,`job_id`=?,`project_id`=?,`worker_id`=?,`updated_at`=? WHERE id = ?'
			fn: "UpdateResource",
			inputs: []interface{}{
				&resModel.ResourceMeta{
					ProjectID: "p111",
					ID:        "w111",
					Job:       "j111",
					Worker:    "w111",
					Executor:  "e111",
					Deleted:   true,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("UPDATE `resource_meta` SET")).WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			fn: "GetResourceByID",
			inputs: []interface{}{
				ResourceKey{
					JobID: "j111",
					ID:    "r222",
				},
			},
			output: &resModel.ResourceMeta{
				Model: model.Model{
					SeqID:     1,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				ID:        "r333",
				ProjectID: "111-222-333",
				Job:       "j111",
				Worker:    "w222",
				Executor:  "e444",
				Deleted:   true,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `resource_meta` WHERE job_id = ? AND id = ?")).WithArgs("j111", "r222").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "id", "job_id",
						"worker_id", "executor_id", "deleted", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "111-222-333", "r333", "j111", "w222", "e444", true, 1))
			},
		},
		{
			fn: "GetResourceByID",
			inputs: []interface{}{
				ResourceKey{
					JobID: "j111",
					ID:    "r222",
				},
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `resource_meta` WHERE job_id = ? AND id = ?")).WithArgs("j111", "r222").WillReturnError(
					errors.New("GetResourceByID error"))
			},
		},
		{
			fn: "QueryResourcesByJobID",
			inputs: []interface{}{
				"j111",
			},
			output: []*resModel.ResourceMeta{
				{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:        "r333",
					ProjectID: "111-222-333",
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e444",
					Deleted:   true,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `resource_meta` WHERE job_id = ?")).WithArgs("j111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "id", "job_id",
						"worker_id", "executor_id", "deleted", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "111-222-333", "r333", "j111", "w222", "e444", true, 1))
			},
		},
		{
			fn: "QueryResourcesByJobID",
			inputs: []interface{}{
				"j111",
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `resource_meta` WHERE job_id")).WithArgs("j111").WillReturnError(
					errors.New("QueryResourcesByJobID error"))
			},
		},
		{
			fn: "QueryResourcesByExecutorID",
			inputs: []interface{}{
				"e444",
			},
			output: []*resModel.ResourceMeta{
				{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:        "r333",
					ProjectID: "111-222-333",
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e444",
					Deleted:   true,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `resource_meta` WHERE executor_id = ?")).WithArgs("e444").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "id", "job_id",
						"worker_id", "executor_id", "deleted", "seq_id",
					}).AddRow(createdAt, updatedAt, "111-222-333", "r333", "j111", "w222", "e444", true, 1))
			},
		},
		{
			fn: "QueryResourcesByExecutorID",
			inputs: []interface{}{
				"e444",
			},
			err: derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `resource_meta` WHERE executor_id")).WithArgs("e444").WillReturnError(
					errors.New("QueryResourcesByExecutorID error"))
			},
		},
		{
			fn: "SetGCPendingByJobs",
			inputs: []interface{}{
				[]string{
					"job-1",
					"job-2",
					"job-3",
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "UPDATE `resource_meta` SET `gc_pending`=?,`updated_at`=? WHERE job_id in"
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs(
						true,
						anyTime{},
						"job-1",
						"job-2",
						"job-3").
					WillReturnResult(driver.RowsAffected(1))
			},
		},
		{
			fn: "DeleteResourcesByExecutorID",
			inputs: []interface{}{
				engineModel.ExecutorID("executor-1"),
			},
			output: &ormResult{rowsAffected: 1},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "DELETE FROM `resource_meta` WHERE executor_id"
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs("executor-1").
					WillReturnResult(driver.RowsAffected(1))
			},
		},
		{
			fn: "GetOneResourceForGC",
			output: &resModel.ResourceMeta{
				Model: model.Model{
					SeqID:     1,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				ID:        "resource-1",
				Job:       "job-1",
				Worker:    "worker-1",
				Executor:  "executor-1",
				GCPending: true,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "SELECT * FROM `resource_meta` WHERE gc_pending = true ORDER BY"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
					WillReturnRows(
						sqlmock.NewRows([]string{
							"created_at", "updated_at", "project_id", "id", "job_id",
							"worker_id", "executor_id", "deleted", "gc_pending", "seq_id",
						}).AddRow(createdAt, updatedAt, "", "resource-1", "job-1", "worker-1", "executor-1", false, true, 1))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestError(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	mock.ExpectQuery("SELECT [*] FROM `project_infos`").WillReturnRows(sqlmock.NewRows([]string{
		"created_at", "updated_at", "id", "name",
		"seq_id",
	}))
	res, err := cli.QueryProjects(context.TODO())
	require.Nil(t, err)
	require.Len(t, res, 0)

	mock.ExpectQuery("SELECT [*] FROM `project_infos` WHERE id").WithArgs("p111").WillReturnRows(
		sqlmock.NewRows([]string{
			"created_at", "updated_at", "id", "name",
			"seq_id",
		}))
	res2, err := cli.GetProjectByID(context.TODO(), "p111")
	require.Nil(t, res2)
	require.Error(t, err)
	e, ok := err.(*perrors.Error)
	require.True(t, ok)
	require.True(t, e.Is(derror.ErrMetaEntryNotFound))
}

func TestLogicEpoch(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	testCases := []tCase{
		{
			fn:     "GenEpoch",
			inputs: []interface{}{},
			err:    derror.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `logic_epoches`").WillReturnError(
					errors.New("InitializeEpoch error"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
	defer cancel()

	cli, err := NewMockClient()
	require.NoError(t, err)
	defer cli.Close()

	// test normal function
	err = failpoint.Enable("github.com/pingcap/tiflow/engine/pkg/orm/initializedDelay", "sleep(2000)")
	require.NoError(t, err)
	ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
		return ctx.Value(fpname) != nil
	})
	ctx2 := context.WithValue(ctx, "github.com/pingcap/tiflow/engine/pkg/orm/initializedDelay", struct{}{})

	err = cli.Initialize(ctx2)
	require.Error(t, err)
	require.Regexp(t, "context deadline exceed", err.Error())
	failpoint.Disable("github.com/pingcap/tiflow/engine/pkg/orm/initializedDelay")

	// test transaction
	ctx, cancel = context.WithTimeout(context.TODO(), 1*time.Second)
	defer cancel()

	err = failpoint.Enable("github.com/pingcap/tiflow/engine/pkg/orm/genEpochDelay", "sleep(2000)")
	require.NoError(t, err)
	ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
		return ctx.Value(fpname) != nil
	})
	ctx2 = context.WithValue(ctx, "github.com/pingcap/tiflow/engine/pkg/orm/genEpochDelay", struct{}{})

	_, err = cli.GenEpoch(ctx2)
	require.Error(t, err)
	require.Regexp(t, "context deadline exceed", err.Error())
	failpoint.Disable("github.com/pingcap/tiflow/engine/pkg/orm/model/genEpochDelay")
}

func testInner(t *testing.T, m sqlmock.Sqlmock, cli Client, c tCase) {
	// set the mock expectation
	c.mockExpectResFn(m)

	var args []reflect.Value
	args = append(args, reflect.ValueOf(context.Background()))
	for _, ip := range c.inputs {
		args = append(args, reflect.ValueOf(ip))
	}
	result := reflect.ValueOf(cli).MethodByName(c.fn).Call(args)
	// only error
	if len(result) == 1 {
		if c.err == nil {
			require.Nil(t, result[0].Interface())
		} else {
			require.NotNil(t, result[0].Interface())
			require.Error(t, result[0].Interface().(error))
		}
	} else if len(result) == 2 {
		// result and error
		if c.err != nil {
			require.NotNil(t, result[1].Interface())
			require.Error(t, result[1].Interface().(error))
		} else {
			require.Equal(t, c.output, result[0].Interface())
		}
	}
}
