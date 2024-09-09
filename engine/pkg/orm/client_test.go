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
	"fmt"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/failpoint"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	engineModel "github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	metaMock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
	"github.com/stretchr/testify/require"
)

const (
	defaultTestStoreType = metaModel.StoreTypeMySQL
)

type tCase struct {
	fn     string        // function name
	inputs []interface{} // function args

	output interface{} // function output
	err    error       // function error

	mockExpectResFn func(mock sqlmock.Sqlmock) // sqlmock expectation
}

func mockGetDBConn(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	// common execution for orm
	mock.ExpectQuery("SELECT VERSION()").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.35-log"))
	return db, mock
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
	_, err := NewClient(nil)
	require.Error(t, err)

	sqlDB, mock := mockGetDBConn(t)
	defer sqlDB.Close()
	defer mock.ExpectClose()
	_, err = newClient(sqlDB, defaultTestStoreType)
	require.Nil(t, err)
}

func TestProject(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t)
	defer sqlDB.Close()
	defer mock.ExpectClose()
	cli, err := newClient(sqlDB, defaultTestStoreType)
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
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
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
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
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
			err:    errors.ErrMetaOpFail.GenWithStackByArgs(),
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
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
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

	sqlDB, mock := mockGetDBConn(t)
	defer sqlDB.Close() //nolint: staticcheck
	defer mock.ExpectClose()
	cli, err := newClient(sqlDB, defaultTestStoreType)
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
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
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
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
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

	sqlDB, mock := mockGetDBConn(t)
	defer sqlDB.Close()
	defer mock.ExpectClose()
	cli, err := newClient(sqlDB, defaultTestStoreType)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	extForTest := frameModel.MasterMetaExt{
		Selectors: []*label.Selector{
			{
				Key:    "test",
				Target: "test-val",
				Op:     label.OpEq,
			},
		},
	}
	extJSONForTest := `{"selectors":[{"label":"test","target":"test-val","op":"eq"}]}`

	testCases := []tCase{
		{
			fn: "InsertJob",
			inputs: []interface{}{
				&frameModel.MasterMeta{
					Model: model.Model{
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID: "p111",
					ID:        "j111",
					Type:      1,
					NodeID:    "n111",
					Epoch:     1,
					State:     1,
					Addr:      "127.0.0.1",
					Config:    []byte{0x11, 0x22},
					Ext:       extForTest,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `master_meta` [(]`created_at`,"+
					"`updated_at`,`project_id`,`id`,`type`,`state`,`node_id`,"+
					"`address`,`epoch`,`config`,`error_message`,`detail`,"+
					"`ext`,`deleted`[)]").
					WithArgs(createdAt, updatedAt, "p111", "j111", 1, 1, "n111",
						"127.0.0.1", 1, []byte{0x11, 0x22}, sqlmock.AnyArg(),
						sqlmock.AnyArg(), extForTest, nil).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "InsertJob",
			inputs: []interface{}{
				&frameModel.MasterMeta{
					Model: model.Model{
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID: "p111",
					ID:        "j111",
					Type:      1,
					NodeID:    "n111",
					Epoch:     1,
					State:     1,
					Addr:      "127.0.0.1",
					Config:    []byte{0x11, 0x22},
					Ext:       extForTest,
				},
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `master_meta` [(]`created_at`," +
					"`updated_at`,`project_id`,`id`,`type`,`state`,`node_id`," +
					"`address`,`epoch`,`config`,`error_message`,`detail`," +
					"`ext`,`deleted`[)]").
					WillReturnError(&mysql.MySQLError{Number: 1062, Message: "Duplicate entry '123456' for key 'uidx_mid'"})
			},
		},
		{
			fn: "UpsertJob",
			inputs: []interface{}{
				&frameModel.MasterMeta{
					ProjectID: "p111",
					ID:        "j111",
					Type:      1,
					NodeID:    "n111",
					Epoch:     1,
					State:     1,
					Addr:      "127.0.0.1",
					Config:    []byte{0x11, 0x22},
					Ext:       extForTest,
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
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "UPDATE `master_meta` SET `deleted`=? WHERE id = ? AND `master_meta`.`deleted` IS NULL"
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).WithArgs(
					anyTime{}, "j111").WillReturnError(errors.New("DeleteJob error"))
			},
		},
		{
			// DELETE FROM `master_meta` WHERE project_id = '111-222-334' AND job_id = '111'
			fn: "DeleteJob",
			inputs: []interface{}{
				"j112",
			},
			output: &ormResult{
				rowsAffected: 1,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "UPDATE `master_meta` SET `deleted`=? WHERE id = ? AND `master_meta`.`deleted` IS NULL"
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).WithArgs(
					anyTime{}, "j112").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			fn: "UpdateJob",
			inputs: []interface{}{
				"j111",
				(&frameModel.MasterMeta{
					ProjectID: "p111",
					ID:        "j111",
					Type:      1,
					NodeID:    "n111",
					Epoch:     1,
					State:     1,
					Addr:      "127.0.0.1",
					Config:    []byte{0x11, 0x22},
				}).RefreshValues(),
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta(
					"UPDATE `master_meta` SET `address`=?,`epoch`=?,`node_id`=?,`updated_at`=? WHERE id = ? AND `master_meta`.`deleted` IS NULL")).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			fn: "UpdateJob",
			inputs: []interface{}{
				"j111",
				(&frameModel.MasterMeta{
					ProjectID: "p111",
					ID:        "j111",
					Type:      1,
					NodeID:    "n111",
					Epoch:     1,
					State:     1,
					Addr:      "127.0.0.1",
					Config:    []byte{0x11, 0x22},
				}).UpdateStateValues(),
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta(
					"UPDATE `master_meta` SET `state`=?,`updated_at`=? WHERE id = ? AND `master_meta`.`deleted` IS NULL")).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			fn: "UpdateJob",
			inputs: []interface{}{
				"j111",
				(&frameModel.MasterMeta{
					ProjectID: "p111",
					ID:        "j111",
					Type:      1,
					NodeID:    "n111",
					Epoch:     1,
					State:     1,
					Addr:      "127.0.0.1",
					Config:    []byte{0x11, 0x22},
					ErrorMsg:  "error message",
					Detail:    []byte("job detail"),
				}).UpdateErrorValues(),
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta(
					"UPDATE `master_meta` SET `error_message`=?,`updated_at`=? WHERE id = ? AND `master_meta`.`deleted` IS NULL")).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			fn: "UpdateJob",
			inputs: []interface{}{
				"j111",
				(&frameModel.MasterMeta{
					ProjectID: "p111",
					ID:        "j111",
					Type:      1,
					NodeID:    "n111",
					Epoch:     1,
					State:     1,
					Addr:      "127.0.0.1",
					Config:    []byte{0x11, 0x22},
					ErrorMsg:  "error message",
					Detail:    []byte("job detail"),
				}).ExitValues(),
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta(
					"UPDATE `master_meta` SET `detail`=?,`error_message`=?,`state`=?,`updated_at`=? WHERE id = ? AND `master_meta`.`deleted` IS NULL")).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			// SELECT * FROM `master_meta` WHERE project_id = '111-222-333' AND job_id = '111' ORDER BY `master_meta`.`id` LIMIT 1
			fn: "GetJobByID",
			inputs: []interface{}{
				"j111",
			},
			output: &frameModel.MasterMeta{
				Model: model.Model{
					SeqID:     1,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				ProjectID: "p111",
				ID:        "j111",
				Type:      1,
				NodeID:    "n111",
				Epoch:     1,
				State:     1,
				Addr:      "127.0.0.1",
				Config:    []byte{0x11, 0x22},
				Ext:       extForTest,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "SELECT * FROM `master_meta` WHERE id = ? AND `master_meta`.`deleted` IS NULL"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).WithArgs("j111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "id",
						"type", "state", "node_id", "address", "epoch", "config", "seq_id", "ext",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", 1, 1, "n111", "127.0.0.1", 1, []byte{0x11, 0x22}, 1,
						extJSONForTest))
			},
		},
		{
			fn: "GetJobByID",
			inputs: []interface{}{
				"j111",
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `master_meta` WHERE id").WithArgs("j111").WillReturnError(
					errors.New("GetJobByID error"))
			},
		},
		{
			// SELECT * FROM `master_meta` WHERE project_id = '111-222-333'
			fn: "QueryJobsByProjectID",
			inputs: []interface{}{
				"p111",
			},
			output: []*frameModel.MasterMeta{
				{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID: "p111",
					ID:        "j111",
					Type:      1,
					NodeID:    "n111",
					Epoch:     1,
					State:     1,
					Addr:      "1.1.1.1",
					Config:    []byte{0x11, 0x22},
					Ext:       extForTest,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `master_meta` WHERE project_id").WithArgs("p111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "id",
						"type", "state", "node_id", "address", "epoch", "config", "seq_id", "ext",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", 1, 1, "n111", "1.1.1.1", 1, []byte{0x11, 0x22}, 1, extJSONForTest))
			},
		},
		{
			fn: "QueryJobsByProjectID",
			inputs: []interface{}{
				"p111",
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `master_meta` WHERE project_id").WithArgs("p111").WillReturnError(
					errors.New("QueryJobsByProjectID error"))
			},
		},
		{
			//  SELECT * FROM `master_meta` WHERE project_id = '111-222-333' AND job_status = 1
			fn: "QueryJobsByState",
			inputs: []interface{}{
				"p111",
				1,
			},
			output: []*frameModel.MasterMeta{
				{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID: "p111",
					ID:        "j111",
					Type:      1,
					NodeID:    "n111",
					Epoch:     1,
					State:     1,
					Addr:      "127.0.0.1",
					Config:    []byte{0x11, 0x22},
					Ext:       extForTest,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "SELECT * FROM `master_meta` WHERE (project_id = ? AND state = ?) AND `master_meta`.`deleted` IS NULL"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).WithArgs("p111", 1).WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "id",
						"type", "state", "node_id", "address", "epoch", "config", "seq_id", "ext",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", 1, 1, "n111", "127.0.0.1", 1, []byte{0x11, 0x22}, 1, extJSONForTest))
			},
		},
		{
			fn: "QueryJobsByState",
			inputs: []interface{}{
				"p111",
				1,
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "SELECT * FROM `master_meta` WHERE (project_id = ? AND state = ?) AND `master_meta`.`deleted` IS NULL"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).WithArgs("p111", 1).WillReturnError(
					errors.New("QueryJobsByState error"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestWorker(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t)
	defer sqlDB.Close()
	defer mock.ExpectClose()
	cli, err := newClient(sqlDB, defaultTestStoreType)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []tCase{
		{
			// INSERT INTO `worker_statuses` (`created_at`,`updated_at`,`project_id`,`job_id`,`id`,`type`,`state`,`epoch`,`error_message`,`extend_bytes`)
			// VALUES ('2022-04-29 18:49:40.932','2022-04-29 18:49:40.932','p111','j111','w222',1,'1',10,'error','<binary>') ON DUPLICATE KEY
			// UPDATE `updated_at`=VALUES(`updated_at`),`project_id`=VALUES(`project_id`),`job_id`=VALUES(`job_id`),`id`=VALUES(`id`),
			// `type`=VALUES(`type`),`state`=VALUES(`state`),`epoch`=VALUES(`epoch`),`error_message`=VALUES(`error_message`),`extend_bytes`=VALUES(`extend_bytes`)
			fn: "UpsertWorker",
			inputs: []interface{}{
				&frameModel.WorkerStatus{
					Model: model.Model{
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID: "p111",
					JobID:     "j111",
					ID:        "w222",
					Type:      1,
					State:     1,
					Epoch:     10,
					ErrorMsg:  "error",
					ExtBytes:  []byte{0x11, 0x22},
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
					ProjectID: "p111",
					JobID:     "j111",
					ID:        "w222",
					Type:      1,
					State:     1,
					Epoch:     10,
					ErrorMsg:  "error",
					ExtBytes:  []byte{0x11, 0x22},
				},
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `worker_statuses` [(]`created_at`,`updated_at`,`project_id`,`job_id`," +
					"`id`,`type`,`state`,`epoch`,`error_message`,`extend_bytes`,`seq_id`[)]").WillReturnError(&mysql.MySQLError{Number: 1062, Message: "error"})
			},
		},
		{
			fn: "DeleteWorker",
			inputs: []interface{}{
				"j111",
				"w222",
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
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
			// 'UPDATE `worker_statuses` SET `epoch`=?,`error-message`=?,`extend-bytes`=?,`id`=?,`job_id`=?,`project_id`=?,`status`=?,`type`=?,`updated_at`=? WHERE job_id = ? && id = ?'
			fn: "UpdateWorker",
			inputs: []interface{}{
				&frameModel.WorkerStatus{
					ProjectID: "p111",
					JobID:     "j111",
					ID:        "w111",
					Type:      1,
					State:     1,
					Epoch:     10,
					ErrorMsg:  "error",
					ExtBytes:  []byte{0x11, 0x22},
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
				ProjectID: "p111",
				JobID:     "j111",
				ID:        "w222",
				Type:      1,
				State:     1,
				Epoch:     10,
				ErrorMsg:  "error",
				ExtBytes:  []byte{0x11, 0x22},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_statuses` WHERE job_id").WithArgs("j111", "w222").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"id", "type", "state", "epoch", "error_message", "extend_bytes", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", "w222", 1, 1, 10, "error", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "GetWorkerByID",
			inputs: []interface{}{
				"j111",
				"w222",
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
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
					ProjectID: "p111",
					JobID:     "j111",
					ID:        "w222",
					Type:      1,
					State:     1,
					Epoch:     10,
					ErrorMsg:  "error",
					ExtBytes:  []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_statuses` WHERE job_id").WithArgs("j111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"id", "type", "state", "epoch", "error_message", "extend_bytes", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", "w222", 1, 1, 10, "error", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "QueryWorkersByMasterID",
			inputs: []interface{}{
				"j111",
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_statuses` WHERE job_id").WithArgs("j111").WillReturnError(
					errors.New("QueryWorkersByMasterID error"))
			},
		},
		{
			// SELECT * FROM `worker_statuses` WHERE project_id = '111-222-333' AND job_id = '111' AND worker_statuses = 1
			fn: "QueryWorkersByState",
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
					ProjectID: "p111",
					JobID:     "j111",
					ID:        "w222",
					Type:      1,
					State:     1,
					Epoch:     10,
					ErrorMsg:  "error",
					ExtBytes:  []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_statuses` WHERE job_id").WithArgs("j111", 1).WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"id", "type", "state", "epoch", "error_message", "extend_bytes", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", "w222", 1, 1, 10, "error", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "QueryWorkersByState",
			inputs: []interface{}{
				"j111",
				1,
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_statuses` WHERE job_id").WithArgs("j111", 1).WillReturnError(
					errors.New("QueryWorkersByState error"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestResource(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t)
	defer sqlDB.Close()
	defer mock.ExpectClose()
	cli, err := newClient(sqlDB, defaultTestStoreType)
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
					TenantID:  "111-222-333",
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
						"count(*)",
					}).AddRow(0))
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `resource_meta` (`created_at`,`updated_at`,`project_id`,`tenant_id`,`id`,`job_id`,"+
					"`worker_id`,`executor_id`,`gc_pending`,`deleted`,`seq_id`)")).WithArgs(
					createdAt, updatedAt, "111-222-333", "111-222-333", "r333", "j111", "w222", "e444", false, false, 1).
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
					TenantID:  "111-222-333",
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
			err: errors.ErrDuplicateResourceID.GenWithStackByArgs("r333"),
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
					TenantID:  "",
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e444",
					Deleted:   true,
				},
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `resource_meta` (`created_at`,`updated_at`,`project_id`,`tenant_id`,`id`,`job_id`,"+
					"`worker_id`,`executor_id`,`gc_pending`,`deleted`,`seq_id`)")).WithArgs(
					createdAt, updatedAt, "111-222-333", "", "r333", "j111", "w222", "e444", false, true, 1).WillReturnError(&mysql.MySQLError{Number: 1062, Message: "error"})
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
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
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
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
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
						"created_at", "updated_at", "project_id", "tenant_id", "id", "job_id",
						"worker_id", "executor_id", "deleted", "seq_id",
					}).AddRow(
						createdAt, updatedAt, "111-222-333", "", "r333", "j111", "w222", "e444", true, 1))
			},
		},
		{
			fn: "QueryResourcesByJobID",
			inputs: []interface{}{
				"j111",
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `resource_meta` WHERE job_id")).WithArgs("j111").WillReturnError(
					errors.New("QueryResourcesByJobID error"))
			},
		},
		{
			fn: "QueryResourcesByExecutorIDs",
			inputs: []interface{}{
				engineModel.ExecutorID("e444"),
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
					TenantID:  "333-222-111",
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e444",
					Deleted:   true,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `resource_meta` WHERE executor_id in")).WithArgs("e444").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "tenant_id", "id", "job_id",
						"worker_id", "executor_id", "deleted", "seq_id",
					}).AddRow(createdAt, updatedAt, "111-222-333", "333-222-111", "r333", "j111", "w222", "e444", true, 1))
			},
		},
		{
			fn: "QueryResourcesByExecutorIDs",
			inputs: []interface{}{
				engineModel.ExecutorID("e444"),
				engineModel.ExecutorID("e555"),
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
					TenantID:  "333-222-111",
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e444",
					Deleted:   true,
				},
				{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:        "r333",
					ProjectID: "111-222-333",
					TenantID:  "333-222-111",
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e555",
					Deleted:   true,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `resource_meta` WHERE executor_id in")).
					WithArgs("e444", "e555").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "tenant_id", "id", "job_id",
						"worker_id", "executor_id", "deleted", "seq_id",
					}).AddRow(createdAt, updatedAt, "111-222-333", "333-222-111", "r333", "j111", "w222", "e444", true, 1).
						AddRow(createdAt, updatedAt, "111-222-333", "333-222-111", "r333", "j111", "w222", "e555", true, 1),
				)
			},
		},
		{
			fn: "QueryResourcesByExecutorIDs",
			inputs: []interface{}{
				engineModel.ExecutorID("e444"),
			},
			err: errors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `resource_meta` WHERE executor_id in")).WithArgs("e444").WillReturnError(
					errors.New("QueryResourcesByExecutorIDs error"))
			},
		},
		{
			fn: "SetGCPendingByJobs",
			inputs: []interface{}{
				"job-1",
				"job-2",
				"job-3",
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
			fn: "DeleteResourcesByTypeAndExecutorIDs",
			inputs: []interface{}{
				resModel.ResourceTypeLocalFile,
				engineModel.ExecutorID("executor-1"),
			},
			output: &ormResult{rowsAffected: 1},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "DELETE FROM `resource_meta` WHERE executor_id = ? and id like"
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs("executor-1", "/local%").
					WillReturnResult(driver.RowsAffected(1))
			},
		},
		{
			fn: "DeleteResourcesByTypeAndExecutorIDs",
			inputs: []interface{}{
				resModel.ResourceTypeLocalFile,
				engineModel.ExecutorID("executor-1"),
				engineModel.ExecutorID("executor-2"),
			},
			output: &ormResult{rowsAffected: 2},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "DELETE FROM `resource_meta` WHERE executor_id in (?,?) and id like"
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs("executor-1", "executor-2", "/local%").
					WillReturnResult(driver.RowsAffected(2))
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
		fmt.Println("test case", tc.fn)
		testInner(t, mock, cli, tc)
	}
}

func TestError(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t)
	defer sqlDB.Close()
	defer mock.ExpectClose()
	cli, err := newClient(sqlDB, defaultTestStoreType)
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
	require.True(t, errors.Is(err, errors.ErrMetaEntryNotFound))
}

func TestContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
	defer cancel()

	db, mock := mockGetDBConn(t)
	defer db.Close()
	defer mock.ExpectClose()

	conn := metaMock.NewClientConnWithDB(db)
	require.NotNil(t, conn)
	defer conn.Close()

	// test normal function
	err := failpoint.Enable("github.com/pingcap/tiflow/engine/pkg/orm/initializedDelay", "sleep(2000)")
	require.NoError(t, err)
	ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
		return ctx.Value(fpname) != nil
	})
	ctx2 := context.WithValue(ctx, "github.com/pingcap/tiflow/engine/pkg/orm/initializedDelay", struct{}{})

	// NEED enable failpoint here, or you will meet sql mock NOT MATCH error
	err = InitAllFrameworkModels(ctx2, conn)
	require.Error(t, err)
	require.Regexp(t, "context deadline exceed", err.Error())
	failpoint.Disable("github.com/pingcap/tiflow/engine/pkg/orm/initializedDelay")
}

func TestJobOp(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t)
	defer sqlDB.Close()
	defer mock.ExpectClose()
	cli, err := newClient(sqlDB, defaultTestStoreType)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []tCase{
		// SetJobCanceling successfully
		{
			fn: "SetJobCanceling",
			inputs: []interface{}{
				"job-111",
			},
			output: &ormResult{
				rowsAffected: 1,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(
					"SELECT count(*) FROM `job_ops` WHERE job_id = ?")).
					WithArgs("job-111").WillReturnRows(
					sqlmock.NewRows([]string{
						"count(0)",
					}).AddRow(0))
				mock.ExpectExec(regexp.QuoteMeta(
					"INSERT INTO `job_ops` (`created_at`,`updated_at`,`op`,`job_id`")).
					WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), model.JobOpStatusCanceling, "job-111").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			},
		},
		// SetJobCanceling does nothing because cancelling op exists
		{
			fn: "SetJobCanceling",
			inputs: []interface{}{
				"job-111",
			},
			output: &ormResult{
				rowsAffected: 0,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(
					"SELECT count(*) FROM `job_ops` WHERE job_id = ?")).
					WithArgs("job-111").WillReturnRows(
					sqlmock.NewRows([]string{
						"count(1)",
					}).AddRow(1))
				mock.ExpectQuery(
					"SELECT [*] FROM `job_ops` WHERE job_id = ?").
					WithArgs("job-111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "op", "job_id", "seq_id",
					}).AddRow(createdAt, updatedAt, model.JobOpStatusCanceling, "job-111", 1))
				mock.ExpectCommit()
			},
		},
		// SetJobCanceling returns error if job is already cancelled
		{
			fn: "SetJobCanceling",
			inputs: []interface{}{
				"job-111",
			},
			output: &ormResult{
				rowsAffected: 0,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(
					"SELECT count(*) FROM `job_ops` WHERE job_id = ?")).
					WithArgs("job-111").WillReturnRows(
					sqlmock.NewRows([]string{
						"count(1)",
					}).AddRow(1))
				mock.ExpectQuery(
					"SELECT [*] FROM `job_ops` WHERE job_id = ?").
					WithArgs("job-111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "op", "job_id", "seq_id",
					}).AddRow(createdAt, updatedAt, model.JobOpStatusCanceled, "job-111", 1))
				mock.ExpectRollback()
			},
			err: errors.ErrJobAlreadyCanceled.GenWithStackByArgs("job-111"),
		},
		// SetJobCanceling updates job operation to canceling if exists a noop job operation
		{
			fn: "SetJobCanceling",
			inputs: []interface{}{
				"job-111",
			},
			output: &ormResult{
				rowsAffected: 2,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(
					"SELECT count(*) FROM `job_ops` WHERE job_id = ?")).
					WithArgs("job-111").WillReturnRows(
					sqlmock.NewRows([]string{
						"count(1)",
					}).AddRow(1))
				mock.ExpectQuery(
					"SELECT [*] FROM `job_ops` WHERE job_id = ?").
					WithArgs("job-111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "op", "job_id", "seq_id",
					}).AddRow(createdAt, updatedAt, model.JobOpStatusNoop, "job-111", 1))
				mock.ExpectExec("ON DUPLICATE KEY UPDATE").WillReturnResult(sqlmock.NewResult(1, 2))
				mock.ExpectCommit()
			},
		},
		// SetJobCanceled
		{
			fn: "SetJobCanceled",
			inputs: []interface{}{
				"job-111",
			},
			output: &ormResult{
				rowsAffected: 1,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "UPDATE `job_ops` SET `op`=?,`updated_at`=? WHERE job_id = ? AND op = ?"
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs(model.JobOpStatusCanceled, anyTime{}, "job-111", model.JobOpStatusCanceling).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		// QueryJobOp
		{
			fn: "QueryJobOp",
			inputs: []interface{}{
				"job-1",
			},
			output: &model.JobOp{
				Model: model.Model{
					SeqID:     1,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				JobID: "job-1",
				Op:    model.JobOpStatusCanceling,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "SELECT * FROM `job_ops` WHERE job_id = ?"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
					WillReturnRows(
						sqlmock.NewRows([]string{"created_at", "updated_at", "op", "job_id", "seq_id"}).
							AddRow(createdAt, updatedAt, model.JobOpStatusCanceling, "job-1", 1))
			},
		},
		// QueryJobOpsByStatus
		{
			fn: "QueryJobOpsByStatus",
			inputs: []interface{}{
				model.JobOpStatusCanceling,
			},
			output: []*model.JobOp{
				{
					Model: model.Model{
						SeqID:     1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					JobID: "job-1",
					Op:    model.JobOpStatusCanceling,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				expectedSQL := "SELECT * FROM `job_ops` WHERE op = ?"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
					WithArgs(model.JobOpStatusCanceling).
					WillReturnRows(
						sqlmock.NewRows([]string{"created_at", "updated_at", "op", "job_id", "seq_id"}).
							AddRow(createdAt, updatedAt, model.JobOpStatusCanceling, "job-1", 1))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestExecutorClient(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t)
	defer sqlDB.Close()
	defer mock.ExpectClose()
	cli, err := newClient(sqlDB, defaultTestStoreType)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	executor := &model.Executor{
		Model: model.Model{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		},
		ID:      "executor-0-1234",
		Name:    "executor-0",
		Address: "127.0.0.1:1234",
		Labels: map[label.Key]label.Value{
			"key1": "val1",
			"key2": "val2",
		},
	}

	testCases := []tCase{
		{
			fn: "CreateExecutor",
			inputs: []interface{}{
				executor,
			},
			output: &ormResult{
				rowsAffected: 1,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `executors` (`created_at`,`updated_at`,`id`,`name`,`address`,`labels`) VALUES (?,?,?,?,?,?)")).
					WithArgs(createdAt, updatedAt, executor.ID, executor.Name, executor.Address, "{\"key1\":\"val1\",\"key2\":\"val2\"}").
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "UpdateExecutor",
			inputs: []interface{}{
				executor,
			},
			output: &ormResult{
				rowsAffected: 1,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("UPDATE `executors` SET `address`=?,`id`=?,`labels`=?,`name`=?,`updated_at`=? WHERE id = ?")).
					WithArgs(executor.Address, executor.ID, "{\"key1\":\"val1\",\"key2\":\"val2\"}", executor.Name, sqlmock.AnyArg(), executor.ID).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			fn: "DeleteExecutor",
			inputs: []interface{}{
				executor.ID,
			},
			output: &ormResult{
				rowsAffected: 1,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `executors` WHERE id = ?")).
					WithArgs(executor.ID).WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			fn:     "QueryExecutors",
			inputs: []interface{}{},
			output: []*model.Executor{executor},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `executors`")).
					WillReturnRows(sqlmock.NewRows([]string{
						"seq_id", "created_at", "updated_at", "id", "name", "address", "labels",
					}).AddRow(1, createdAt, updatedAt, executor.ID, executor.Name,
						executor.Address, "{\"key1\":\"val1\",\"key2\":\"val2\"}"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
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
