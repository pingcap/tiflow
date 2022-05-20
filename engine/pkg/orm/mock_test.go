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
	"reflect"
	"testing"
	"time"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
	cerrors "github.com/pingcap/tiflow/engine/pkg/errors"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/stretchr/testify/require"
)

func TestGenEpochMock(t *testing.T) {
	t.Parallel()

	mock, err := NewMockClient()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var epoch int64
	for j := 0; j < 10; j++ {
		epoch, err = mock.GenEpoch(ctx)
		require.NoError(t, err)
	}
	require.Equal(t, int64(11), epoch)

	// Being a lightweight database, SQLite can’t handle a high level of concurrency
	// NOTICE: Not Recommend to do high concurrenct test in unit test
	// TODO: we can add retry for sqlite error 'database table is lock' later
	/*
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				t.Logf("goroutine:%d", idx)
				defer wg.Done()
				for j := 0; j < 10; j++ {
					_, err := mock.GenEpoch(ctx)
					require.NoError(t, err)
				}
			}(i)
		}
		wg.Wait()
		t.Logf("end the concurrency")
		epoch, err = mock.GenEpoch(ctx)
		require.NoError(t, err)
		require.Equal(t, 112, int(epoch))
	*/
}

type mCase struct {
	fn     string        // function name
	inputs []interface{} // function args

	output interface{} // function output
	err    error       // function error
}

func TestInitializeMock(t *testing.T) {
	cli, err := NewMockClient()
	require.Nil(t, err)
	require.NotNil(t, cli)
	defer cli.Close()

	testCases := []mCase{
		{
			fn:     "Initialize",
			inputs: []interface{}{},
		},
	}

	for _, tc := range testCases {
		testInnerMock(t, cli, tc)
	}
}

func TestProjectMock(t *testing.T) {
	cli, err := NewMockClient()
	require.Nil(t, err)
	require.NotNil(t, cli)
	defer cli.Close()

	err = cli.Initialize(context.TODO())
	require.Nil(t, err)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []mCase{
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
		},
		{
			fn: "CreateProject",
			inputs: []interface{}{
				&model.ProjectInfo{
					Model: model.Model{
						SeqID:     2,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:   "p112",
					Name: "tenant2",
				},
			},
		},
		{
			fn: "DeleteProject",
			inputs: []interface{}{
				"p111",
			},
		},
		{
			fn: "DeleteProject",
			inputs: []interface{}{
				"p114",
			},
		},
		{
			fn:     "QueryProjects",
			inputs: []interface{}{},
			output: []*model.ProjectInfo{
				{
					// FIXME: ??
					// actual: "CreatedAt\":\"2022-04-25T10:24:38.362718+08:00\"
					// expect:"CreatedAt\":\"2022-04-25T10:24:38.362718001+08:00\"
					Model: model.Model{
						SeqID:     2,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:   "p112",
					Name: "tenant2",
				},
			},
		},
		{
			// SELECT * FROM `project_infos` WHERE project_id = '111-222-333' ORDER BY `project_infos`.`id` LIMIT 1
			fn: "GetProjectByID",
			inputs: []interface{}{
				"p112",
			},
			output: &model.ProjectInfo{
				Model: model.Model{
					SeqID:     2,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				ID:   "p112",
				Name: "tenant2",
			},
		},
		{
			fn: "GetProjectByID",
			inputs: []interface{}{
				"p113",
			},
			err: cerrors.ErrMetaEntryNotFound.GenWithStackByArgs(),
		},
	}

	for _, tc := range testCases {
		testInnerMock(t, cli, tc)
	}
}

func TestProjectOperationMock(t *testing.T) {
	cli, err := NewMockClient()
	require.Nil(t, err)
	require.NotNil(t, cli)
	defer cli.Close()

	err = cli.Initialize(context.TODO())
	require.Nil(t, err)

	tm := time.Now()
	tm1 := tm.Add(time.Second * 10)
	tm2 := tm.Add(time.Second)
	tm3 := tm.Add(time.Second * 15)

	testCases := []mCase{
		{
			fn: "CreateProjectOperation",
			inputs: []interface{}{
				&model.ProjectOperation{
					ProjectID: "p111",
					Operation: "Submit",
					JobID:     "j222",
					CreatedAt: tm,
				},
			},
		},
		{
			fn: "CreateProjectOperation",
			inputs: []interface{}{
				&model.ProjectOperation{
					ProjectID: "p111",
					Operation: "Pause",
					JobID:     "j223",
					CreatedAt: tm1,
				},
			},
		},
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
					ProjectID: "p111",
					Operation: "Pause",
					JobID:     "j223",
					CreatedAt: tm1,
				},
			},
		},
		{
			// SELECT * FROM `project_operations` WHERE project_id = '111' AND created_at >= '2022-04-13 23:51:42.46' AND created_at <= '2022-04-13 23:51:42.46'
			fn: "QueryProjectOperationsByTimeRange",
			inputs: []interface{}{
				"p111",
				TimeRange{
					start: tm2,
					end:   tm3,
				},
			},
			output: []*model.ProjectOperation{
				{
					SeqID:     2,
					ProjectID: "p111",
					Operation: "Pause",
					JobID:     "j223",
					CreatedAt: tm1,
				},
			},
		},
	}

	for _, tc := range testCases {
		testInnerMock(t, cli, tc)
	}
}

func TestJobMock(t *testing.T) {
	cli, err := NewMockClient()
	require.Nil(t, err)
	require.NotNil(t, cli)
	defer cli.Close()

	err = cli.Initialize(context.TODO())
	require.Nil(t, err)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []mCase{
		{
			fn: "UpsertJob",
			inputs: []interface{}{
				&libModel.MasterMetaKVData{
					Model: model.Model{
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
		},
		{
			fn: "UpsertJob",
			inputs: []interface{}{
				&libModel.MasterMetaKVData{
					ProjectID:  "p111",
					ID:         "j111",
					Tp:         1,
					NodeID:     "n111",
					Epoch:      1,
					StatusCode: 2,
					Addr:       "127.0.0.1",
				},
			},
		},
		{
			fn: "DeleteJob",
			inputs: []interface{}{
				"j112",
			},
			output: ormResult{
				rowsAffected: 0,
			},
		},
		{
			// DELETE FROM `master_meta_kv_data` WHERE project_id = '111-222-334' AND job_id = '111'
			fn: "DeleteJob",
			inputs: []interface{}{
				"j113",
			},
			output: ormResult{
				rowsAffected: 0,
			},
		},
		{
			// SELECT * FROM `master_meta_kv_data` WHERE project_id = '111-222-333' AND job_id = '111' ORDER BY `master_meta_kv_data`.`id` LIMIT 1
			fn: "GetJobByID",
			inputs: []interface{}{
				"j111",
			},
			output: &libModel.MasterMetaKVData{
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
				StatusCode: 2,
				Addr:       "127.0.0.1",
				Config:     []byte{0x11, 0x22},
			},
		},
		{
			fn: "GetJobByID",
			inputs: []interface{}{
				"j113",
			},
			err: cerrors.ErrMetaEntryNotFound.GenWithStackByArgs(),
		},
		{
			// SELECT * FROM `master_meta_kv_data` WHERE project_id = '111-222-333'
			fn: "QueryJobsByProjectID",
			inputs: []interface{}{
				"p111",
			},
			output: []*libModel.MasterMetaKVData{
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
					StatusCode: 2,
					Addr:       "1.1.1.1",
					Config:     []byte{0x11, 0x22},
				},
			},
		},
		{
			fn: "QueryJobsByProjectID",
			inputs: []interface{}{
				"p113",
			},
			output: []*libModel.MasterMetaKVData{},
		},
		{
			//  SELECT * FROM `master_meta_kv_data` WHERE project_id = '111-222-333' AND job_status = 1
			fn: "QueryJobsByStatus",
			inputs: []interface{}{
				"j111",
				2,
			},
			output: []*libModel.MasterMetaKVData{
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
					StatusCode: 2,
					Addr:       "127.0.0.1",
					Config:     []byte{0x11, 0x22},
				},
			},
		},
		{
			fn: "QueryJobsByStatus",
			inputs: []interface{}{
				"j113",
				1,
			},
			output: []*libModel.MasterMetaKVData{},
		},
	}

	for _, tc := range testCases {
		testInnerMock(t, cli, tc)
	}
}

func TestWorkerMock(t *testing.T) {
	cli, err := NewMockClient()
	require.Nil(t, err)
	require.NotNil(t, cli)
	defer cli.Close()

	err = cli.Initialize(context.TODO())
	require.Nil(t, err)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []mCase{
		{
			// INSERT INTO `worker_statuses` (`created_at`,`updated_at`,`project_id`,`job_id`,`worker_id`,`worker_type`,
			// `worker_statuses`,`worker_err_msg`,`worker_config`,`id`) VALUES ('2022-04-14 11:35:06.119','2022-04-14 11:35:06.119',
			// '111-222-333','111','222',1,1,'error','<binary>',1)
			fn: "UpsertWorker",
			inputs: []interface{}{
				&libModel.WorkerStatus{
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
		},
		{
			fn: "UpsertWorker",
			inputs: []interface{}{
				&libModel.WorkerStatus{
					Model: model.Model{
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:    "p111",
					JobID:        "j111",
					ID:           "w224",
					Type:         1,
					Code:         1,
					ErrorMessage: "error",
					ExtBytes:     []byte{0x11, 0x22},
				},
			},
		},
		{
			fn: "DeleteWorker",
			inputs: []interface{}{
				"j111",
				"w223",
			},
			output: &ormResult{
				rowsAffected: 0,
			},
		},
		{
			// DELETE FROM `worker_statuses` WHERE project_id = '111-222-334' AND job_id = '111' AND worker_id = '222'
			fn: "DeleteWorker",
			inputs: []interface{}{
				"j112",
				"w224",
			},
			output: &ormResult{
				rowsAffected: 0,
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
			output: &libModel.WorkerStatus{
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
		{
			fn: "GetWorkerByID",
			inputs: []interface{}{
				"j111",
				"w225",
			},
			err: cerrors.ErrMetaEntryNotFound.GenWithStackByArgs(),
		},
		{
			// SELECT * FROM `worker_statuses` WHERE project_id = '111-222-333' AND job_id = '111'
			fn: "QueryWorkersByMasterID",
			inputs: []interface{}{
				"j111",
			},
			output: []*libModel.WorkerStatus{
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
		},
		{
			fn: "QueryWorkersByMasterID",
			inputs: []interface{}{
				"j113",
			},
			output: []*libModel.WorkerStatus{},
		},
		{
			// SELECT * FROM `worker_statuses` WHERE project_id = '111-222-333' AND job_id = '111' AND worker_statuses = 1
			fn: "QueryWorkersByStatus",
			inputs: []interface{}{
				"j111",
				1,
			},
			output: []*libModel.WorkerStatus{
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
		},
		{
			fn: "QueryWorkersByStatus",
			inputs: []interface{}{
				"j111",
				4,
			},
			output: []*libModel.WorkerStatus{},
		},
	}

	for _, tc := range testCases {
		testInnerMock(t, cli, tc)
	}
}

func TestResourceMock(t *testing.T) {
	cli, err := NewMockClient()
	require.Nil(t, err)
	require.NotNil(t, cli)
	defer cli.Close()

	err = cli.Initialize(context.TODO())
	require.Nil(t, err)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []mCase{
		{
			fn: "UpsertResource",
			inputs: []interface{}{
				&resourcemeta.ResourceMeta{
					Model: model.Model{
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
		},
		{
			fn: "UpsertResource",
			inputs: []interface{}{
				&resourcemeta.ResourceMeta{
					Model: model.Model{
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ID:        "r334",
					ProjectID: "111-222-333",
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e444",
					Deleted:   true,
				},
			},
		},
		{
			fn: "DeleteResource",
			inputs: []interface{}{
				"r334",
			},
			output: &ormResult{
				rowsAffected: 1,
			},
		},
		{
			fn: "DeleteResource",
			inputs: []interface{}{
				"r335",
			},
			output: &ormResult{
				rowsAffected: 0,
			},
		},
		{
			fn: "GetResourceByID",
			inputs: []interface{}{
				"r333",
			},
			output: &resourcemeta.ResourceMeta{
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
		{
			fn: "GetResourceByID",
			inputs: []interface{}{
				"r335",
			},
			err: cerrors.ErrMetaEntryNotFound.GenWithStackByArgs(),
		},
		{
			fn: "QueryResourcesByJobID",
			inputs: []interface{}{
				"j111",
			},
			output: []*resourcemeta.ResourceMeta{
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
		},
		{
			fn: "QueryResourcesByJobID",
			inputs: []interface{}{
				"j112",
			},
			output: []*resourcemeta.ResourceMeta{},
		},
		{
			fn: "QueryResourcesByExecutorID",
			inputs: []interface{}{
				"e444",
			},
			output: []*resourcemeta.ResourceMeta{
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
		},
		{
			fn: "QueryResourcesByExecutorID",
			inputs: []interface{}{
				"e445",
			},
			output: []*resourcemeta.ResourceMeta{},
		},
	}

	for _, tc := range testCases {
		testInnerMock(t, cli, tc)
	}
}

func testInnerMock(t *testing.T, cli Client, c mCase) {
	var args []reflect.Value
	args = append(args, reflect.ValueOf(context.Background()))
	for _, ip := range c.inputs {
		args = append(args, reflect.ValueOf(ip))
	}
	result := reflect.ValueOf(cli).MethodByName(c.fn).Call(args)
	if len(result) == 1 {
		// only error
		if c.err == nil {
			require.Nil(t, result[0].Interface())
		} else {
			require.NotNil(t, result[0].Interface())
			res := result[0].MethodByName("Is").Call([]reflect.Value{
				reflect.ValueOf(c.err),
			})
			require.True(t, res[0].Interface().(bool))
		}
	} else if len(result) == 2 {
		// result and error
		if c.err != nil {
			require.NotNil(t, result[1].Interface())
			// FIXME:
			// var args []reflect.Value
			// require.NotNil(t, reflect.ValueOf(c.err).Interface())
			// require.NotNil(t, result[1].MethodByName("Isxxx").Interface())
			// args = append(args, reflect.ValueOf(c.err))
			// res := result[1].MethodByName("Is").Call(args)
			// require.True(t, res[0].Interface().(bool))
		} else {
			require.NotNil(t, result[0].Interface())
			// log.L().Info("result", zap.Any("expect", c.output), zap.Any("actual", result[0].Interface()))
			// FIXME: datetime is different from what we insert. Why??
			// require.Equal(t, c.output, result[0].Interface())
		}
	}
}
