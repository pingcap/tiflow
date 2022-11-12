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
	"fmt"
	"reflect"
	"testing"
	"time"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	engineModel "github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/pkg/errors"
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
}

func TestProjectMock(t *testing.T) {
	cli, err := NewMockClient()
	require.Nil(t, err)
	require.NotNil(t, cli)
	defer cli.Close()

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
			err: errors.ErrMetaEntryNotFound.GenWithStackByArgs(),
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

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []mCase{
		{
			fn: "UpsertJob",
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
				},
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
					State:     2,
					Addr:      "127.0.0.1",
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
			// DELETE FROM `master_meta` WHERE project_id = '111-222-334' AND job_id = '111'
			fn: "DeleteJob",
			inputs: []interface{}{
				"j113",
			},
			output: ormResult{
				rowsAffected: 0,
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
				State:     2,
				Addr:      "127.0.0.1",
				Config:    []byte{0x11, 0x22},
			},
		},
		{
			fn: "GetJobByID",
			inputs: []interface{}{
				"j113",
			},
			err: errors.ErrMetaEntryNotFound.GenWithStackByArgs(),
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
					State:     2,
					Addr:      "1.1.1.1",
					Config:    []byte{0x11, 0x22},
				},
			},
		},
		{
			fn: "QueryJobsByProjectID",
			inputs: []interface{}{
				"p113",
			},
			output: []*frameModel.MasterMeta{},
		},
		{
			//  SELECT * FROM `master_meta` WHERE project_id = '111-222-333' AND job_status = 1
			fn: "QueryJobsByState",
			inputs: []interface{}{
				"j111",
				2,
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
					State:     2,
					Addr:      "127.0.0.1",
					Config:    []byte{0x11, 0x22},
				},
			},
		},
		{
			fn: "QueryJobsByState",
			inputs: []interface{}{
				"j113",
				1,
			},
			output: []*frameModel.MasterMeta{},
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
					ErrorMsg:  "error",
					ExtBytes:  []byte{0x11, 0x22},
				},
			},
		},
		{
			fn: "UpsertWorker",
			inputs: []interface{}{
				&frameModel.WorkerStatus{
					Model: model.Model{
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID: "p111",
					JobID:     "j111",
					ID:        "w224",
					Type:      1,
					State:     1,
					ErrorMsg:  "error",
					ExtBytes:  []byte{0x11, 0x22},
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
				ErrorMsg:  "error",
				ExtBytes:  []byte{0x11, 0x22},
			},
		},
		{
			fn: "GetWorkerByID",
			inputs: []interface{}{
				"j111",
				"w225",
			},
			err: errors.ErrMetaEntryNotFound.GenWithStackByArgs(),
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
					ErrorMsg:  "error",
					ExtBytes:  []byte{0x11, 0x22},
				},
			},
		},
		{
			fn: "QueryWorkersByMasterID",
			inputs: []interface{}{
				"j113",
			},
			output: []*frameModel.WorkerStatus{},
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
					ErrorMsg:  "error",
					ExtBytes:  []byte{0x11, 0x22},
				},
			},
		},
		{
			fn: "QueryWorkersByState",
			inputs: []interface{}{
				"j111",
				4,
			},
			output: []*frameModel.WorkerStatus{},
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

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []mCase{
		{
			fn: "UpsertResource",
			inputs: []interface{}{
				&resModel.ResourceMeta{
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
				&resModel.ResourceMeta{
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
				ResourceKey{
					JobID: "j111",
					ID:    "r334",
				},
			},
			output: &ormResult{
				rowsAffected: 1,
			},
		},
		{
			fn: "DeleteResource",
			inputs: []interface{}{
				ResourceKey{
					JobID: "j111",
					ID:    "r335",
				},
			},
			output: &ormResult{
				rowsAffected: 0,
			},
		},
		{
			fn: "GetResourceByID",
			inputs: []interface{}{
				ResourceKey{
					JobID: "j111",
					ID:    "r333",
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
		},
		{
			fn: "GetResourceByID",
			inputs: []interface{}{
				ResourceKey{
					JobID: "j111",
					ID:    "r335",
				},
			},
			err: errors.ErrMetaEntryNotFound.GenWithStackByArgs(),
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
		},
		{
			fn: "QueryResourcesByJobID",
			inputs: []interface{}{
				"j112",
			},
			output: []*resModel.ResourceMeta{},
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
					Job:       "j111",
					Worker:    "w222",
					Executor:  "e444",
					Deleted:   true,
				},
			},
		},
		{
			fn: "QueryResourcesByExecutorIDs",
			inputs: []interface{}{
				engineModel.ExecutorID("e444"),
			},
			output: []*resModel.ResourceMeta{},
		},
	}

	for _, tc := range testCases {
		fmt.Println("testing", tc.fn)
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
		} else {
			require.NotNil(t, result[0].Interface())
		}
	}
}
