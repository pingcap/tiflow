// Copyright 2021 PingCAP, Inc.
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

// this file implement all of the APIs of the DataMigration service.

package master

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/deepmap/oapi-codegen/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/tempurl"

	"github.com/pingcap/tiflow/dm/checker"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/pbmock"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/openapi/fixtures"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// some data for test.
var (
	source1Name = "mysql-replica-01"
)

// func (t *openAPISuite) TestTaskAPI(c *check.C) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	s := setupTestServer(ctx, s.T())
// 	s.Equal(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB", `return(true)`), check.IsNil)
// 	s.Equal(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/MockSkipRemoveMetaData", `return(true)`), check.IsNil)
// 	checker.CheckSyncConfigFunc = mockCheckSyncConfig
// 	ctrl := gomock.NewController(c)
// 	defer func() {
// 		checker.CheckSyncConfigFunc = checker.CheckSyncConfig
// 		cancel()
// 		s.Close()
// 		ctrl.Finish()
// 		s.Equal(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB"), check.IsNil)
// 		s.Equal(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/MockSkipRemoveMetaData"), check.IsNil)
// 	}()

// 	dbCfg := config.GetDBConfigForTest()
// 	source1 := openapi.Source{
// 		Enable:     true,
// 		SourceName: source1Name,
// 		EnableGtid: false,
// 		Host:       dbCfg.Host,
// 		Password:   dbCfg.Password,
// 		Port:       dbCfg.Port,
// 		User:       dbCfg.User,
// 	}
// 	// create source
// 	sourceURL := "/api/v1/sources"
// 	createSourceReq := openapi.CreateSourceRequest{Source: source1}
// 	result := testutil.NewRequest().Post(sourceURL).WithJsonBody(createSourceReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	// check http status code
// 	s.Equal(http.StatusCreated,result.Code())

// 	// add mock worker  start workers, the unbounded sources should be bounded
// 	ctx1, cancel1 := context.WithCancel(ctx)
// 	defer cancel1()
// 	workerName1 := "worker-1"
// s.NoError(s1.scheduler.AddWorker(workerName1, "172.16.10.72:8262"))
// 	go func(ctx context.Context, workerName string) {
// 		s.Equal(ha.KeepAlive(ctx, s.etcdClient, workerName, keepAliveTTL), check.IsNil)
// 	}(ctx1, workerName1)
// 	// wait worker ready
// 	s.Equal(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
// 		w := s1.scheduler.GetWorkerBySource(source1.SourceName)
// 		return w != nil
// 	}), check.IsTrue)

// 	// create task
// 	taskURL := "/api/v1/tasks"

// 	task, err := fixtures.GenNoShardOpenAPITaskForTest()
// 	s.NoError(err)
// 	// use a valid target db
// 	task.TargetConfig.Host = dbCfg.Host
// 	task.TargetConfig.Port = dbCfg.Port
// 	task.TargetConfig.User = dbCfg.User
// 	task.TargetConfig.Password = dbCfg.Password

// 	// create task
// 	createTaskReq := openapi.CreateTaskRequest{Task: task}
// 	result = testutil.NewRequest().Post(taskURL).WithJsonBody(createTaskReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusCreated,result.Code())
// 	var createTaskResp openapi.Task
// 	err = result.UnmarshalBodyToObject(&createTaskResp)
// 	s.NoError(err)
// 	s.Equal(task.Name, check.Equals, createTaskResp.Name)
// 	subTaskM := s1.scheduler.GetSubTaskCfgsByTask(task.Name)
// 	s.Equal(len(subTaskM) == 1, check.IsTrue)
// 	s.Equal(subTaskM[source1Name].Name, check.Equals, task.Name)

// 	// get task
// 	task1URL := fmt.Sprintf("%s/%s", taskURL, task.Name)
// 	var task1FromHTTP openapi.Task
// 	result = testutil.NewRequest().Get(task1URL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	s.Equal(result.UnmarshalBodyToObject(&task1FromHTTP), check.IsNil)
// 	s.Equal(task.Name, check.Equals, task1FromHTTP.Name)

// 	// update a task
// 	s.Equal(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/scheduler/operateCheckSubtasksCanUpdate", `return("success")`), check.IsNil)
// 	clone := task
// 	batch := 1000
// 	clone.SourceConfig.IncrMigrateConf.ReplBatch = &batch
// 	updateReq := openapi.UpdateTaskRequest{Task: clone}
// 	result = testutil.NewRequest().Put(task1URL).WithJsonBody(updateReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	s.Equal(result.UnmarshalBodyToObject(&task1FromHTTP), check.IsNil)
// 	s.Equal(clone.SourceConfig.IncrMigrateConf.ReplBatch, check.DeepEquals, task1FromHTTP.SourceConfig.IncrMigrateConf.ReplBatch)
// 	s.Equal(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/operateCheckSubtasksCanUpdate"), check.IsNil)

// 	// list tasks
// 	result = testutil.NewRequest().Get(taskURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	var resultTaskList openapi.GetTaskListResponse
// 	err = result.UnmarshalBodyToObject(&resultTaskList)
// 	s.NoError(err)
// 	s.Equal(resultTaskList.Total, check.Equals, 1)
// 	s.Equal(resultTaskList.Data[0].Name, check.Equals, task.Name)

// 	t.testImportTaskTemplate(c, &task, s)

// 	// start task
// 	startTaskURL := fmt.Sprintf("%s/%s/start", taskURL, task.Name)
// 	result = testutil.NewRequest().Post(startTaskURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	s.Equal(s1.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect, check.Equals, pb.Stage_Running)

// 	// get task status
// 	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
// 	mockTaskQueryStatus(mockWorkerClient, task.Name, source1.SourceName, workerName1, false)
// 	s1.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
// 	taskStatusURL := fmt.Sprintf("%s/%s/status", taskURL, task.Name)
// 	result = testutil.NewRequest().Get(taskStatusURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	var resultTaskStatus openapi.GetTaskStatusResponse
// 	err = result.UnmarshalBodyToObject(&resultTaskStatus)
// 	s.NoError(err)
// 	s.Equal(resultTaskStatus.Total, check.Equals, 1) // only 1 subtask
// 	s.Equal(resultTaskStatus.Data[0].Name, check.Equals, task.Name)
// 	s.Equal(resultTaskStatus.Data[0].Stage, check.Equals, openapi.TaskStageRunning)
// 	s.Equal(resultTaskStatus.Data[0].WorkerName, check.Equals, workerName1)
// 	s.Equal(resultTaskStatus.Data[0].DumpStatus.CompletedTables, check.Equals, float64(0))
// 	s.Equal(resultTaskStatus.Data[0].DumpStatus.TotalTables, check.Equals, int64(1))
// 	s.Equal(resultTaskStatus.Data[0].DumpStatus.EstimateTotalRows, check.Equals, float64(10))

// 	// get task status with source name
// 	taskStatusURL = fmt.Sprintf("%s/%s/status?source_name_list=%s", taskURL, task.Name, source1Name)
// 	result = testutil.NewRequest().Get(taskStatusURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	var resultTaskStatusWithStatus openapi.GetTaskStatusResponse
// 	err = result.UnmarshalBodyToObject(&resultTaskStatusWithStatus)
// 	s.NoError(err)
// 	s.Equal(resultTaskStatusWithStatus, check.DeepEquals, resultTaskStatus)

// 	// list task with status
// 	result = testutil.NewRequest().Get(taskURL+"?with_status=true").GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	var resultListTask openapi.GetTaskListResponse
// 	err = result.UnmarshalBodyToObject(&resultListTask)
// 	s.NoError(err)
// 	s.Equal(resultListTask.Data, check.HasLen, 1)
// 	s.Equal(resultListTask.Total, check.Equals, 1)
// 	s.Equal(resultListTask.Data[0].StatusList, check.NotNil)
// 	statusList := *resultListTask.Data[0].StatusList
// 	status := statusList[0]
// 	s.Equal(status.WorkerName, check.Equals, workerName1)
// 	s.Equal(status.Name, check.Equals, task.Name)

// 	// list with filter
// 	result = testutil.NewRequest().Get(taskURL+"?stage=Stopped").GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	resultListTask = openapi.GetTaskListResponse{} // reset
// 	err = result.UnmarshalBodyToObject(&resultListTask)
// 	s.NoError(err)
// 	s.Equal(resultListTask.Data, check.HasLen, 0)

// 	result = testutil.NewRequest().Get(taskURL+"?stage=Running").GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	resultListTask = openapi.GetTaskListResponse{} // reset
// 	err = result.UnmarshalBodyToObject(&resultListTask)
// 	s.NoError(err)
// 	s.Equal(resultListTask.Data, check.HasLen, 1)

// 	result = testutil.NewRequest().Get(taskURL+"?stage=Running&source_name_list=notsource").GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	resultListTask = openapi.GetTaskListResponse{} // reset
// 	err = result.UnmarshalBodyToObject(&resultListTask)
// 	s.NoError(err)
// 	s.Equal(resultListTask.Data, check.HasLen, 0)

// 	result = testutil.NewRequest().Get(taskURL+"?stage=Running&source_name_list="+source1Name).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	resultListTask = openapi.GetTaskListResponse{} // reset
// 	err = result.UnmarshalBodyToObject(&resultListTask)
// 	s.NoError(err)
// 	s.Equal(resultListTask.Data, check.HasLen, 1)

// 	// get task with status
// 	result = testutil.NewRequest().Get(task1URL+"?with_status=true").GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	s.Equal(result.UnmarshalBodyToObject(&task1FromHTTP), check.IsNil)
// 	s.Equal(task.Name, check.Equals, task1FromHTTP.Name)
// 	statusList = *task1FromHTTP.StatusList
// 	s.Equal(statusList, check.HasLen, 1)
// 	s.Equal(statusList[0].WorkerName, check.Equals, workerName1)
// 	s.Equal(statusList[0].Name, check.Equals, task.Name)

// 	// test some error happened on worker
// 	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
// 	mockTaskQueryStatus(mockWorkerClient, task.Name, source1.SourceName, workerName1, true)
// 	s1.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
// 	result = testutil.NewRequest().Get(taskURL+"?with_status=true").GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	s.Equal(result.UnmarshalBodyToObject(&resultListTask), check.IsNil)
// 	s.Equal(resultListTask.Data, check.HasLen, 1)
// 	s.Equal(resultListTask.Total, check.Equals, 1)
// 	s.Equal(resultListTask.Data[0].StatusList, check.NotNil)
// 	statusList = *resultListTask.Data[0].StatusList
// 	s.Equal(statusList, check.HasLen, 1)
// 	status = statusList[0]
// 	s.Equal(status.ErrorMsg, check.NotNil)

// 	// test convertTaskConfig
// 	convertReq := openapi.ConverterTaskRequest{}
// 	convertResp := openapi.ConverterTaskResponse{}
// 	convertURL := fmt.Sprintf("%s/%s", taskURL, "converters")
// 	result = testutil.NewRequest().Post(convertURL).WithJsonBody(convertReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusBadRequest,result.Code()) // not valid req

// 	// from task to taskConfig
// 	convertReq.Task = &task
// 	result = testutil.NewRequest().Post(convertURL).WithJsonBody(convertReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	err = result.UnmarshalBodyToObject(&convertResp)
// 	s.NoError(err)
// 	s.Equal(convertResp.Task, check.NotNil)
// 	s.Equal(convertResp.TaskConfigFile, check.NotNil)
// 	taskConfigFile := convertResp.TaskConfigFile

// 	// from taskCfg to task
// 	convertReq.Task = nil
// 	convertReq.TaskConfigFile = &taskConfigFile
// 	result = testutil.NewRequest().Post(convertURL).WithJsonBody(convertReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	err = result.UnmarshalBodyToObject(&convertResp)
// 	s.NoError(err)
// 	s.Equal(convertResp.Task, check.NotNil)
// 	s.Equal(convertResp.TaskConfigFile, check.NotNil)
// 	taskConfigFile2 := convertResp.TaskConfigFile
// 	s.Equal(taskConfigFile, check.Equals, taskConfigFile2)

// 	t.testSourceOperationWithTask(c, &source1, &task, s)

// 	// stop task
// 	stopTaskURL := fmt.Sprintf("%s/%s/stop", taskURL, task.Name)
// 	stopTaskReq := openapi.StopTaskRequest{}
// 	result = testutil.NewRequest().Post(stopTaskURL).WithJsonBody(stopTaskReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	s.Equal(s1.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect, check.Equals, pb.Stage_Stopped)

// 	// delete task
// 	result = testutil.NewRequest().Delete(task1URL+"?force=true").GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusNoContent,result.Code())
// 	subTaskM = s1.scheduler.GetSubTaskCfgsByTask(task.Name)
// 	s.Equal(len(subTaskM) == 0, check.IsTrue)

// 	// list tasks
// 	result = testutil.NewRequest().Get(taskURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK,result.Code())
// 	resultListTask = openapi.GetTaskListResponse{} // reset
// 	err = result.UnmarshalBodyToObject(&resultTaskList)
// 	s.NoError(err)
// 	s.Equal(resultTaskList.Total, check.Equals, 0)
// }

// func (t *openAPISuite) testImportTaskTemplate(c *check.C, task *openapi.Task, s *Server) {
// 	// test batch import task config
// 	taskBatchImportURL := "/api/v1/tasks/templates/import"
// 	req := openapi.TaskTemplateRequest{Overwrite: false}
// 	result := testutil.NewRequest().Post(taskBatchImportURL).WithJsonBody(req).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(result.Code(), check.Equals, http.StatusAccepted)
// 	var resp openapi.TaskTemplateResponse
// 	s.Equal(result.UnmarshalBodyToObject(&resp), check.IsNil)
// 	s.Equal(resp.SuccessTaskList, check.HasLen, 1)
// 	s.Equal(resp.SuccessTaskList[0], check.Equals, task.Name)
// 	s.Equal(resp.FailedTaskList, check.HasLen, 0)

// 	// import again without overwrite will fail
// 	result = testutil.NewRequest().Post(taskBatchImportURL).WithJsonBody(req).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(result.Code(), check.Equals, http.StatusAccepted)
// 	s.Equal(result.UnmarshalBodyToObject(&resp), check.IsNil)
// 	s.Equal(resp.SuccessTaskList, check.HasLen, 0)
// 	s.Equal(resp.FailedTaskList, check.HasLen, 1)
// 	s.Equal(resp.FailedTaskList[0].TaskName, check.Equals, task.Name)

// 	// import again with overwrite will success
// 	req.Overwrite = true
// 	result = testutil.NewRequest().Post(taskBatchImportURL).WithJsonBody(req).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(result.UnmarshalBodyToObject(&resp), check.IsNil)
// 	s.Equal(resp.SuccessTaskList, check.HasLen, 1)
// 	s.Equal(resp.SuccessTaskList[0], check.Equals, task.Name)
// 	s.Equal(resp.FailedTaskList, check.HasLen, 0)
// }

// func (t *openAPISuite) testSourceOperationWithTask(c *check.C, source *openapi.Source, task *openapi.Task, s *Server) {
// 	source1URL := fmt.Sprintf("/api/v1/sources/%s", source.SourceName)
// 	disableSource1URL := fmt.Sprintf("%s/disable", source1URL)
// 	enableSource1URL := fmt.Sprintf("%s/enable", source1URL)
// 	transferSource1URL := fmt.Sprintf("%s/transfer", source1URL)

// 	// disable
// 	result := testutil.NewRequest().Post(disableSource1URL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK, result.Code())
// 	s.Equal(s1.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect, check.Equals, pb.Stage_Stopped)

// 	// enable again
// 	result = testutil.NewRequest().Post(enableSource1URL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusOK, result.Code())
// 	s.Equal(s1.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect, check.Equals, pb.Stage_Running)

// 	// test transfer failed,success transfer is tested in IT test
// 	req := openapi.WorkerNameRequest{WorkerName: "not exist"}
// 	result = testutil.NewRequest().Post(transferSource1URL).WithJsonBody(req).GoWithHTTPHandler(s.T(), s1.openapiHandles)
// 	s.Equal(http.StatusBadRequest, result.Code())
// 	var resp openapi.ErrorWithMessage
// 	s.Equal(result.UnmarshalBodyToObject(&resp), check.IsNil)
// 	s.Equal(resp.ErrorCode, check.Equals, int(terror.ErrSchedulerWorkerNotExist.Code()))
// }

func setupTestServer(ctx context.Context, t *testing.T) *Server {
	t.Helper()
	// create a new cluster
	cfg1 := NewConfig()
	require.NoError(t, cfg1.FromContent(SampleConfig))
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = t.TempDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.AdvertiseAddr = cfg1.MasterAddr
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)
	cfg1.OpenAPI = true

	s1 := NewServer(cfg1)
	require.NoError(t, s1.Start(ctx))
	// wait the first one become the leader
	require.True(t, utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s1.election.IsLeader() && s1.scheduler.Started()
	}))
	return s1
}

// nolint:unparam
func mockRelayQueryStatus(
	mockWorkerClient *pbmock.MockWorkerClient, sourceName, workerName string, stage pb.Stage,
) {
	queryResp := &pb.QueryStatusResponse{
		Result: true,
		SourceStatus: &pb.SourceStatus{
			Worker: workerName,
			Source: sourceName,
		},
	}
	if stage == pb.Stage_Running {
		queryResp.SourceStatus.RelayStatus = &pb.RelayStatus{Stage: stage}
	}
	if stage == pb.Stage_Paused {
		queryResp.Result = false
		queryResp.Msg = "some error happened"
	}
	mockWorkerClient.EXPECT().QueryStatus(
		gomock.Any(),
		&pb.QueryStatusRequest{Name: ""},
	).Return(queryResp, nil).MaxTimes(maxRetryNum)
}

// nolint:unparam
func mockPurgeRelay(mockWorkerClient *pbmock.MockWorkerClient) {
	resp := &pb.CommonWorkerResponse{Result: true}
	mockWorkerClient.EXPECT().PurgeRelay(gomock.Any(), gomock.Any()).Return(resp, nil).MaxTimes(maxRetryNum)
}

func mockTaskQueryStatus(
	mockWorkerClient *pbmock.MockWorkerClient, taskName, sourceName, workerName string, needError bool,
) {
	var queryResp *pb.QueryStatusResponse
	if needError {
		queryResp = &pb.QueryStatusResponse{
			Result: false,
			Msg:    "some error happened",
			SourceStatus: &pb.SourceStatus{
				Worker: workerName,
				Source: sourceName,
			},
		}
	} else {
		queryResp = &pb.QueryStatusResponse{
			Result: true,
			SourceStatus: &pb.SourceStatus{
				Worker: workerName,
				Source: sourceName,
			},
			SubTaskStatus: []*pb.SubTaskStatus{
				{
					Stage: pb.Stage_Running,
					Name:  taskName,
					Status: &pb.SubTaskStatus_Dump{
						Dump: &pb.DumpStatus{
							CompletedTables:   0.0,
							EstimateTotalRows: 10.0,
							FinishedBytes:     0.0,
							FinishedRows:      5.0,
							TotalTables:       1,
						},
					},
				},
			},
		}
	}

	mockWorkerClient.EXPECT().QueryStatus(
		gomock.Any(),
		gomock.Any(),
	).Return(queryResp, nil).MaxTimes(maxRetryNum)
}

func mockCheckSyncConfig(ctx context.Context, cfgs []*config.SubTaskConfig, errCnt, warnCnt int64) (string, error) {
	return "", nil
}

type OpenAPIViewSuite struct {
	suite.Suite
}

func (s *OpenAPIViewSuite) SetupSuite() {
	s.NoError(log.InitLogger(&log.Config{}))
	checker.CheckSyncConfigFunc = mockCheckSyncConfig
	checkAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
	s.NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB", `return(true)`))
}

func (s *OpenAPIViewSuite) TearDownSuite() {
	checker.CheckSyncConfigFunc = checker.CheckSyncConfig
	checkAndAdjustSourceConfigFunc = checkAndAdjustSourceConfig
	s.NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB"), check.IsNil)
}

func (s *OpenAPIViewSuite) TestClusterAPI() {
	ctx1, cancel1 := context.WithCancel(context.Background())
	s1 := setupTestServer(ctx1, s.T())
	defer func() {
		cancel1()
		s1.Close()
	}()

	// join a new master node to an existing cluster
	cfg2 := NewConfig()
	s.Nil(cfg2.FromContent(SampleConfig))
	cfg2.Name = "dm-master-2"
	cfg2.DataDir = s.T().TempDir()
	cfg2.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg2.PeerUrls = tempurl.Alloc()
	cfg2.AdvertisePeerUrls = cfg2.PeerUrls
	cfg2.Join = s1.cfg.MasterAddr // join to an existing cluster
	cfg2.AdvertiseAddr = cfg2.MasterAddr
	s2 := NewServer(cfg2)
	ctx2, cancel2 := context.WithCancel(context.Background())
	require.NoError(s.T(), s2.Start(ctx2))

	defer func() {
		cancel2()
		s2.Close()
	}()

	baseURL := "/api/v1/cluster/"
	masterURL := baseURL + "masters"

	result := testutil.NewRequest().Get(masterURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var resultMasters openapi.GetClusterMasterListResponse
	err := result.UnmarshalBodyToObject(&resultMasters)
	s.NoError(err)
	s.Equal(2, resultMasters.Total)
	s.Equal(s1.cfg.Name, resultMasters.Data[0].Name)
	s.Equal(s1.cfg.PeerUrls, resultMasters.Data[0].Addr)
	s.True(resultMasters.Data[0].Leader)
	s.True(resultMasters.Data[0].Alive)

	// check cluster id
	clusterInfoURL := baseURL + "info"
	result = testutil.NewRequest().Get(clusterInfoURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var info openapi.GetClusterInfoResponse
	s.NoError(result.UnmarshalBodyToObject(&info))
	s.Greater(info.ClusterId, uint64(0))
	s.Nil(info.Topology)

	// update topo info
	fakeHost := "1.1.1.1"
	fakePort := 8261
	masterTopo := []openapi.MasterTopology{{Host: fakeHost, Port: fakePort}}
	workerTopo := []openapi.WorkerTopology{{Host: fakeHost, Port: fakePort}}
	grafanaTopo := openapi.GrafanaTopology{Host: fakeHost, Port: fakePort}
	prometheusTopo := openapi.PrometheusTopology{Host: fakeHost, Port: fakePort}
	alertMangerTopo := openapi.AlertManagerTopology{Host: fakeHost, Port: fakePort}
	topo := openapi.ClusterTopology{
		MasterTopologyList:   &masterTopo,
		WorkerTopologyList:   &workerTopo,
		GrafanaTopology:      &grafanaTopo,
		AlertManagerTopology: &alertMangerTopo,
		PrometheusTopology:   &prometheusTopo,
	}
	result = testutil.NewRequest().Put(clusterInfoURL).WithJsonBody(topo).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	info = openapi.GetClusterInfoResponse{}
	s.NoError(result.UnmarshalBodyToObject(&info))
	s.EqualValues(&topo, info.Topology)
	// get again
	result = testutil.NewRequest().Get(clusterInfoURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&info))
	s.EqualValues(&topo, info.Topology)

	// offline master-2 with retry
	// operate etcd cluster may met `etcdserver: unhealthy cluster`, add some retry
	for i := 0; i < 20; i++ {
		result = testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", masterURL, s2.cfg.Name)).GoWithHTTPHandler(s.T(), s1.openapiHandles)
		if result.Code() == http.StatusBadRequest {
			s.Equal(http.StatusBadRequest, result.Code())
			errResp := &openapi.ErrorWithMessage{}
			err = result.UnmarshalBodyToObject(errResp)
			s.Nil(err)
			s.Regexp("etcdserver: unhealthy cluster", errResp.ErrorMsg)
			time.Sleep(time.Second)
		} else {
			s.Equal(http.StatusNoContent, result.Code())
			break
		}
	}
	cancel2() // stop dm-master-2

	// list master again get one node
	result = testutil.NewRequest().Get(masterURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&resultMasters))
	s.Equal(1, resultMasters.Total)

	workerName1 := "worker1"
	s.NoError(s1.scheduler.AddWorker(workerName1, "172.16.10.72:8262"))
	// list worker node
	workerURL := baseURL + "workers"
	result = testutil.NewRequest().Get(workerURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	var resultWorkers openapi.GetClusterWorkerListResponse
	s.NoError(result.UnmarshalBodyToObject(&resultWorkers))
	s.Equal(1, resultWorkers.Total)

	// offline worker-1
	result = testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", workerURL, workerName1)).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusNoContent, result.Code())
	// after offline, no worker node
	result = testutil.NewRequest().Get(workerURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	err = result.UnmarshalBodyToObject(&resultWorkers)
	s.Nil(err)
	s.Equal(0, resultWorkers.Total)

	cancel1()
}

func (s *OpenAPIViewSuite) TestRedirectRequestToLeader() {
	ctx1, cancel1 := context.WithCancel(context.Background())
	s1 := setupTestServer(ctx1, s.T())
	defer func() {
		cancel1()
		s1.Close()
	}()

	// join a new master node to an existing cluster
	cfg2 := NewConfig()
	s.Nil(cfg2.FromContent(SampleConfig))
	cfg2.Name = "dm-master-2"
	cfg2.DataDir = s.T().TempDir()
	cfg2.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg2.PeerUrls = tempurl.Alloc()
	cfg2.AdvertisePeerUrls = cfg2.PeerUrls
	cfg2.Join = s1.cfg.MasterAddr // join to an existing cluster
	cfg2.AdvertiseAddr = cfg2.MasterAddr
	cfg2.OpenAPI = true
	s2 := NewServer(cfg2)
	ctx2, cancel2 := context.WithCancel(context.Background())
	require.NoError(s.T(), s2.Start(ctx2))

	defer func() {
		cancel2()
		s2.Close()
	}()

	// wait the second master ready
	s.False(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s2.election.IsLeader()
	}))

	baseURL := "/api/v1/sources"
	// list source from leader
	result := testutil.NewRequest().Get(baseURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var resultListSource openapi.GetSourceListResponse
	s.NoError(result.UnmarshalBodyToObject(&resultListSource))
	s.Len(resultListSource.Data, 0)
	s.Equal(0, resultListSource.Total)

	// list source not from leader will get a redirect
	result = testutil.NewRequest().Get(baseURL).GoWithHTTPHandler(s.T(), s2.openapiHandles)
	s.Equal(http.StatusTemporaryRedirect, result.Code())
}

func (s *OpenAPIViewSuite) TestOpenAPIWillNotStartInDefaultConfig() {
	// create a new cluster
	cfg1 := NewConfig()
	s.NoError(cfg1.FromContent(SampleConfig))
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = s.T().TempDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.AdvertiseAddr = cfg1.MasterAddr
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 := NewServer(cfg1)
	ctx, cancel := context.WithCancel(context.Background())
	s.NoError(s1.Start(ctx))
	s.Nil(s1.openapiHandles)
	cancel()
	s1.Close()
}

func (s *OpenAPIViewSuite) TestTaskTemplatesAPI() {
	ctx, cancel := context.WithCancel(context.Background())
	s1 := setupTestServer(ctx, s.T())
	defer func() {
		cancel()
		s1.Close()
	}()

	dbCfg := config.GetDBConfigForTest()
	source1 := openapi.Source{
		SourceName: source1Name,
		EnableGtid: false,
		Host:       dbCfg.Host,
		Password:   dbCfg.Password,
		Port:       dbCfg.Port,
		User:       dbCfg.User,
	}
	createReq := openapi.CreateSourceRequest{Source: source1}
	// create source
	sourceURL := "/api/v1/sources"
	result := testutil.NewRequest().Post(sourceURL).WithJsonBody(createReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	// check http status code
	s.Equal(http.StatusCreated, result.Code())

	// create task config template
	url := "/api/v1/tasks/templates"

	task, err := fixtures.GenNoShardOpenAPITaskForTest()
	s.NoError(err)
	// use a valid target db
	task.TargetConfig.Host = dbCfg.Host
	task.TargetConfig.Port = dbCfg.Port
	task.TargetConfig.User = dbCfg.User
	task.TargetConfig.Password = dbCfg.Password

	// create one
	result = testutil.NewRequest().Post(url).WithJsonBody(task).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusCreated, result.Code())
	var createTaskResp openapi.Task
	s.NoError(result.UnmarshalBodyToObject(&createTaskResp))
	s.Equal(createTaskResp.Name, task.Name)

	// create again will fail
	result = testutil.NewRequest().Post(url).WithJsonBody(task).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusBadRequest, result.Code())
	var errResp openapi.ErrorWithMessage
	s.NoError(result.UnmarshalBodyToObject(&errResp))
	s.Equal(int(terror.ErrOpenAPITaskConfigExist.Code()), errResp.ErrorCode)

	// list templates
	result = testutil.NewRequest().Get(url).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var resultTaskList openapi.GetTaskListResponse
	s.NoError(result.UnmarshalBodyToObject(&resultTaskList))
	s.Equal(1, resultTaskList.Total)
	s.Equal(task.Name, resultTaskList.Data[0].Name)

	// get detail
	oneURL := fmt.Sprintf("%s/%s", url, task.Name)
	result = testutil.NewRequest().Get(oneURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var respTask openapi.Task
	s.NoError(result.UnmarshalBodyToObject(&respTask))
	s.Equal(task.Name, respTask.Name)

	// get not exist
	notExistURL := fmt.Sprintf("%s/%s", url, "notexist")
	result = testutil.NewRequest().Get(notExistURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusBadRequest, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&errResp))
	s.Equal(int(terror.ErrOpenAPITaskConfigNotExist.Code()), errResp.ErrorCode)

	// update
	task.TaskMode = openapi.TaskTaskModeAll
	result = testutil.NewRequest().Put(oneURL).WithJsonBody(task).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&respTask))
	s.Equal(task.Name, respTask.Name)

	// update not exist will fail
	task.Name = "notexist"
	result = testutil.NewRequest().Put(notExistURL).WithJsonBody(task).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusBadRequest, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&errResp))
	s.Equal(int(terror.ErrOpenAPITaskConfigNotExist.Code()), errResp.ErrorCode)

	// delete task config template
	result = testutil.NewRequest().Delete(oneURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusNoContent, result.Code())
	result = testutil.NewRequest().Get(url).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&resultTaskList))
	s.Equal(0, resultTaskList.Total)
}

func (s *OpenAPIViewSuite) TestSourceAPI() {
	ctx, cancel := context.WithCancel(context.Background())
	s1 := setupTestServer(ctx, s.T())
	defer func() {
		cancel()
		s1.Close()
	}()

	baseURL := "/api/v1/sources"

	dbCfg := config.GetDBConfigForTest()
	purgeInterVal := int64(10)
	source1 := openapi.Source{
		SourceName: source1Name,
		Enable:     true,
		EnableGtid: false,
		Host:       dbCfg.Host,
		Password:   dbCfg.Password,
		Port:       dbCfg.Port,
		User:       dbCfg.User,
		Purge:      &openapi.Purge{Interval: &purgeInterVal},
	}
	createReq := openapi.CreateSourceRequest{Source: source1}
	result := testutil.NewRequest().Post(baseURL).WithJsonBody(createReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	// check http status code
	s.Equal(http.StatusCreated, result.Code())
	var resultSource openapi.Source
	s.NoError(result.UnmarshalBodyToObject(&resultSource))
	s.Equal(source1.User, resultSource.User)
	s.Equal(source1.Host, resultSource.Host)
	s.Equal(source1.Port, resultSource.Port)
	s.Equal(source1.Password, resultSource.Password)
	s.Equal(source1.EnableGtid, resultSource.EnableGtid)
	s.Equal(source1.SourceName, resultSource.SourceName)
	s.EqualValues(source1.Purge.Interval, resultSource.Purge.Interval)

	// create source with same name will failed
	result = testutil.NewRequest().Post(baseURL).WithJsonBody(createReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	// check http status code
	s.Equal(http.StatusBadRequest, result.Code())
	var errResp openapi.ErrorWithMessage
	s.NoError(result.UnmarshalBodyToObject(&errResp))
	s.Equal(int(terror.ErrSchedulerSourceCfgExist.Code()), errResp.ErrorCode)

	// get source
	source1URL := fmt.Sprintf("%s/%s", baseURL, source1Name)
	var source1FromHTTP openapi.Source
	result = testutil.NewRequest().Get(source1URL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&source1FromHTTP))
	s.Equal(source1FromHTTP.SourceName, source1.SourceName)
	// update a source
	clone := source1
	clone.EnableGtid = true
	updateReq := openapi.UpdateSourceRequest{Source: clone}
	result = testutil.NewRequest().Put(source1URL).WithJsonBody(updateReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&source1FromHTTP))
	s.Equal(source1FromHTTP.EnableGtid, clone.EnableGtid)

	// get source not existed
	sourceNotExistedURL := fmt.Sprintf("%s/not_existed", baseURL)
	result = testutil.NewRequest().Get(sourceNotExistedURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusNotFound, result.Code())
	// get source status
	var source1Status openapi.GetSourceStatusResponse
	source1StatusURL := fmt.Sprintf("%s/%s/status", baseURL, source1Name)
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&source1Status))
	s.Len(source1Status.Data, 1)
	s.Equal(source1.SourceName, source1Status.Data[0].SourceName)
	s.Equal("", source1Status.Data[0].WorkerName) // no worker now

	// list source
	result = testutil.NewRequest().Get(baseURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	// check http status code
	s.Equal(http.StatusOK, result.Code())
	var resultListSource openapi.GetSourceListResponse
	s.NoError(result.UnmarshalBodyToObject(&resultListSource))
	s.Len(resultListSource.Data, 1)
	s.Equal(1, resultListSource.Total)
	s.Equal(source1.SourceName, resultListSource.Data[0].SourceName)

	// test get source schema and table
	_, mockDB, err := conn.InitMockDBFull()
	s.NoError(err)
	schemaName := "information_schema"
	mockDB.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"Database"}).AddRow(schemaName))

	schemaURL := fmt.Sprintf("%s/%s/schemas", baseURL, source1.SourceName)
	result = testutil.NewRequest().Get(schemaURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var schemaNameList openapi.SchemaNameList
	s.NoError(result.UnmarshalBodyToObject(&schemaNameList))
	s.Len(schemaNameList, 1)
	s.Equal(schemaName, schemaNameList[0])
	s.NoError(mockDB.ExpectationsWereMet())

	_, mockDB, err = conn.InitMockDBFull()
	s.NoError(err)
	tableName := "CHARACTER_SETS"
	mockDB.ExpectQuery("SHOW TABLES FROM " + schemaName).WillReturnRows(sqlmock.NewRows([]string{"Tables_in_information_schema"}).AddRow(tableName))
	tableURL := fmt.Sprintf("%s/%s/schemas/%s", baseURL, source1.SourceName, schemaName)
	result = testutil.NewRequest().Get(tableURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var tableNameList openapi.TableNameList
	s.NoError(result.UnmarshalBodyToObject(&tableNameList))
	s.Len(tableNameList, 1)
	s.Equal(tableName, tableNameList[0])
	s.NoError(mockDB.ExpectationsWereMet())

	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()
	// add mock worker the unbounded sources should be bounded
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	workerName1 := "worker1"
	s.NoError(s1.scheduler.AddWorker(workerName1, "172.16.10.72:8262"))
	go func(ctx context.Context, workerName string) {
		s.NoError(ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL))
	}(ctx1, workerName1)
	// wait worker ready
	s.True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		w := s1.scheduler.GetWorkerBySource(source1.SourceName)
		return w != nil
	}), true)

	// mock worker get status relay not started
	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_InvalidStage)
	s1.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))

	// get source status again,source should be bounded by worker1,but relay not started
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&source1Status))
	s.Equal(source1.SourceName, source1Status.Data[0].SourceName)
	s.Equal(workerName1, source1Status.Data[0].WorkerName) // worker1 is bound
	s.Nil(source1Status.Data[0].RelayStatus)               // not start relay
	s.Equal(1, source1Status.Total)

	// list source with status
	result = testutil.NewRequest().Get(baseURL+"?with_status=true").GoWithHTTPHandler(s.T(), s1.openapiHandles)
	// check http status code
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&resultListSource))
	s.Len(resultListSource.Data, 1)
	s.Equal(1, resultListSource.Total)
	s.Equal(source1.SourceName, resultListSource.Data[0].SourceName)
	statusList := *resultListSource.Data[0].StatusList
	s.Len(statusList, 1)
	status := statusList[0]
	s.Equal(workerName1, status.WorkerName)
	s.Nil(status.RelayStatus)

	// start relay
	enableRelayURL := fmt.Sprintf("%s/relay/enable", source1URL)
	result = testutil.NewRequest().Post(enableRelayURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	// check http status code
	s.Equal(http.StatusOK, result.Code())
	relayWorkers, err := s1.scheduler.GetRelayWorkers(source1Name)
	s.NoError(err)
	s.Len(relayWorkers, 1)

	// mock worker get status relay started
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_Running)
	s1.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again, relay status should not be nil
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&source1Status))
	s.Equal(pb.Stage_Running.String(), source1Status.Data[0].RelayStatus.Stage)

	// mock worker get status meet error
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_Paused)
	s1.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again, error message should not be nil
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&source1Status))
	s.Regexp("some error happened", *source1Status.Data[0].ErrorMsg)
	s.Equal(workerName1, source1Status.Data[0].WorkerName)

	// test list source and filter by enable-relay
	result = testutil.NewRequest().Get(baseURL+"?enable_relay=true").GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&resultListSource))
	s.Len(resultListSource.Data, 1)
	result = testutil.NewRequest().Get(baseURL+"?enable_relay=false").GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&resultListSource))
	s.Len(resultListSource.Data, 0)

	// purge relay
	purgeRelay := fmt.Sprintf("%s/relay/purge", source1URL)
	purgeRelayReq := openapi.PurgeRelayRequest{RelayBinlogName: "binlog.001"}
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockPurgeRelay(mockWorkerClient)
	s1.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	result = testutil.NewRequest().Post(purgeRelay).WithJsonBody(purgeRelayReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())

	// test disable relay
	disableRelayURL := fmt.Sprintf("%s/relay/disable", source1URL)
	disableRelayReq := openapi.DisableRelayRequest{}
	result = testutil.NewRequest().Post(disableRelayURL).WithJsonBody(disableRelayReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	relayWorkers, err = s1.scheduler.GetRelayWorkers(source1Name)
	s.NoError(err)
	s.Len(relayWorkers, 0)

	// mock worker get status relay already stopped
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_InvalidStage)
	s1.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	source1Status = openapi.GetSourceStatusResponse{} // reset
	s.NoError(result.UnmarshalBodyToObject(&source1Status))
	s.Equal(source1.SourceName, source1Status.Data[0].SourceName)
	s.Equal(workerName1, source1Status.Data[0].WorkerName) // worker1 is bound
	s.Nil(source1Status.Data[0].RelayStatus)               // not start relay
	s.Equal(1, source1Status.Total)

	// delete source with --force
	result = testutil.NewRequest().Delete(fmt.Sprintf("%s/%s?force=true", baseURL, source1.SourceName)).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	// check http status code
	s.Equal(http.StatusNoContent, result.Code())

	// delete again will failed
	result = testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", baseURL, source1.SourceName)).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusBadRequest, result.Code())
	var errResp2 openapi.ErrorWithMessage
	s.NoError(result.UnmarshalBodyToObject(&errResp2))
	s.Equal(int(terror.ErrSchedulerSourceCfgNotExist.Code()), errResp2.ErrorCode)

	// list source
	result = testutil.NewRequest().Get(baseURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	// check http status code
	s.Equal(http.StatusOK, result.Code())
	var resultListSource2 openapi.GetSourceListResponse
	s.NoError(result.UnmarshalBodyToObject(&resultListSource2))
	s.Len(resultListSource2.Data, 0)
	s.Equal(0, resultListSource2.Total)
}
func TestOpenAPIViewSuite(t *testing.T) {
	suite.Run(t, new(OpenAPIViewSuite))
}
