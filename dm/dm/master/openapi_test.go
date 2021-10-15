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

	"github.com/deepmap/oapi-codegen/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/pbmock"
	"github.com/pingcap/dm/openapi"
	"github.com/pingcap/dm/openapi/fixtures"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var openAPITestSuite = check.SerialSuites(&openAPISuite{})

// some data for test.
var (
	source1Name = "mysql-replica-01"
)

type openAPISuite struct {
	testT *testing.T

	etcdTestCli     *clientv3.Client
	testEtcdCluster *integration.ClusterV3
	workerClients   map[string]workerrpc.Client
}

func (t *openAPISuite) SetUpSuite(c *check.C) {
	checkAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
}

func (t *openAPISuite) TearDownSuite(c *check.C) {
	checkAndAdjustSourceConfigFunc = checkAndAdjustSourceConfig
}

func (t *openAPISuite) SetUpTest(c *check.C) {
	t.testEtcdCluster = integration.NewClusterV3(t.testT, &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.testEtcdCluster.RandClient()
	t.workerClients = make(map[string]workerrpc.Client)

	c.Assert(ha.ClearTestInfoOperation(t.etcdTestCli), check.IsNil)
}

func (t *openAPISuite) TestRedirectRequestToLeader(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 := NewServer(cfg1)
	c.Assert(s1.Start(ctx), check.IsNil)
	defer s1.Close()

	// wait the first one become the leader
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s1.election.IsLeader()
	}), check.IsTrue)

	// join to an existing cluster
	cfg2 := NewConfig()
	c.Assert(cfg2.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg2.Name = "dm-master-2"
	cfg2.DataDir = c.MkDir()
	cfg2.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg2.PeerUrls = tempurl.Alloc()
	cfg2.AdvertisePeerUrls = cfg2.PeerUrls
	cfg2.Join = cfg1.MasterAddr // join to an existing cluster

	s2 := NewServer(cfg2)
	c.Assert(s2.Start(ctx), check.IsNil)
	defer s2.Close()

	baseURL := "/api/v1/sources"
	// list source from leader
	result := testutil.NewRequest().Get(baseURL).Go(t.testT, s1.echo)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultListSource openapi.GetSourceListResponse
	err := result.UnmarshalBodyToObject(&resultListSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource.Data, check.HasLen, 0)
	c.Assert(resultListSource.Total, check.Equals, 0)

	// list source not from leader will get a redirect
	result2 := testutil.NewRequest().Get(baseURL).Go(t.testT, s2.echo)
	c.Assert(result2.Code(), check.Equals, http.StatusTemporaryRedirect)
	cancel()
}

func (t *openAPISuite) TestSourceAPI(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	s := setupServer(ctx, c)
	defer func() {
		cancel()
		s.Close()
	}()

	baseURL := "/api/v1/sources"

	dbCfg := config.GetDBConfigForTest()
	purgeInterVal := int64(10)
	source1 := openapi.Source{
		SourceName: source1Name,
		EnableGtid: false,
		Host:       dbCfg.Host,
		Password:   dbCfg.Password,
		Port:       dbCfg.Port,
		User:       dbCfg.User,
		Purge:      &openapi.Purge{Interval: &purgeInterVal},
	}
	result := testutil.NewRequest().Post(baseURL).WithJsonBody(source1).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusCreated)
	var resultSource openapi.Source
	err := result.UnmarshalBodyToObject(&resultSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultSource.User, check.Equals, source1.User)
	c.Assert(resultSource.Host, check.Equals, source1.Host)
	c.Assert(resultSource.Port, check.Equals, source1.Port)
	c.Assert(resultSource.Password, check.Equals, source1.Password)
	c.Assert(resultSource.EnableGtid, check.Equals, source1.EnableGtid)
	c.Assert(resultSource.SourceName, check.Equals, source1.SourceName)
	c.Assert(*resultSource.Purge.Interval, check.Equals, *source1.Purge.Interval)

	// create source with same name will failed
	source2 := source1
	result2 := testutil.NewRequest().Post(baseURL).WithJsonBody(source2).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result2.Code(), check.Equals, http.StatusBadRequest)
	var errResp openapi.ErrorWithMessage
	err = result2.UnmarshalBodyToObject(&errResp)
	c.Assert(err, check.IsNil)
	c.Assert(errResp.ErrorCode, check.Equals, int(terror.ErrSchedulerSourceCfgExist.Code()))

	// list source
	result3 := testutil.NewRequest().Get(baseURL).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result3.Code(), check.Equals, http.StatusOK)
	var resultListSource openapi.GetSourceListResponse
	err = result3.UnmarshalBodyToObject(&resultListSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource.Data, check.HasLen, 1)
	c.Assert(resultListSource.Total, check.Equals, 1)
	c.Assert(resultListSource.Data[0].SourceName, check.Equals, source1.SourceName)

	// delete source
	result4 := testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", baseURL, source1.SourceName)).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result4.Code(), check.Equals, http.StatusNoContent)

	// delete again will failed
	result5 := testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", baseURL, source1.SourceName)).Go(t.testT, s.echo)
	c.Assert(result5.Code(), check.Equals, http.StatusBadRequest)
	var errResp2 openapi.ErrorWithMessage
	err = result5.UnmarshalBodyToObject(&errResp2)
	c.Assert(err, check.IsNil)
	c.Assert(errResp2.ErrorCode, check.Equals, int(terror.ErrSchedulerSourceCfgNotExist.Code()))

	// list source
	result6 := testutil.NewRequest().Get(baseURL).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result6.Code(), check.Equals, http.StatusOK)
	var resultListSource2 openapi.GetSourceListResponse
	err = result6.UnmarshalBodyToObject(&resultListSource2)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource2.Data, check.HasLen, 0)
	c.Assert(resultListSource2.Total, check.Equals, 0)
	cancel()
}

func (t *openAPISuite) TestRelayAPI(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	s := setupServer(ctx, c)
	ctrl := gomock.NewController(c)
	defer func() {
		cancel()
		s.Close()
		defer ctrl.Finish()
	}()

	baseURL := "/api/v1/sources"

	source1 := openapi.Source{
		SourceName: source1Name,
		EnableGtid: false,
		Host:       "127.0.0.1",
		Password:   "123456",
		Port:       3306,
		User:       "root",
	}
	result := testutil.NewRequest().Post(baseURL).WithJsonBody(source1).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusCreated)

	// get source status
	source1StatusURL := fmt.Sprintf("%s/%s/status", baseURL, source1.SourceName)
	result2 := testutil.NewRequest().Get(source1StatusURL).Go(t.testT, s.echo)
	c.Assert(result2.Code(), check.Equals, http.StatusOK)

	var getSourceStatusResponse openapi.GetSourceStatusResponse
	err := result2.UnmarshalBodyToObject(&getSourceStatusResponse)
	c.Assert(err, check.IsNil)
	c.Assert(getSourceStatusResponse.Data[0].SourceName, check.Equals, source1.SourceName)
	c.Assert(getSourceStatusResponse.Data[0].WorkerName, check.Equals, "") // no worker bound
	c.Assert(getSourceStatusResponse.Total, check.Equals, 1)

	// add mock worker the unbounded sources should be bounded
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	workerName1 := "worker1"
	c.Assert(s.scheduler.AddWorker(workerName1, "172.16.10.72:8262"), check.IsNil)
	go func(ctx context.Context, workerName string) {
		c.Assert(ha.KeepAlive(ctx, s.etcdClient, workerName, keepAliveTTL), check.IsNil)
	}(ctx1, workerName1)
	// wait worker ready
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		w := s.scheduler.GetWorkerBySource(source1.SourceName)
		return w != nil
	}), check.IsTrue)

	// mock worker get status relay not started
	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_InvalidStage)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))

	// get source status again,source should be bounded by worker1,but relay not started
	result3 := testutil.NewRequest().Get(source1StatusURL).Go(t.testT, s.echo)
	c.Assert(result3.Code(), check.Equals, http.StatusOK)
	var getSourceStatusResponse2 openapi.GetSourceStatusResponse
	err = result3.UnmarshalBodyToObject(&getSourceStatusResponse2)
	c.Assert(err, check.IsNil)
	c.Assert(getSourceStatusResponse2.Data[0].SourceName, check.Equals, source1.SourceName)
	c.Assert(getSourceStatusResponse2.Data[0].WorkerName, check.Equals, workerName1) // worker1 is bound
	c.Assert(getSourceStatusResponse2.Data[0].RelayStatus, check.IsNil)              // not start relay
	c.Assert(getSourceStatusResponse2.Total, check.Equals, 1)

	// start relay
	startRelayURL := fmt.Sprintf("%s/%s/start-relay", baseURL, source1.SourceName)
	openAPIStartRelayReq := openapi.StartRelayRequest{WorkerNameList: []string{workerName1}}
	result4 := testutil.NewRequest().Patch(startRelayURL).WithJsonBody(openAPIStartRelayReq).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result4.Code(), check.Equals, http.StatusOK)
	relayWorkers, err := s.scheduler.GetRelayWorkers(source1Name)
	c.Assert(err, check.IsNil)
	c.Assert(relayWorkers, check.HasLen, 1)

	// mock worker get status relay started
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_Running)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again, relay status should not be nil
	result5 := testutil.NewRequest().Get(source1StatusURL).Go(t.testT, s.echo)
	c.Assert(result5.Code(), check.Equals, http.StatusOK)
	var getSourceStatusResponse3 openapi.GetSourceStatusResponse
	err = result5.UnmarshalBodyToObject(&getSourceStatusResponse3)
	c.Assert(err, check.IsNil)
	c.Assert(getSourceStatusResponse3.Data[0].RelayStatus.Stage, check.Equals, pb.Stage_Running.String())

	// mock worker get status meet error
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_Paused)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again, error message should not be nil
	result6 := testutil.NewRequest().Get(source1StatusURL).Go(t.testT, s.echo)
	c.Assert(result6.Code(), check.Equals, http.StatusOK)
	var getSourceStatusResponse4 openapi.GetSourceStatusResponse
	err = result6.UnmarshalBodyToObject(&getSourceStatusResponse4)
	c.Assert(err, check.IsNil)
	c.Assert(*getSourceStatusResponse4.Data[0].ErrorMsg, check.Equals, "some error happened")
	c.Assert(getSourceStatusResponse4.Data[0].WorkerName, check.Equals, workerName1)

	// test stop relay
	stopRelayURL := fmt.Sprintf("%s/%s/stop-relay", baseURL, source1.SourceName)
	stopRelayReq := openapi.StopRelayRequest{WorkerNameList: []string{workerName1}}
	result7 := testutil.NewRequest().Patch(stopRelayURL).WithJsonBody(stopRelayReq).Go(t.testT, s.echo)
	c.Assert(result7.Code(), check.Equals, http.StatusOK)
	relayWorkers, err = s.scheduler.GetRelayWorkers(source1Name)
	c.Assert(err, check.IsNil)
	c.Assert(relayWorkers, check.HasLen, 0)

	// mock worker get status relay already stopped
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_InvalidStage)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again,source
	result8 := testutil.NewRequest().Get(source1StatusURL).Go(t.testT, s.echo)
	c.Assert(result8.Code(), check.Equals, http.StatusOK)

	var getSourceStatusResponse5 openapi.GetSourceStatusResponse
	err = result8.UnmarshalBodyToObject(&getSourceStatusResponse5)
	c.Assert(err, check.IsNil)
	c.Assert(getSourceStatusResponse5.Data[0].SourceName, check.Equals, source1.SourceName)
	c.Assert(getSourceStatusResponse5.Data[0].WorkerName, check.Equals, workerName1) // worker1 is bound
	c.Assert(getSourceStatusResponse5.Data[0].RelayStatus, check.IsNil)              // not start relay
	c.Assert(getSourceStatusResponse5.Total, check.Equals, 1)
	cancel()
}

func (t *openAPISuite) TestTaskAPI(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	s := setupServer(ctx, c)
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/master/MockSkipAdjustTargetDB", `return(true)`), check.IsNil)
	checker.CheckSyncConfigFunc = mockCheckSyncConfig
	defer func() {
		checker.CheckSyncConfigFunc = checker.CheckSyncConfig
		cancel()
		s.Close()
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
	// create source
	sourceURL := "/api/v1/sources"
	result := testutil.NewRequest().Post(sourceURL).WithJsonBody(source1).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusCreated)

	// add mock worker  start workers, the unbounded sources should be bounded
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	workerName1 := "worker-1"
	c.Assert(s.scheduler.AddWorker(workerName1, "172.16.10.72:8262"), check.IsNil)
	go func(ctx context.Context, workerName string) {
		c.Assert(ha.KeepAlive(ctx, s.etcdClient, workerName, keepAliveTTL), check.IsNil)
	}(ctx1, workerName1)
	// wait worker ready
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		w := s.scheduler.GetWorkerBySource(source1.SourceName)
		return w != nil
	}), check.IsTrue)

	// create task
	taskURL := "/api/v1/tasks"

	task, err := fixtures.GenNoShardOpenAPITaskForTest()
	c.Assert(err, check.IsNil)
	// use a valid target db
	task.TargetConfig.Host = dbCfg.Host
	task.TargetConfig.Port = dbCfg.Port
	task.TargetConfig.User = dbCfg.User
	task.TargetConfig.Password = dbCfg.Password

	createTaskReq := openapi.CreateTaskRequest{RemoveMeta: false, Task: task}
	result2 := testutil.NewRequest().Post(taskURL).WithJsonBody(createTaskReq).Go(t.testT, s.echo)
	c.Assert(result2.Code(), check.Equals, http.StatusCreated)
	var createTaskResp openapi.Task
	err = result2.UnmarshalBodyToObject(&createTaskResp)
	c.Assert(err, check.IsNil)
	c.Assert(task.Name, check.Equals, createTaskResp.Name)
	subTaskM := s.scheduler.GetSubTaskCfgsByTask(task.Name)
	c.Assert(len(subTaskM) == 1, check.IsTrue)
	c.Assert(subTaskM[source1Name].Name, check.Equals, task.Name)

	// stop task
	result3 := testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", taskURL, task.Name)).Go(t.testT, s.echo)
	c.Assert(result3.Code(), check.Equals, http.StatusNoContent)
	subTaskM = s.scheduler.GetSubTaskCfgsByTask(task.Name)
	c.Assert(len(subTaskM) == 0, check.IsTrue)
	c.Assert(failpoint.Disable("github.com/pingcap/dm/dm/master/MockSkipAdjustTargetDB"), check.IsNil)
}

func setupServer(ctx context.Context, c *check.C) *Server {
	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 := NewServer(cfg1)
	c.Assert(s1.Start(ctx), check.IsNil)
	// wait the first one become the leader
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s1.election.IsLeader() && s1.scheduler.Started()
	}), check.IsTrue)
	return s1
}

// nolint:unparam
func mockRelayQueryStatus(
	mockWorkerClient *pbmock.MockWorkerClient, sourceName, workerName string, stage pb.Stage) {
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

func mockCheckSyncConfig(ctx context.Context, cfgs []*config.SubTaskConfig, errCnt, warnCnt int64) error {
	return nil
}
