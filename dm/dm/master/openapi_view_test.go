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
	"github.com/tikv/pd/pkg/tempurl"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"

	"github.com/pingcap/tiflow/dm/checker"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/pbmock"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/openapi/fixtures"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
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
}

func (t *openAPISuite) SetUpSuite(c *check.C) {
	checkAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
	t.testEtcdCluster = integration.NewClusterV3(t.testT, &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.testEtcdCluster.RandClient()
}

func (t *openAPISuite) TearDownSuite(c *check.C) {
	checkAndAdjustSourceConfigFunc = checkAndAdjustSourceConfig
	t.testEtcdCluster.Terminate(t.testT)
}

func (t *openAPISuite) SetUpTest(c *check.C) {
	c.Assert(ha.ClearTestInfoOperation(t.etcdTestCli), check.IsNil)
}

func (t *openAPISuite) TestRedirectRequestToLeader(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.FromContent(SampleConfig), check.IsNil)
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.AdvertiseAddr = cfg1.MasterAddr
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)
	cfg1.OpenAPI = true

	s1 := NewServer(cfg1)
	c.Assert(s1.Start(ctx), check.IsNil)
	defer s1.Close()
	defer cancel()

	// wait the first one become the leader
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s1.election.IsLeader() && s1.scheduler.Started()
	}), check.IsTrue)

	// join to an existing cluster
	cfg2 := NewConfig()
	c.Assert(cfg2.FromContent(SampleConfig), check.IsNil)
	cfg2.Name = "dm-master-2"
	cfg2.DataDir = c.MkDir()
	cfg2.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg2.AdvertiseAddr = cfg2.MasterAddr
	cfg2.PeerUrls = tempurl.Alloc()
	cfg2.AdvertisePeerUrls = cfg2.PeerUrls
	cfg2.Join = cfg1.MasterAddr // join to an existing cluster
	cfg2.OpenAPI = true

	s2 := NewServer(cfg2)
	c.Assert(s2.Start(ctx), check.IsNil)
	defer s2.Close()
	defer cancel() // this cancel must call before s.Close() to avoid deadlock

	// wait the second master ready
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s2.election.IsLeader()
	}), check.IsFalse)

	baseURL := "/api/v1/sources"
	// list source from leader
	result := testutil.NewRequest().Get(baseURL).GoWithHTTPHandler(t.testT, s1.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultListSource openapi.GetSourceListResponse
	err := result.UnmarshalBodyToObject(&resultListSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource.Data, check.HasLen, 0)
	c.Assert(resultListSource.Total, check.Equals, 0)

	// list source not from leader will get a redirect
	result2 := testutil.NewRequest().Get(baseURL).GoWithHTTPHandler(t.testT, s2.openapiHandles)
	c.Assert(result2.Code(), check.Equals, http.StatusTemporaryRedirect)
}

func (t *openAPISuite) TestOpenAPIWillNotStartInDefaultConfig(c *check.C) {
	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.FromContent(SampleConfig), check.IsNil)
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.AdvertiseAddr = cfg1.MasterAddr
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 := NewServer(cfg1)
	ctx, cancel := context.WithCancel(context.Background())
	c.Assert(s1.Start(ctx), check.IsNil)
	// wait the first one become the leader
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s1.election.IsLeader() && s1.scheduler.Started()
	}), check.IsTrue)
	c.Assert(s1.openapiHandles, check.IsNil)
	defer s1.Close()
	defer cancel()
}

func (t *openAPISuite) TestSourceAPI(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	s := setupTestServer(ctx, t.testT)
	defer func() {
		cancel()
		s.Close()
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
	result := testutil.NewRequest().Post(baseURL).WithJsonBody(createReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
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
	result = testutil.NewRequest().Post(baseURL).WithJsonBody(createReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusBadRequest)
	var errResp openapi.ErrorWithMessage
	err = result.UnmarshalBodyToObject(&errResp)
	c.Assert(err, check.IsNil)
	c.Assert(errResp.ErrorCode, check.Equals, int(terror.ErrSchedulerSourceCfgExist.Code()))

	// get source
	source1URL := fmt.Sprintf("%s/%s", baseURL, source1Name)
	var source1FromHTTP openapi.Source
	result = testutil.NewRequest().Get(source1URL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&source1FromHTTP), check.IsNil)
	c.Assert(source1.SourceName, check.Equals, source1FromHTTP.SourceName)
	// update a source
	clone := source1
	clone.EnableGtid = true
	updateReq := openapi.UpdateSourceRequest{Source: clone}
	result = testutil.NewRequest().Put(source1URL).WithJsonBody(updateReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&source1FromHTTP), check.IsNil)
	c.Assert(clone.EnableGtid, check.Equals, source1FromHTTP.EnableGtid)

	// get source not existed
	sourceNotExistedURL := fmt.Sprintf("%s/not_existed", baseURL)
	result = testutil.NewRequest().Get(sourceNotExistedURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusNotFound)
	// get source status
	var source1Status openapi.GetSourceStatusResponse
	source1StatusURL := fmt.Sprintf("%s/%s/status", baseURL, source1Name)
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&source1Status), check.IsNil)
	c.Assert(source1Status.Data, check.HasLen, 1)
	c.Assert(source1Status.Data[0].SourceName, check.Equals, source1.SourceName)
	c.Assert(source1Status.Data[0].WorkerName, check.Equals, "") // no worker now

	// list source
	result = testutil.NewRequest().Get(baseURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultListSource openapi.GetSourceListResponse
	err = result.UnmarshalBodyToObject(&resultListSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource.Data, check.HasLen, 1)
	c.Assert(resultListSource.Total, check.Equals, 1)
	c.Assert(resultListSource.Data[0].SourceName, check.Equals, source1.SourceName)

	// test get source schema and table
	mockDB := conn.InitMockDB(c)
	schemaName := "information_schema"
	mockDB.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"Database"}).AddRow(schemaName))

	schemaURL := fmt.Sprintf("%s/%s/schemas", baseURL, source1.SourceName)
	result = testutil.NewRequest().Get(schemaURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var schemaNameList openapi.SchemaNameList
	err = result.UnmarshalBodyToObject(&schemaNameList)
	c.Assert(err, check.IsNil)
	c.Assert(schemaNameList, check.HasLen, 1)
	c.Assert(schemaNameList[0], check.Equals, schemaName)
	c.Assert(mockDB.ExpectationsWereMet(), check.IsNil)

	mockDB = conn.InitMockDB(c)
	tableName := "CHARACTER_SETS"
	mockDB.ExpectQuery("SHOW FULL TABLES IN `information_schema` WHERE Table_Type != 'VIEW';").WillReturnRows(
		sqlmock.NewRows([]string{"Tables_in_information_schema", "Table_type"}).AddRow(tableName, "BASE TABLE"))
	tableURL := fmt.Sprintf("%s/%s/schemas/%s", baseURL, source1.SourceName, schemaName)
	result = testutil.NewRequest().Get(tableURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var tableNameList openapi.TableNameList
	err = result.UnmarshalBodyToObject(&tableNameList)
	c.Assert(err, check.IsNil)
	c.Assert(tableNameList, check.HasLen, 1)
	c.Assert(tableNameList[0], check.Equals, tableName)
	c.Assert(mockDB.ExpectationsWereMet(), check.IsNil)

	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
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
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&source1Status), check.IsNil)
	c.Assert(source1Status.Data[0].SourceName, check.Equals, source1.SourceName)
	c.Assert(source1Status.Data[0].WorkerName, check.Equals, workerName1) // worker1 is bound
	c.Assert(source1Status.Data[0].RelayStatus, check.IsNil)              // not start relay
	c.Assert(source1Status.Total, check.Equals, 1)

	// list source with status
	result = testutil.NewRequest().Get(baseURL+"?with_status=true").GoWithHTTPHandler(t.testT, s.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	err = result.UnmarshalBodyToObject(&resultListSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource.Data, check.HasLen, 1)
	c.Assert(resultListSource.Total, check.Equals, 1)
	c.Assert(resultListSource.Data[0].SourceName, check.Equals, source1.SourceName)
	statusList := *resultListSource.Data[0].StatusList
	c.Assert(statusList, check.HasLen, 1)
	status := statusList[0]
	c.Assert(status.WorkerName, check.Equals, workerName1)
	c.Assert(status.RelayStatus, check.IsNil)

	// start relay
	enableRelayURL := fmt.Sprintf("%s/relay/enable", source1URL)
	result = testutil.NewRequest().Post(enableRelayURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	relayWorkers, err := s.scheduler.GetRelayWorkers(source1Name)
	c.Assert(err, check.IsNil)
	c.Assert(relayWorkers, check.HasLen, 1)

	// mock worker get status relay started
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_Running)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again, relay status should not be nil
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&source1Status), check.IsNil)
	c.Assert(source1Status.Data[0].RelayStatus.Stage, check.Equals, pb.Stage_Running.String())

	// mock worker get status meet error
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_Paused)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again, error message should not be nil
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&source1Status), check.IsNil)
	c.Assert(*source1Status.Data[0].ErrorMsg, check.Equals, "some error happened")
	c.Assert(source1Status.Data[0].WorkerName, check.Equals, workerName1)

	// test list source and filter by enable-relay
	result = testutil.NewRequest().Get(baseURL+"?enable_relay=true").GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&resultListSource), check.IsNil)
	c.Assert(resultListSource.Data, check.HasLen, 1)
	result = testutil.NewRequest().Get(baseURL+"?enable_relay=false").GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&resultListSource), check.IsNil)
	c.Assert(resultListSource.Data, check.HasLen, 0)

	// purge relay
	purgeRelay := fmt.Sprintf("%s/relay/purge", source1URL)
	purgeRelayReq := openapi.PurgeRelayRequest{RelayBinlogName: "binlog.001"}
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockPurgeRelay(mockWorkerClient)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	result = testutil.NewRequest().Post(purgeRelay).WithJsonBody(purgeRelayReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)

	// test disable relay
	disableRelayURL := fmt.Sprintf("%s/relay/disable", source1URL)
	disableRelayReq := openapi.DisableRelayRequest{}
	result = testutil.NewRequest().Post(disableRelayURL).WithJsonBody(disableRelayReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	relayWorkers, err = s.scheduler.GetRelayWorkers(source1Name)
	c.Assert(err, check.IsNil)
	c.Assert(relayWorkers, check.HasLen, 0)

	// mock worker get status relay already stopped
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_InvalidStage)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	source1Status = openapi.GetSourceStatusResponse{} // reset
	c.Assert(result.UnmarshalBodyToObject(&source1Status), check.IsNil)
	c.Assert(source1Status.Data[0].SourceName, check.Equals, source1.SourceName)
	c.Assert(source1Status.Data[0].WorkerName, check.Equals, workerName1) // worker1 is bound
	c.Assert(source1Status.Data[0].RelayStatus, check.IsNil)              // not start relay
	c.Assert(source1Status.Total, check.Equals, 1)

	// delete source with --force
	result = testutil.NewRequest().Delete(fmt.Sprintf("%s/%s?force=true", baseURL, source1.SourceName)).GoWithHTTPHandler(t.testT, s.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusNoContent)

	// delete again will failed
	result = testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", baseURL, source1.SourceName)).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusBadRequest)
	var errResp2 openapi.ErrorWithMessage
	err = result.UnmarshalBodyToObject(&errResp2)
	c.Assert(err, check.IsNil)
	c.Assert(errResp2.ErrorCode, check.Equals, int(terror.ErrSchedulerSourceCfgNotExist.Code()))

	// list source
	result = testutil.NewRequest().Get(baseURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultListSource2 openapi.GetSourceListResponse
	err = result.UnmarshalBodyToObject(&resultListSource2)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource2.Data, check.HasLen, 0)
	c.Assert(resultListSource2.Total, check.Equals, 0)
}

func (t *openAPISuite) TestTaskAPI(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	s := setupTestServer(ctx, t.testT)
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB", `return(true)`), check.IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/MockSkipRemoveMetaData", `return(true)`), check.IsNil)
	checker.CheckSyncConfigFunc = mockCheckSyncConfig
	ctrl := gomock.NewController(c)
	defer func() {
		checker.CheckSyncConfigFunc = checker.CheckSyncConfig
		cancel()
		s.Close()
		ctrl.Finish()
		c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB"), check.IsNil)
		c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/MockSkipRemoveMetaData"), check.IsNil)
	}()

	dbCfg := config.GetDBConfigForTest()
	source1 := openapi.Source{
		Enable:     true,
		SourceName: source1Name,
		EnableGtid: false,
		Host:       dbCfg.Host,
		Password:   dbCfg.Password,
		Port:       dbCfg.Port,
		User:       dbCfg.User,
	}
	// create source
	sourceURL := "/api/v1/sources"
	createSourceReq := openapi.CreateSourceRequest{Source: source1}
	result := testutil.NewRequest().Post(sourceURL).WithJsonBody(createSourceReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
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

	// create task
	createTaskReq := openapi.CreateTaskRequest{Task: task}
	result = testutil.NewRequest().Post(taskURL).WithJsonBody(createTaskReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusCreated)
	var createTaskResp openapi.Task
	err = result.UnmarshalBodyToObject(&createTaskResp)
	c.Assert(err, check.IsNil)
	c.Assert(task.Name, check.Equals, createTaskResp.Name)
	subTaskM := s.scheduler.GetSubTaskCfgsByTask(task.Name)
	c.Assert(len(subTaskM) == 1, check.IsTrue)
	c.Assert(subTaskM[source1Name].Name, check.Equals, task.Name)

	// get task
	task1URL := fmt.Sprintf("%s/%s", taskURL, task.Name)
	var task1FromHTTP openapi.Task
	result = testutil.NewRequest().Get(task1URL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&task1FromHTTP), check.IsNil)
	c.Assert(task.Name, check.Equals, task1FromHTTP.Name)

	// update a task
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/scheduler/operateCheckSubtasksCanUpdate", `return("success")`), check.IsNil)
	clone := task
	batch := 1000
	clone.SourceConfig.IncrMigrateConf.ReplBatch = &batch
	updateReq := openapi.UpdateTaskRequest{Task: clone}
	result = testutil.NewRequest().Put(task1URL).WithJsonBody(updateReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&task1FromHTTP), check.IsNil)
	c.Assert(clone.SourceConfig.IncrMigrateConf.ReplBatch, check.DeepEquals, task1FromHTTP.SourceConfig.IncrMigrateConf.ReplBatch)
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/operateCheckSubtasksCanUpdate"), check.IsNil)

	// list tasks
	result = testutil.NewRequest().Get(taskURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultTaskList openapi.GetTaskListResponse
	err = result.UnmarshalBodyToObject(&resultTaskList)
	c.Assert(err, check.IsNil)
	c.Assert(resultTaskList.Total, check.Equals, 1)
	c.Assert(resultTaskList.Data[0].Name, check.Equals, task.Name)

	t.testImportTaskTemplate(c, &task, s)

	// start task
	startTaskURL := fmt.Sprintf("%s/%s/start", taskURL, task.Name)
	result = testutil.NewRequest().Post(startTaskURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(s.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect, check.Equals, pb.Stage_Running)

	// get task status
	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	mockTaskQueryStatus(mockWorkerClient, task.Name, source1.SourceName, workerName1, false)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	taskStatusURL := fmt.Sprintf("%s/%s/status", taskURL, task.Name)
	result = testutil.NewRequest().Get(taskStatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultTaskStatus openapi.GetTaskStatusResponse
	err = result.UnmarshalBodyToObject(&resultTaskStatus)
	c.Assert(err, check.IsNil)
	c.Assert(resultTaskStatus.Total, check.Equals, 1) // only 1 subtask
	c.Assert(resultTaskStatus.Data[0].Name, check.Equals, task.Name)
	c.Assert(resultTaskStatus.Data[0].Stage, check.Equals, openapi.TaskStageRunning)
	c.Assert(resultTaskStatus.Data[0].WorkerName, check.Equals, workerName1)
	c.Assert(resultTaskStatus.Data[0].DumpStatus.CompletedTables, check.Equals, float64(0))
	c.Assert(resultTaskStatus.Data[0].DumpStatus.TotalTables, check.Equals, int64(1))
	c.Assert(resultTaskStatus.Data[0].DumpStatus.EstimateTotalRows, check.Equals, float64(10))

	// get task status with source name
	taskStatusURL = fmt.Sprintf("%s/%s/status?source_name_list=%s", taskURL, task.Name, source1Name)
	result = testutil.NewRequest().Get(taskStatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultTaskStatusWithStatus openapi.GetTaskStatusResponse
	err = result.UnmarshalBodyToObject(&resultTaskStatusWithStatus)
	c.Assert(err, check.IsNil)
	c.Assert(resultTaskStatusWithStatus, check.DeepEquals, resultTaskStatus)

	// list task with status
	result = testutil.NewRequest().Get(taskURL+"?with_status=true").GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultListTask openapi.GetTaskListResponse
	err = result.UnmarshalBodyToObject(&resultListTask)
	c.Assert(err, check.IsNil)
	c.Assert(resultListTask.Data, check.HasLen, 1)
	c.Assert(resultListTask.Total, check.Equals, 1)
	c.Assert(resultListTask.Data[0].StatusList, check.NotNil)
	statusList := *resultListTask.Data[0].StatusList
	status := statusList[0]
	c.Assert(status.WorkerName, check.Equals, workerName1)
	c.Assert(status.Name, check.Equals, task.Name)

	// list with filter
	result = testutil.NewRequest().Get(taskURL+"?stage=Stopped").GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	resultListTask = openapi.GetTaskListResponse{} // reset
	err = result.UnmarshalBodyToObject(&resultListTask)
	c.Assert(err, check.IsNil)
	c.Assert(resultListTask.Data, check.HasLen, 0)

	result = testutil.NewRequest().Get(taskURL+"?stage=Running").GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	resultListTask = openapi.GetTaskListResponse{} // reset
	err = result.UnmarshalBodyToObject(&resultListTask)
	c.Assert(err, check.IsNil)
	c.Assert(resultListTask.Data, check.HasLen, 1)

	result = testutil.NewRequest().Get(taskURL+"?stage=Running&source_name_list=notsource").GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	resultListTask = openapi.GetTaskListResponse{} // reset
	err = result.UnmarshalBodyToObject(&resultListTask)
	c.Assert(err, check.IsNil)
	c.Assert(resultListTask.Data, check.HasLen, 0)

	result = testutil.NewRequest().Get(taskURL+"?stage=Running&source_name_list="+source1Name).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	resultListTask = openapi.GetTaskListResponse{} // reset
	err = result.UnmarshalBodyToObject(&resultListTask)
	c.Assert(err, check.IsNil)
	c.Assert(resultListTask.Data, check.HasLen, 1)

	// get task with status
	result = testutil.NewRequest().Get(task1URL+"?with_status=true").GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&task1FromHTTP), check.IsNil)
	c.Assert(task.Name, check.Equals, task1FromHTTP.Name)
	statusList = *task1FromHTTP.StatusList
	c.Assert(statusList, check.HasLen, 1)
	c.Assert(statusList[0].WorkerName, check.Equals, workerName1)
	c.Assert(statusList[0].Name, check.Equals, task.Name)

	// test some error happened on worker
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockTaskQueryStatus(mockWorkerClient, task.Name, source1.SourceName, workerName1, true)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	result = testutil.NewRequest().Get(taskURL+"?with_status=true").GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(result.UnmarshalBodyToObject(&resultListTask), check.IsNil)
	c.Assert(resultListTask.Data, check.HasLen, 1)
	c.Assert(resultListTask.Total, check.Equals, 1)
	c.Assert(resultListTask.Data[0].StatusList, check.NotNil)
	statusList = *resultListTask.Data[0].StatusList
	c.Assert(statusList, check.HasLen, 1)
	status = statusList[0]
	c.Assert(status.ErrorMsg, check.NotNil)

	// test convertTaskConfig
	convertReq := openapi.ConverterTaskRequest{}
	convertResp := openapi.ConverterTaskResponse{}
	convertURL := fmt.Sprintf("%s/%s", taskURL, "converters")
	result = testutil.NewRequest().Post(convertURL).WithJsonBody(convertReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusBadRequest) // not valid req

	// from task to taskConfig
	convertReq.Task = &task
	result = testutil.NewRequest().Post(convertURL).WithJsonBody(convertReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	err = result.UnmarshalBodyToObject(&convertResp)
	c.Assert(err, check.IsNil)
	c.Assert(convertResp.Task, check.NotNil)
	c.Assert(convertResp.TaskConfigFile, check.NotNil)
	taskConfigFile := convertResp.TaskConfigFile

	// from taskCfg to task
	convertReq.Task = nil
	convertReq.TaskConfigFile = &taskConfigFile
	result = testutil.NewRequest().Post(convertURL).WithJsonBody(convertReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	err = result.UnmarshalBodyToObject(&convertResp)
	c.Assert(err, check.IsNil)
	c.Assert(convertResp.Task, check.NotNil)
	c.Assert(convertResp.TaskConfigFile, check.NotNil)
	taskConfigFile2 := convertResp.TaskConfigFile
	c.Assert(taskConfigFile, check.Equals, taskConfigFile2)

	t.testSourceOperationWithTask(c, &source1, &task, s)

	// stop task
	stopTaskURL := fmt.Sprintf("%s/%s/stop", taskURL, task.Name)
	stopTaskReq := openapi.StopTaskRequest{}
	result = testutil.NewRequest().Post(stopTaskURL).WithJsonBody(stopTaskReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(s.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect, check.Equals, pb.Stage_Stopped)

	// delete task
	result = testutil.NewRequest().Delete(task1URL+"?force=true").GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusNoContent)
	subTaskM = s.scheduler.GetSubTaskCfgsByTask(task.Name)
	c.Assert(len(subTaskM) == 0, check.IsTrue)

	// list tasks
	result = testutil.NewRequest().Get(taskURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	resultListTask = openapi.GetTaskListResponse{} // reset
	err = result.UnmarshalBodyToObject(&resultTaskList)
	c.Assert(err, check.IsNil)
	c.Assert(resultTaskList.Total, check.Equals, 0)
}

func (t *openAPISuite) TestClusterAPI(c *check.C) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	s1 := setupTestServer(ctx1, t.testT)
	defer func() {
		cancel1()
		s1.Close()
	}()

	// join a new master node to an existing cluster
	cfg2 := NewConfig()
	c.Assert(cfg2.FromContent(SampleConfig), check.IsNil)
	cfg2.Name = "dm-master-2"
	cfg2.DataDir = c.MkDir()
	cfg2.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg2.PeerUrls = tempurl.Alloc()
	cfg2.AdvertisePeerUrls = cfg2.PeerUrls
	cfg2.Join = s1.cfg.MasterAddr // join to an existing cluster
	cfg2.AdvertiseAddr = cfg2.MasterAddr
	s2 := NewServer(cfg2)
	ctx2, cancel2 := context.WithCancel(context.Background())
	c.Assert(s2.Start(ctx2), check.IsNil)
	defer func() {
		cancel2()
		s2.Close()
	}()

	baseURL := "/api/v1/cluster/"
	masterURL := baseURL + "masters"

	result := testutil.NewRequest().Get(masterURL).GoWithHTTPHandler(t.testT, s1.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultMasters openapi.GetClusterMasterListResponse
	err := result.UnmarshalBodyToObject(&resultMasters)
	c.Assert(err, check.IsNil)
	c.Assert(resultMasters.Total, check.Equals, 2)
	c.Assert(resultMasters.Data[0].Name, check.Equals, s1.cfg.Name)
	c.Assert(resultMasters.Data[0].Addr, check.Equals, s1.cfg.PeerUrls)
	c.Assert(resultMasters.Data[0].Leader, check.IsTrue)
	c.Assert(resultMasters.Data[0].Alive, check.IsTrue)

	// check cluster id
	clusterIDURL := baseURL + "info"
	resp := testutil.NewRequest().Get(clusterIDURL).GoWithHTTPHandler(t.testT, s1.openapiHandles)
	c.Assert(resp.Code(), check.Equals, http.StatusOK)
	var clusterIDResp openapi.GetClusterInfoResponse
	err = resp.UnmarshalBodyToObject(&clusterIDResp)
	c.Assert(err, check.IsNil)
	c.Assert(clusterIDResp.ClusterId, check.Greater, uint64(0))

	// offline master-2 with retry
	// operate etcd cluster may met `etcdserver: unhealthy cluster`, add some retry
	for i := 0; i < 20; i++ {
		result = testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", masterURL, s2.cfg.Name)).GoWithHTTPHandler(t.testT, s1.openapiHandles)
		if result.Code() == http.StatusBadRequest {
			c.Assert(result.Code(), check.Equals, http.StatusBadRequest)
			errResp := &openapi.ErrorWithMessage{}
			err = result.UnmarshalBodyToObject(&errResp)
			c.Assert(err, check.IsNil)
			c.Assert(errResp.ErrorMsg, check.Matches, "etcdserver: unhealthy cluster")
			time.Sleep(time.Second)
		} else {
			c.Assert(result.Code(), check.Equals, http.StatusNoContent)
			break
		}
	}
	cancel2() // stop dm-master-2

	// list master again get one node
	result = testutil.NewRequest().Get(masterURL).GoWithHTTPHandler(t.testT, s1.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	err = result.UnmarshalBodyToObject(&resultMasters)
	c.Assert(err, check.IsNil)
	c.Assert(resultMasters.Total, check.Equals, 1)

	workerName1 := "worker1"
	c.Assert(s1.scheduler.AddWorker(workerName1, "172.16.10.72:8262"), check.IsNil)
	// list worker node
	workerURL := baseURL + "workers"
	result = testutil.NewRequest().Get(workerURL).GoWithHTTPHandler(t.testT, s1.openapiHandles)
	var resultWorkers openapi.GetClusterWorkerListResponse
	err = result.UnmarshalBodyToObject(&resultWorkers)
	c.Assert(err, check.IsNil)
	c.Assert(resultWorkers.Total, check.Equals, 1)

	// offline worker-1
	result = testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", workerURL, workerName1)).GoWithHTTPHandler(t.testT, s1.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusNoContent)
	// after offline, no worker node
	result = testutil.NewRequest().Get(workerURL).GoWithHTTPHandler(t.testT, s1.openapiHandles)
	err = result.UnmarshalBodyToObject(&resultWorkers)
	c.Assert(err, check.IsNil)
	c.Assert(resultWorkers.Total, check.Equals, 0)

	cancel1()
}

func (t *openAPISuite) TestTaskTemplatesAPI(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	s := setupTestServer(ctx, t.testT)
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB", `return(true)`), check.IsNil)
	checker.CheckSyncConfigFunc = mockCheckSyncConfig
	defer func() {
		checker.CheckSyncConfigFunc = checker.CheckSyncConfig
		cancel()
		s.Close()
		c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB"), check.IsNil)
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
	result := testutil.NewRequest().Post(sourceURL).WithJsonBody(createReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusCreated)

	// create task config template
	url := "/api/v1/tasks/templates"

	task, err := fixtures.GenNoShardOpenAPITaskForTest()
	c.Assert(err, check.IsNil)
	// use a valid target db
	task.TargetConfig.Host = dbCfg.Host
	task.TargetConfig.Port = dbCfg.Port
	task.TargetConfig.User = dbCfg.User
	task.TargetConfig.Password = dbCfg.Password

	// create one
	result = testutil.NewRequest().Post(url).WithJsonBody(task).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusCreated)
	var createTaskResp openapi.Task
	err = result.UnmarshalBodyToObject(&createTaskResp)
	c.Assert(err, check.IsNil)
	c.Assert(task.Name, check.Equals, createTaskResp.Name)

	// create again will fail
	result = testutil.NewRequest().Post(url).WithJsonBody(task).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusBadRequest)
	var errResp openapi.ErrorWithMessage
	err = result.UnmarshalBodyToObject(&errResp)
	c.Assert(err, check.IsNil)
	c.Assert(errResp.ErrorCode, check.Equals, int(terror.ErrOpenAPITaskConfigExist.Code()))

	// list templates
	result = testutil.NewRequest().Get(url).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultTaskList openapi.GetTaskListResponse
	err = result.UnmarshalBodyToObject(&resultTaskList)
	c.Assert(err, check.IsNil)
	c.Assert(resultTaskList.Total, check.Equals, 1)
	c.Assert(resultTaskList.Data[0].Name, check.Equals, task.Name)

	// get detail
	oneURL := fmt.Sprintf("%s/%s", url, task.Name)
	result = testutil.NewRequest().Get(oneURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var respTask openapi.Task
	err = result.UnmarshalBodyToObject(&respTask)
	c.Assert(err, check.IsNil)
	c.Assert(respTask.Name, check.Equals, task.Name)

	// get not exist
	notExistURL := fmt.Sprintf("%s/%s", url, "notexist")
	result = testutil.NewRequest().Get(notExistURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusBadRequest)
	err = result.UnmarshalBodyToObject(&errResp)
	c.Assert(err, check.IsNil)
	c.Assert(errResp.ErrorCode, check.Equals, int(terror.ErrOpenAPITaskConfigNotExist.Code()))

	// update
	task.TaskMode = openapi.TaskTaskModeAll
	result = testutil.NewRequest().Put(oneURL).WithJsonBody(task).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	err = result.UnmarshalBodyToObject(&respTask)
	c.Assert(err, check.IsNil)
	c.Assert(respTask.Name, check.Equals, task.Name)

	// update not exist will fail
	task.Name = "notexist"
	result = testutil.NewRequest().Put(notExistURL).WithJsonBody(task).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusBadRequest)
	err = result.UnmarshalBodyToObject(&errResp)
	c.Assert(err, check.IsNil)
	c.Assert(errResp.ErrorCode, check.Equals, int(terror.ErrOpenAPITaskConfigNotExist.Code()))

	// delete task config template
	result = testutil.NewRequest().Delete(oneURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusNoContent)
	result = testutil.NewRequest().Get(url).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	err = result.UnmarshalBodyToObject(&resultTaskList)
	c.Assert(err, check.IsNil)
	c.Assert(resultTaskList.Total, check.Equals, 0)
}

func (t *openAPISuite) testImportTaskTemplate(c *check.C, task *openapi.Task, s *Server) {
	// test batch import task config
	taskBatchImportURL := "/api/v1/tasks/templates/import"
	req := openapi.TaskTemplateRequest{Overwrite: false}
	result := testutil.NewRequest().Post(taskBatchImportURL).WithJsonBody(req).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusAccepted)
	var resp openapi.TaskTemplateResponse
	c.Assert(result.UnmarshalBodyToObject(&resp), check.IsNil)
	c.Assert(resp.SuccessTaskList, check.HasLen, 1)
	c.Assert(resp.SuccessTaskList[0], check.Equals, task.Name)
	c.Assert(resp.FailedTaskList, check.HasLen, 0)

	// import again without overwrite will fail
	result = testutil.NewRequest().Post(taskBatchImportURL).WithJsonBody(req).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusAccepted)
	c.Assert(result.UnmarshalBodyToObject(&resp), check.IsNil)
	c.Assert(resp.SuccessTaskList, check.HasLen, 0)
	c.Assert(resp.FailedTaskList, check.HasLen, 1)
	c.Assert(resp.FailedTaskList[0].TaskName, check.Equals, task.Name)

	// import again with overwrite will success
	req.Overwrite = true
	result = testutil.NewRequest().Post(taskBatchImportURL).WithJsonBody(req).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.UnmarshalBodyToObject(&resp), check.IsNil)
	c.Assert(resp.SuccessTaskList, check.HasLen, 1)
	c.Assert(resp.SuccessTaskList[0], check.Equals, task.Name)
	c.Assert(resp.FailedTaskList, check.HasLen, 0)
}

func (t *openAPISuite) testSourceOperationWithTask(c *check.C, source *openapi.Source, task *openapi.Task, s *Server) {
	source1URL := fmt.Sprintf("/api/v1/sources/%s", source.SourceName)
	disableSource1URL := fmt.Sprintf("%s/disable", source1URL)
	enableSource1URL := fmt.Sprintf("%s/enable", source1URL)
	transferSource1URL := fmt.Sprintf("%s/transfer", source1URL)

	// disable
	result := testutil.NewRequest().Post(disableSource1URL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(s.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect, check.Equals, pb.Stage_Stopped)

	// enable again
	result = testutil.NewRequest().Post(enableSource1URL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(s.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect, check.Equals, pb.Stage_Running)

	// test transfer failed,success transfer is tested in IT test
	req := openapi.WorkerNameRequest{WorkerName: "not exist"}
	result = testutil.NewRequest().Post(transferSource1URL).WithJsonBody(req).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusBadRequest)
	var resp openapi.ErrorWithMessage
	c.Assert(result.UnmarshalBodyToObject(&resp), check.IsNil)
	c.Assert(resp.ErrorCode, check.Equals, int(terror.ErrSchedulerWorkerNotExist.Code()))
}

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

// nolint:unparam
func mockPurgeRelay(mockWorkerClient *pbmock.MockWorkerClient) {
	resp := &pb.CommonWorkerResponse{Result: true}
	mockWorkerClient.EXPECT().PurgeRelay(gomock.Any(), gomock.Any()).Return(resp, nil).MaxTimes(maxRetryNum)
}

func mockTaskQueryStatus(
	mockWorkerClient *pbmock.MockWorkerClient, taskName, sourceName, workerName string, needError bool) {
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
