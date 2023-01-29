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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/deepmap/oapi-codegen/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/checker"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/openapi/fixtures"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pbmock"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/tempurl"
)

// some data for test.
var (
	source1Name = "mysql-replica-01"
)

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
}

func (s *OpenAPIViewSuite) SetupTest() {
	checker.CheckSyncConfigFunc = mockCheckSyncConfig
	CheckAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
	s.NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/master/MockSkipAdjustTargetDB", `return(true)`))
	s.NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/master/MockSkipRemoveMetaData", `return(true)`))
}

func (s *OpenAPIViewSuite) TearDownTest() {
	checker.CheckSyncConfigFunc = checker.CheckSyncConfig
	CheckAndAdjustSourceConfigFunc = checkAndAdjustSourceConfig
	s.NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/master/MockSkipAdjustTargetDB"))
	s.NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/master/MockSkipRemoveMetaData"))
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

func (s *OpenAPIViewSuite) TestReverseRequestToLeader() {
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

	// list source from non-leader will get result too
	result, err := HTTPTestWithTestResponseRecorder(testutil.NewRequest().Get(baseURL), s2.openapiHandles)
	s.NoError(err)
	s.Equal(http.StatusOK, result.Code())
	var resultListSource2 openapi.GetSourceListResponse
	s.NoError(result.UnmarshalBodyToObject(&resultListSource2))
	s.Len(resultListSource2.Data, 0)
	s.Equal(0, resultListSource2.Total)
}

func (s *OpenAPIViewSuite) TestReverseRequestToHttpsLeader() {
	pwd2, err := os.Getwd()
	require.NoError(s.T(), err)
	caPath := pwd2 + "/tls_for_test/ca.pem"
	certPath := pwd2 + "/tls_for_test/dm.pem"
	keyPath := pwd2 + "/tls_for_test/dm.key"

	// master1
	masterAddr1 := tempurl.Alloc()[len("http://"):]
	peerAddr1 := tempurl.Alloc()[len("http://"):]
	cfg1 := NewConfig()
	require.NoError(s.T(), cfg1.Parse([]string{
		"--name=dm-master-tls-1",
		fmt.Sprintf("--data-dir=%s", s.T().TempDir()),
		fmt.Sprintf("--master-addr=https://%s", masterAddr1),
		fmt.Sprintf("--advertise-addr=https://%s", masterAddr1),
		fmt.Sprintf("--peer-urls=https://%s", peerAddr1),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr1),
		fmt.Sprintf("--initial-cluster=dm-master-tls-1=https://%s", peerAddr1),
		"--ssl-ca=" + caPath,
		"--ssl-cert=" + certPath,
		"--ssl-key=" + keyPath,
	}))
	cfg1.OpenAPI = true
	s1 := NewServer(cfg1)
	ctx1, cancel1 := context.WithCancel(context.Background())
	require.NoError(s.T(), s1.Start(ctx1))
	defer func() {
		cancel1()
		s1.Close()
	}()
	// wait the first one become the leader
	require.True(s.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s1.election.IsLeader() && s1.scheduler.Started()
	}))

	// master2
	masterAddr2 := tempurl.Alloc()[len("http://"):]
	peerAddr2 := tempurl.Alloc()[len("http://"):]
	cfg2 := NewConfig()
	require.NoError(s.T(), cfg2.Parse([]string{
		"--name=dm-master-tls-2",
		fmt.Sprintf("--data-dir=%s", s.T().TempDir()),
		fmt.Sprintf("--master-addr=https://%s", masterAddr2),
		fmt.Sprintf("--advertise-addr=https://%s", masterAddr2),
		fmt.Sprintf("--peer-urls=https://%s", peerAddr2),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr2),
		"--ssl-ca=" + caPath,
		"--ssl-cert=" + certPath,
		"--ssl-key=" + keyPath,
	}))
	cfg2.OpenAPI = true
	cfg2.Join = s1.cfg.MasterAddr // join to an existing cluster
	s2 := NewServer(cfg2)
	ctx2, cancel2 := context.WithCancel(context.Background())
	require.NoError(s.T(), s2.Start(ctx2))
	defer func() {
		cancel2()
		s2.Close()
	}()
	// wait the second master ready
	require.False(s.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
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

	// with tls, list source not from leader will get result too
	result, err = HTTPTestWithTestResponseRecorder(testutil.NewRequest().Get(baseURL), s2.openapiHandles)
	s.NoError(err)
	s.Equal(http.StatusOK, result.Code())
	var resultListSource2 openapi.GetSourceListResponse
	s.NoError(result.UnmarshalBodyToObject(&resultListSource2))
	s.Len(resultListSource2.Data, 0)
	s.Equal(0, resultListSource2.Total)

	// without tls, list source not from leader will be 502
	s.NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/master/MockNotSetTls", `return()`))
	result, err = HTTPTestWithTestResponseRecorder(testutil.NewRequest().Get(baseURL), s2.openapiHandles)
	s.NoError(err)
	s.Equal(http.StatusBadGateway, result.Code())
	s.NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/master/MockNotSetTls"))
}

// httptest.ResponseRecorder is not http.CloseNotifier, will panic when test reverse proxy.
// We need to implement the interface ourselves.
// ref: https://github.com/gin-gonic/gin/blob/ce20f107f5dc498ec7489d7739541a25dcd48463/context_test.go#L1747-L1765
type TestResponseRecorder struct {
	*httptest.ResponseRecorder
	closeChannel chan bool
}

func (r *TestResponseRecorder) CloseNotify() <-chan bool {
	return r.closeChannel
}

func CreateTestResponseRecorder() *TestResponseRecorder {
	return &TestResponseRecorder{
		httptest.NewRecorder(),
		make(chan bool, 1),
	}
}

func HTTPTestWithTestResponseRecorder(r *testutil.RequestBuilder, handler http.Handler) (*testutil.CompletedRequest, error) {
	if r == nil {
		return nil, nil
	}
	if r.Error != nil {
		return nil, r.Error
	}
	var bodyReader io.Reader
	if r.Body != nil {
		bodyReader = bytes.NewReader(r.Body)
	}

	req := httptest.NewRequest(r.Method, r.Path, bodyReader)
	for h, v := range r.Headers {
		req.Header.Add(h, v)
	}
	if host, ok := r.Headers["Host"]; ok {
		req.Host = host
	}
	for _, c := range r.Cookies {
		req.AddCookie(c)
	}

	rec := CreateTestResponseRecorder()
	handler.ServeHTTP(rec, req)

	return &testutil.CompletedRequest{
		Recorder: rec.ResponseRecorder,
	}, nil
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
		Password:   &dbCfg.Password,
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
		Password:   &dbCfg.Password,
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
	mockDB.ExpectQuery("SHOW FULL TABLES IN `information_schema` WHERE Table_Type != 'VIEW';").WillReturnRows(
		sqlmock.NewRows([]string{"Tables_in_information_schema", "Table_type"}).AddRow(tableName, "BASE TABLE"))
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

	// create with no password
	sourceNoPassword := source1
	sourceNoPassword.Password = nil
	createReqNoPassword := openapi.CreateSourceRequest{Source: sourceNoPassword}
	result = testutil.NewRequest().Post(baseURL).WithJsonBody(createReqNoPassword).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusCreated, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&resultSource))
	s.Nil(resultSource.Password)

	// update to have password
	sourceHasPassword := source1
	updateReqHasPassword := openapi.UpdateSourceRequest{Source: sourceHasPassword}
	result = testutil.NewRequest().Put(source1URL).WithJsonBody(updateReqHasPassword).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&source1FromHTTP))
	s.Equal(source1FromHTTP.Password, sourceHasPassword.Password)

	// update with no password, will use old password
	updateReqNoPassword := openapi.UpdateSourceRequest{Source: sourceNoPassword}
	result = testutil.NewRequest().Put(source1URL).WithJsonBody(updateReqNoPassword).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&source1FromHTTP))
	s.Nil(source1FromHTTP.Password)
	// password is old
	conf := s1.scheduler.GetSourceCfgByID(source1FromHTTP.SourceName)
	s.NotNil(conf)
	s.Equal(*sourceHasPassword.Password, conf.From.Password)

	// delete source with --force
	result = testutil.NewRequest().Delete(fmt.Sprintf("%s/%s?force=true", baseURL, source1.SourceName)).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusNoContent, result.Code())
}

func (s *OpenAPIViewSuite) testImportTaskTemplate(task *openapi.Task, s1 *Server) {
	// test batch import task config
	taskBatchImportURL := "/api/v1/tasks/templates/import"
	req := openapi.TaskTemplateRequest{Overwrite: false}
	result := testutil.NewRequest().Post(taskBatchImportURL).WithJsonBody(req).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusAccepted, result.Code())
	var resp openapi.TaskTemplateResponse
	s.NoError(result.UnmarshalBodyToObject(&resp))
	s.Len(resp.SuccessTaskList, 1)
	s.Equal(task.Name, resp.SuccessTaskList[0])
	s.Len(resp.FailedTaskList, 0)

	// import again without overwrite will fail
	result = testutil.NewRequest().Post(taskBatchImportURL).WithJsonBody(req).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusAccepted, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&resp))
	s.Len(resp.SuccessTaskList, 0)
	s.Len(resp.FailedTaskList, 1)
	s.Equal(task.Name, resp.FailedTaskList[0].TaskName)

	// import again with overwrite will success
	req.Overwrite = true
	result = testutil.NewRequest().Post(taskBatchImportURL).WithJsonBody(req).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.NoError(result.UnmarshalBodyToObject(&resp))
	s.Len(resp.SuccessTaskList, 1)
	s.Equal(task.Name, resp.SuccessTaskList[0])
	s.Len(resp.FailedTaskList, 0)
}

func (s *OpenAPIViewSuite) testSourceOperationWithTask(source *openapi.Source, task *openapi.Task, s1 *Server) {
	source1URL := fmt.Sprintf("/api/v1/sources/%s", source.SourceName)
	disableSource1URL := fmt.Sprintf("%s/disable", source1URL)
	enableSource1URL := fmt.Sprintf("%s/enable", source1URL)
	transferSource1URL := fmt.Sprintf("%s/transfer", source1URL)

	// disable
	result := testutil.NewRequest().Post(disableSource1URL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.Equal(pb.Stage_Stopped, s1.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect)

	// enable again
	result = testutil.NewRequest().Post(enableSource1URL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.Equal(pb.Stage_Running, s1.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect)

	// test transfer failed,success transfer is tested in IT test
	req := openapi.WorkerNameRequest{WorkerName: "not exist"}
	result = testutil.NewRequest().Post(transferSource1URL).WithJsonBody(req).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusBadRequest, result.Code())
	var resp openapi.ErrorWithMessage
	s.NoError(result.UnmarshalBodyToObject(&resp))
	s.Equal(int(terror.ErrSchedulerWorkerNotExist.Code()), resp.ErrorCode)
}

func (s *OpenAPIViewSuite) TestTaskAPI() {
	ctx, cancel := context.WithCancel(context.Background())
	s1 := setupTestServer(ctx, s.T())
	ctrl := gomock.NewController(s.T())
	defer func() {
		cancel()
		s1.Close()
		ctrl.Finish()
	}()

	dbCfg := config.GetDBConfigForTest()
	source1 := openapi.Source{
		Enable:     true,
		SourceName: source1Name,
		EnableGtid: false,
		Host:       dbCfg.Host,
		Password:   &dbCfg.Password,
		Port:       dbCfg.Port,
		User:       dbCfg.User,
	}
	// create source
	sourceURL := "/api/v1/sources"
	createSourceReq := openapi.CreateSourceRequest{Source: source1}
	result := testutil.NewRequest().Post(sourceURL).WithJsonBody(createSourceReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	// check http status code
	s.Equal(http.StatusCreated, result.Code())

	// add mock worker  start workers, the unbounded sources should be bounded
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	workerName1 := "worker-1"
	s.NoError(s1.scheduler.AddWorker(workerName1, "172.16.10.72:8262"))
	go func(ctx context.Context, workerName string) {
		s.NoError(ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL))
	}(ctx1, workerName1)
	// wait worker ready
	s.True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		w := s1.scheduler.GetWorkerBySource(source1.SourceName)
		return w != nil
	}), true)

	// create task
	taskURL := "/api/v1/tasks"

	task, err := fixtures.GenNoShardOpenAPITaskForTest()
	s.NoError(err)
	// use a valid target db
	task.TargetConfig.Host = dbCfg.Host
	task.TargetConfig.Port = dbCfg.Port
	task.TargetConfig.User = dbCfg.User
	task.TargetConfig.Password = dbCfg.Password

	// create task
	createTaskReq := openapi.CreateTaskRequest{Task: task}
	result = testutil.NewRequest().Post(taskURL).WithJsonBody(createTaskReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusCreated, result.Code())
	var createTaskResp openapi.OperateTaskResponse
	s.NoError(result.UnmarshalBodyToObject(&createTaskResp))
	s.Equal(createTaskResp.Task.Name, task.Name)
	subTaskM := s1.scheduler.GetSubTaskCfgsByTask(task.Name)
	s.Len(subTaskM, 1)
	s.Equal(task.Name, subTaskM[source1Name].Name)

	// get task
	task1URL := fmt.Sprintf("%s/%s", taskURL, task.Name)
	var task1FromHTTP openapi.Task
	result = testutil.NewRequest().Get(task1URL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&task1FromHTTP))
	s.Equal(task1FromHTTP.Name, task.Name)

	// update a task
	s.NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/operateCheckSubtasksCanUpdate", `return("success")`))
	clone := task
	batch := 1000
	clone.SourceConfig.IncrMigrateConf.ReplBatch = &batch
	updateReq := openapi.UpdateTaskRequest{Task: clone}
	result = testutil.NewRequest().Put(task1URL).WithJsonBody(updateReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var updateResp openapi.OperateTaskResponse
	s.NoError(result.UnmarshalBodyToObject(&updateResp))
	s.EqualValues(updateResp.Task.SourceConfig.IncrMigrateConf.ReplBatch, clone.SourceConfig.IncrMigrateConf.ReplBatch)
	s.NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/operateCheckSubtasksCanUpdate"))

	// list tasks
	result = testutil.NewRequest().Get(taskURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var resultTaskList openapi.GetTaskListResponse
	s.NoError(result.UnmarshalBodyToObject(&resultTaskList))
	s.Equal(1, resultTaskList.Total)
	s.Equal(task.Name, resultTaskList.Data[0].Name)

	s.testImportTaskTemplate(&task, s1)

	// start task
	startTaskURL := fmt.Sprintf("%s/%s/start", taskURL, task.Name)
	result = testutil.NewRequest().Post(startTaskURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.Equal(pb.Stage_Running, s1.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect)

	// get task status
	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	mockTaskQueryStatus(mockWorkerClient, task.Name, source1.SourceName, workerName1, false)
	s1.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	taskStatusURL := fmt.Sprintf("%s/%s/status", taskURL, task.Name)
	result = testutil.NewRequest().Get(taskStatusURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var resultTaskStatus openapi.GetTaskStatusResponse
	s.NoError(result.UnmarshalBodyToObject(&resultTaskStatus))
	s.Equal(1, resultTaskStatus.Total) // only 1 subtask
	s.Equal(task.Name, resultTaskStatus.Data[0].Name)
	s.Equal(openapi.TaskStageRunning, resultTaskStatus.Data[0].Stage)
	s.Equal(workerName1, resultTaskStatus.Data[0].WorkerName)
	s.Equal(float64(0), resultTaskStatus.Data[0].DumpStatus.CompletedTables)
	s.Equal(int64(1), resultTaskStatus.Data[0].DumpStatus.TotalTables)
	s.Equal(float64(10), resultTaskStatus.Data[0].DumpStatus.EstimateTotalRows)

	// get task status with source name
	taskStatusURL = fmt.Sprintf("%s/%s/status?source_name_list=%s", taskURL, task.Name, source1Name)
	result = testutil.NewRequest().Get(taskStatusURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var resultTaskStatusWithStatus openapi.GetTaskStatusResponse
	s.NoError(result.UnmarshalBodyToObject(&resultTaskStatusWithStatus))
	s.EqualValues(resultTaskStatus, resultTaskStatusWithStatus)

	// list task with status
	result = testutil.NewRequest().Get(taskURL+"?with_status=true").GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	var resultListTask openapi.GetTaskListResponse
	s.NoError(result.UnmarshalBodyToObject(&resultListTask))
	s.Len(resultListTask.Data, 1)
	s.Equal(1, resultListTask.Total)
	s.NotNil(resultListTask.Data[0].StatusList)
	statusList := *resultListTask.Data[0].StatusList
	status := statusList[0]
	s.Equal(workerName1, status.WorkerName)
	s.Equal(task.Name, status.Name)

	// list with filter
	result = testutil.NewRequest().Get(taskURL+"?stage=Stopped").GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	resultListTask = openapi.GetTaskListResponse{} // reset
	s.NoError(result.UnmarshalBodyToObject(&resultListTask))
	s.Len(resultListTask.Data, 0)

	result = testutil.NewRequest().Get(taskURL+"?stage=Running").GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	resultListTask = openapi.GetTaskListResponse{} // reset
	s.NoError(result.UnmarshalBodyToObject(&resultListTask))
	s.Len(resultListTask.Data, 1)

	result = testutil.NewRequest().Get(taskURL+"?stage=Running&source_name_list=notsource").GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	resultListTask = openapi.GetTaskListResponse{} // reset
	s.NoError(result.UnmarshalBodyToObject(&resultListTask))
	s.Len(resultListTask.Data, 0)

	result = testutil.NewRequest().Get(taskURL+"?stage=Running&source_name_list="+source1Name).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	resultListTask = openapi.GetTaskListResponse{} // reset
	s.NoError(result.UnmarshalBodyToObject(&resultListTask))
	s.Len(resultListTask.Data, 1)

	// get task with status
	result = testutil.NewRequest().Get(task1URL+"?with_status=true").GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&task1FromHTTP))
	s.Equal(task1FromHTTP.Name, task.Name)
	statusList = *task1FromHTTP.StatusList
	s.Len(statusList, 1)
	s.Equal(workerName1, statusList[0].WorkerName)
	s.Equal(task.Name, statusList[0].Name)

	// test some error happened on worker
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockTaskQueryStatus(mockWorkerClient, task.Name, source1.SourceName, workerName1, true)
	s1.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	result = testutil.NewRequest().Get(taskURL+"?with_status=true").GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&resultListTask))
	s.Len(resultListTask.Data, 1)
	s.Equal(1, resultListTask.Total)
	s.NotNil(resultListTask.Data[0].StatusList)
	statusList = *resultListTask.Data[0].StatusList
	s.Len(statusList, 1)
	status = statusList[0]
	s.NotNil(status.ErrorMsg)

	// test convertTaskConfig
	convertReq := openapi.ConverterTaskRequest{}
	convertResp := openapi.ConverterTaskResponse{}
	convertURL := fmt.Sprintf("%s/%s", taskURL, "converters")
	result = testutil.NewRequest().Post(convertURL).WithJsonBody(convertReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusBadRequest, result.Code()) // not valid req

	// from task to taskConfig
	convertReq.Task = &task
	result = testutil.NewRequest().Post(convertURL).WithJsonBody(convertReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&convertResp))
	s.NotNil(convertResp.Task)
	s.NotNil(convertResp.TaskConfigFile)
	taskConfigFile := convertResp.TaskConfigFile

	// from taskCfg to task
	convertReq.Task = nil
	convertReq.TaskConfigFile = &taskConfigFile
	result = testutil.NewRequest().Post(convertURL).WithJsonBody(convertReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.NoError(result.UnmarshalBodyToObject(&convertResp))
	s.NotNil(convertResp.Task)
	s.NotNil(convertResp.TaskConfigFile)
	taskConfigFile2 := convertResp.TaskConfigFile
	s.Equal(taskConfigFile2, taskConfigFile)

	s.testSourceOperationWithTask(&source1, &task, s1)

	// stop task
	stopTaskURL := fmt.Sprintf("%s/%s/stop", taskURL, task.Name)
	stopTaskReq := openapi.StopTaskRequest{}
	result = testutil.NewRequest().Post(stopTaskURL).WithJsonBody(stopTaskReq).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	s.Equal(pb.Stage_Stopped, s1.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect)

	// delete task
	result = testutil.NewRequest().Delete(task1URL+"?force=true").GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusNoContent, result.Code())
	subTaskM = s1.scheduler.GetSubTaskCfgsByTask(task.Name)
	s.Len(subTaskM, 0)

	// list tasks
	result = testutil.NewRequest().Get(taskURL).GoWithHTTPHandler(s.T(), s1.openapiHandles)
	s.Equal(http.StatusOK, result.Code())
	resultListTask = openapi.GetTaskListResponse{} // reset
	s.NoError(result.UnmarshalBodyToObject(&resultTaskList))
	s.Equal(0, resultTaskList.Total)
}

func TestOpenAPIViewSuite(t *testing.T) {
	suite.Run(t, new(OpenAPIViewSuite))
}
