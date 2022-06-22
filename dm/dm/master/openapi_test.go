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

	"github.com/stretchr/testify/require"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/deepmap/oapi-codegen/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/tiflow/dm/checker"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/master/workerrpc"
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

func (t *openAPISuite) TestReverseRequestToLeader(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
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
	c.Assert(cfg2.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
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

	// list source from non-leader will get result too
	result2, err := HTTPTestWithTestResponseRecorder(testutil.NewRequest().Get(baseURL), s2.openapiHandles)
	c.Assert(err, check.IsNil)
	c.Assert(result2.Code(), check.Equals, http.StatusOK)
	var resultListSource2 openapi.GetSourceListResponse
	c.Assert(result2.UnmarshalBodyToObject(&resultListSource2), check.IsNil)
	c.Assert(resultListSource2.Data, check.HasLen, 0)
	c.Assert(resultListSource2.Total, check.Equals, 0)
}

func (t *openAPISuite) TestReverseRequestToHttpsLeader(c *check.C) {
	pwd, err := os.Getwd()
	require.NoError(t.testT, err)
	caPath := pwd + "/tls_for_test/ca.pem"
	certPath := pwd + "/tls_for_test/dm.pem"
	keyPath := pwd + "/tls_for_test/dm.key"

	// master1
	masterAddr1 := tempurl.Alloc()[len("http://"):]
	peerAddr1 := tempurl.Alloc()[len("http://"):]
	cfg1 := NewConfig()
	require.NoError(t.testT, cfg1.Parse([]string{
		"--name=dm-master-tls-1",
		fmt.Sprintf("--data-dir=%s", t.testT.TempDir()),
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
	require.NoError(t.testT, s1.Start(ctx1))
	defer func() {
		cancel1()
		s1.Close()
	}()
	// wait the first one become the leader
	require.True(t.testT, utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s1.election.IsLeader() && s1.scheduler.Started()
	}))

	// master2
	masterAddr2 := tempurl.Alloc()[len("http://"):]
	peerAddr2 := tempurl.Alloc()[len("http://"):]
	cfg2 := NewConfig()
	require.NoError(t.testT, cfg2.Parse([]string{
		"--name=dm-master-tls-2",
		fmt.Sprintf("--data-dir=%s", t.testT.TempDir()),
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
	require.NoError(t.testT, s2.Start(ctx2))
	defer func() {
		cancel2()
		s2.Close()
	}()
	// wait the second master ready
	require.False(t.testT, utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s2.election.IsLeader()
	}))

	baseURL := "/api/v1/sources"
	// list source from leader
	result := testutil.NewRequest().Get(baseURL).GoWithHTTPHandler(t.testT, s1.openapiHandles)
	require.Equal(t.testT, http.StatusOK, result.Code())
	var resultListSource openapi.GetSourceListResponse
	require.NoError(t.testT, result.UnmarshalBodyToObject(&resultListSource))
	require.Len(t.testT, resultListSource.Data, 0)
	require.Equal(t.testT, 0, resultListSource.Total)

	// with tls, list source not from leader will get result too
	result, err = HTTPTestWithTestResponseRecorder(testutil.NewRequest().Get(baseURL), s2.openapiHandles)
	require.NoError(t.testT, err)
	require.Equal(t.testT, http.StatusOK, result.Code())
	var resultListSource2 openapi.GetSourceListResponse
	require.NoError(t.testT, result.UnmarshalBodyToObject(&resultListSource2))
	require.Len(t.testT, resultListSource2.Data, 0)
	require.Equal(t.testT, 0, resultListSource2.Total)

	// without tls, list source not from leader will be 502
	require.NoError(t.testT, failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/MockNotSetTls", `return()`))
	result, err = HTTPTestWithTestResponseRecorder(testutil.NewRequest().Get(baseURL), s2.openapiHandles)
	require.NoError(t.testT, err)
	require.Equal(t.testT, http.StatusBadGateway, result.Code())
	require.NoError(t.testT, failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/MockNotSetTls"))
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

func (t *openAPISuite) TestOpenAPIWillNotStartInDefaultConfig(c *check.C) {
	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
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
	result := testutil.NewRequest().Post(baseURL).WithJsonBody(source1).GoWithHTTPHandler(t.testT, s.openapiHandles)
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
	result = testutil.NewRequest().Post(baseURL).WithJsonBody(source2).GoWithHTTPHandler(t.testT, s.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusBadRequest)
	var errResp openapi.ErrorWithMessage
	err = result.UnmarshalBodyToObject(&errResp)
	c.Assert(err, check.IsNil)
	c.Assert(errResp.ErrorCode, check.Equals, int(terror.ErrSchedulerSourceCfgExist.Code()))

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
	mockDB.ExpectQuery("SHOW TABLES FROM " + schemaName).WillReturnRows(sqlmock.NewRows([]string{"Tables_in_information_schema"}).AddRow(tableName))
	tableURL := fmt.Sprintf("%s/%s/schemas/%s", baseURL, source1.SourceName, schemaName)
	result = testutil.NewRequest().Get(tableURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var tableNameList openapi.TableNameList
	err = result.UnmarshalBodyToObject(&tableNameList)
	c.Assert(err, check.IsNil)
	c.Assert(tableNameList, check.HasLen, 1)
	c.Assert(tableNameList[0], check.Equals, tableName)
	c.Assert(mockDB.ExpectationsWereMet(), check.IsNil)

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
	cancel()
}

func (t *openAPISuite) TestRelayAPI(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	s := setupServer(ctx, c)
	ctrl := gomock.NewController(c)
	defer func() {
		cancel()
		s.Close()
		ctrl.Finish()
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
	result := testutil.NewRequest().Post(baseURL).WithJsonBody(source1).GoWithHTTPHandler(t.testT, s.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusCreated)

	// get source status
	source1StatusURL := fmt.Sprintf("%s/%s/status", baseURL, source1.SourceName)
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)

	var getSourceStatusResponse openapi.GetSourceStatusResponse
	err := result.UnmarshalBodyToObject(&getSourceStatusResponse)
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
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var getSourceStatusResponse2 openapi.GetSourceStatusResponse
	err = result.UnmarshalBodyToObject(&getSourceStatusResponse2)
	c.Assert(err, check.IsNil)
	c.Assert(getSourceStatusResponse2.Data[0].SourceName, check.Equals, source1.SourceName)
	c.Assert(getSourceStatusResponse2.Data[0].WorkerName, check.Equals, workerName1) // worker1 is bound
	c.Assert(getSourceStatusResponse2.Data[0].RelayStatus, check.IsNil)              // not start relay
	c.Assert(getSourceStatusResponse2.Total, check.Equals, 1)

	// list source with status
	result = testutil.NewRequest().Get(baseURL+"?with_status=true").GoWithHTTPHandler(t.testT, s.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultListSource openapi.GetSourceListResponse
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
	startRelayURL := fmt.Sprintf("%s/%s/start-relay", baseURL, source1.SourceName)
	openAPIStartRelayReq := openapi.StartRelayRequest{WorkerNameList: []string{workerName1}}
	result = testutil.NewRequest().Post(startRelayURL).WithJsonBody(openAPIStartRelayReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
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
	var getSourceStatusResponse3 openapi.GetSourceStatusResponse
	err = result.UnmarshalBodyToObject(&getSourceStatusResponse3)
	c.Assert(err, check.IsNil)
	c.Assert(getSourceStatusResponse3.Data[0].RelayStatus.Stage, check.Equals, pb.Stage_Running.String())

	// mock worker get status meet error
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_Paused)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again, error message should not be nil
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var getSourceStatusResponse4 openapi.GetSourceStatusResponse
	err = result.UnmarshalBodyToObject(&getSourceStatusResponse4)
	c.Assert(err, check.IsNil)
	c.Assert(*getSourceStatusResponse4.Data[0].ErrorMsg, check.Equals, "some error happened")
	c.Assert(getSourceStatusResponse4.Data[0].WorkerName, check.Equals, workerName1)

	// test pause relay
	pauseRelayURL := fmt.Sprintf("%s/%s/pause-relay", baseURL, source1.SourceName)
	result = testutil.NewRequest().Post(pauseRelayURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	relayWorkers, err = s.scheduler.GetRelayWorkers(source1Name)
	c.Assert(err, check.IsNil)
	c.Assert(relayWorkers, check.HasLen, 1) // pause relay will not affect relay workers

	// test resume relay
	resumeRelayURL := fmt.Sprintf("%s/%s/resume-relay", baseURL, source1.SourceName)
	result = testutil.NewRequest().Post(resumeRelayURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	relayWorkers, err = s.scheduler.GetRelayWorkers(source1Name)
	c.Assert(err, check.IsNil)
	c.Assert(relayWorkers, check.HasLen, 1) // pause relay will not affect relay workers

	// test stop relay
	stopRelayURL := fmt.Sprintf("%s/%s/stop-relay", baseURL, source1.SourceName)
	stopRelayReq := openapi.StopRelayRequest{WorkerNameList: []string{workerName1}}
	result = testutil.NewRequest().Post(stopRelayURL).WithJsonBody(stopRelayReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	relayWorkers, err = s.scheduler.GetRelayWorkers(source1Name)
	c.Assert(err, check.IsNil)
	c.Assert(relayWorkers, check.HasLen, 0)

	// mock worker get status relay already stopped
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_InvalidStage)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again,source
	result = testutil.NewRequest().Get(source1StatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)

	var getSourceStatusResponse5 openapi.GetSourceStatusResponse
	err = result.UnmarshalBodyToObject(&getSourceStatusResponse5)
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
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB", `return(true)`), check.IsNil)
	checker.CheckSyncConfigFunc = mockCheckSyncConfig
	ctrl := gomock.NewController(c)
	defer func() {
		checker.CheckSyncConfigFunc = checker.CheckSyncConfig
		cancel()
		s.Close()
		ctrl.Finish()
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
	result := testutil.NewRequest().Post(sourceURL).WithJsonBody(source1).GoWithHTTPHandler(t.testT, s.openapiHandles)
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
	result = testutil.NewRequest().Post(taskURL).WithJsonBody(createTaskReq).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusCreated)
	var createTaskResp openapi.Task
	err = result.UnmarshalBodyToObject(&createTaskResp)
	c.Assert(err, check.IsNil)
	c.Assert(task.Name, check.Equals, createTaskResp.Name)
	subTaskM := s.scheduler.GetSubTaskCfgsByTask(task.Name)
	c.Assert(len(subTaskM) == 1, check.IsTrue)
	c.Assert(subTaskM[source1Name].Name, check.Equals, task.Name)

	// list tasks
	result = testutil.NewRequest().Get(taskURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultTaskList openapi.GetTaskListResponse
	err = result.UnmarshalBodyToObject(&resultTaskList)
	c.Assert(err, check.IsNil)
	c.Assert(resultTaskList.Total, check.Equals, 1)
	c.Assert(resultTaskList.Data[0].Name, check.Equals, task.Name)

	// test batch import task config
	taskBatchImportURL := "/api/v1/task/configs/import"
	req := openapi.TaskConfigRequest{Overwrite: false}
	result = testutil.NewRequest().Post(taskBatchImportURL).WithJsonBody(req).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusAccepted)
	var resp openapi.TaskConfigResponse
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

	// pause and resume task
	pauseTaskURL := fmt.Sprintf("%s/%s/pause", taskURL, task.Name)
	result = testutil.NewRequest().Post(pauseTaskURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(s.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect, check.Equals, pb.Stage_Paused)

	resumeTaskURL := fmt.Sprintf("%s/%s/resume", taskURL, task.Name)
	result = testutil.NewRequest().Post(resumeTaskURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	c.Assert(s.scheduler.GetExpectSubTaskStage(task.Name, source1Name).Expect, check.Equals, pb.Stage_Running)

	// get task status
	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	mockTaskQueryStatus(mockWorkerClient, task.Name, source1.SourceName, workerName1)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	taskStatusURL := fmt.Sprintf("%s/%s/status", taskURL, task.Name)
	result = testutil.NewRequest().Get(taskStatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultTaskStatus openapi.GetTaskStatusResponse
	err = result.UnmarshalBodyToObject(&resultTaskStatus)
	c.Assert(err, check.IsNil)
	c.Assert(resultTaskStatus.Total, check.Equals, 1) // only 1 subtask
	c.Assert(resultTaskStatus.Data[0].Name, check.Equals, task.Name)
	c.Assert(resultTaskStatus.Data[0].Stage, check.Equals, pb.Stage_Running.String())
	c.Assert(resultTaskStatus.Data[0].WorkerName, check.Equals, workerName1)

	// get task status with source name
	taskStatusURL = fmt.Sprintf("%s/%s/status?source_name_list=%s", taskURL, task.Name, source1Name)
	result = testutil.NewRequest().Get(taskStatusURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultTaskStatusWithStatus openapi.GetTaskStatusResponse
	err = result.UnmarshalBodyToObject(&resultTaskStatusWithStatus)
	c.Assert(err, check.IsNil)
	c.Assert(resultTaskStatusWithStatus, check.DeepEquals, resultTaskStatus)

	// stop task
	result = testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", taskURL, task.Name)).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusNoContent)
	subTaskM = s.scheduler.GetSubTaskCfgsByTask(task.Name)
	c.Assert(len(subTaskM) == 0, check.IsTrue)
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB"), check.IsNil)

	// list tasks
	result = testutil.NewRequest().Get(taskURL).GoWithHTTPHandler(t.testT, s.openapiHandles)
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultTaskList2 openapi.GetTaskListResponse
	err = result.UnmarshalBodyToObject(&resultTaskList2)
	c.Assert(err, check.IsNil)
	c.Assert(resultTaskList2.Total, check.Equals, 0)
}

func (t *openAPISuite) TestClusterAPI(c *check.C) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	s1 := setupServer(ctx1, c)
	defer func() {
		cancel1()
		s1.Close()
	}()

	// join a new master node to an existing cluster
	cfg2 := NewConfig()
	c.Assert(cfg2.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
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
		s2.Close()
		cancel2()
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

func (t *openAPISuite) TestTaskConfigsAPI(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	s := setupServer(ctx, c)
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
	// create source
	sourceURL := "/api/v1/sources"
	result := testutil.NewRequest().Post(sourceURL).WithJsonBody(source1).GoWithHTTPHandler(t.testT, s.openapiHandles)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusCreated)

	// create task config template
	url := "/api/v1/task/configs"

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

func setupServer(ctx context.Context, c *check.C) *Server {
	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.AdvertiseAddr = cfg1.MasterAddr
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)
	cfg1.OpenAPI = true

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

func mockTaskQueryStatus(
	mockWorkerClient *pbmock.MockWorkerClient, taskName, sourceName, workerName string) {
	queryResp := &pb.QueryStatusResponse{
		Result: true,
		SourceStatus: &pb.SourceStatus{
			Worker: workerName,
			Source: sourceName,
		},
		SubTaskStatus: []*pb.SubTaskStatus{
			{
				Stage: pb.Stage_Running,
				Name:  taskName,
				Status: &pb.SubTaskStatus_Sync{
					Sync: &pb.SyncStatus{
						TotalEvents:         0,
						TotalTps:            0,
						RecentTps:           0,
						MasterBinlog:        "",
						MasterBinlogGtid:    "",
						SyncerBinlog:        "",
						SyncerBinlogGtid:    "",
						BlockingDDLs:        nil,
						UnresolvedGroups:    nil,
						Synced:              false,
						BinlogType:          "",
						SecondsBehindMaster: 0,
					},
				},
			},
		},
	}
	mockWorkerClient.EXPECT().QueryStatus(
		gomock.Any(),
		&pb.QueryStatusRequest{Name: taskName},
	).Return(queryResp, nil).MaxTimes(maxRetryNum)
}

func mockCheckSyncConfig(ctx context.Context, cfgs []*config.SubTaskConfig, errCnt, warnCnt int64) error {
	return nil
}
