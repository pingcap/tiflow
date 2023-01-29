// Copyright 2019 PingCAP, Inc.
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

package master

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	toolutils "github.com/pingcap/tidb/util"
	tidbmock "github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tiflow/dm/checker"
	common2 "github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/master/scheduler"
	"github.com/pingcap/tiflow/dm/master/shardddl"
	"github.com/pingcap/tiflow/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/openapi/fixtures"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pbmock"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/tempurl"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/verify"
	"go.etcd.io/etcd/tests/v3/integration"
	"google.golang.org/grpc"
)

// use task config from integration test `sharding`.
var taskConfig = `---
name: test
task-mode: all
is-sharding: true
shard-mode: ""
meta-schema: "dm_meta"
enable-heartbeat: true
ignore-checking-items: ["all"]

target-database:
  host: "127.0.0.1"
  port: 4000
  user: "root"
  password: ""

mysql-instances:
  - source-id: "mysql-replica-01"
    block-allow-list:  "instance"
    route-rules: ["sharding-route-rules-table", "sharding-route-rules-schema"]
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

  - source-id: "mysql-replica-02"
    block-allow-list:  "instance"
    route-rules: ["sharding-route-rules-table", "sharding-route-rules-schema"]
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

block-allow-list:
  instance:
    do-dbs: ["~^sharding[\\d]+"]
    do-tables:
    -  db-name: "~^sharding[\\d]+"
       tbl-name: "~^t[\\d]+"

routes:
  sharding-route-rules-table:
    schema-pattern: sharding*
    table-pattern: t*
    target-schema: db_target
    target-table: t_target

  sharding-route-rules-schema:
    schema-pattern: sharding*
    target-schema: db_target

mydumpers:
  global:
    threads: 4
    chunk-filesize: 64
    skip-tz-utc: true
    extra-args: "--regex '^sharding.*'"

loaders:
  global:
    pool-size: 16
    dir: "./dumped_data"

syncers:
  global:
    worker-count: 16
    batch: 100
`

var (
	errGRPCFailed         = "test grpc request failed"
	errGRPCFailedReg      = fmt.Sprintf("(?m).*%s.*", errGRPCFailed)
	errCheckSyncConfig    = "(?m).*check sync config with error.*"
	errCheckSyncConfigReg = fmt.Sprintf("(?m).*%s.*", errCheckSyncConfig)
	keepAliveTTL          = int64(10)
)

type testMasterSuite struct {
	suite.Suite

	workerClients     map[string]workerrpc.Client
	saveMaxRetryNum   int
	electionTTLBackup int

	testEtcdCluster *integration.ClusterV3
	etcdTestCli     *clientv3.Client
}

func TestMasterSuite(t *testing.T) {
	suite.Run(t, new(testMasterSuite))
}

var pwd string

func (t *testMasterSuite) SetupSuite() {
	require.NoError(t.T(), log.InitLogger(&log.Config{}))
	var err error
	pwd, err = os.Getwd()
	require.NoError(t.T(), err)
	integration.BeforeTestExternal(t.T())
	t.workerClients = make(map[string]workerrpc.Client)
	t.saveMaxRetryNum = maxRetryNum
	t.electionTTLBackup = electionTTL
	electionTTL = 3
	maxRetryNum = 2
	checkAndAdjustSourceConfigForDMCtlFunc = checkAndNoAdjustSourceConfigMock
}

func (t *testMasterSuite) TearDownSuite() {
	maxRetryNum = t.saveMaxRetryNum
	electionTTL = t.electionTTLBackup
	checkAndAdjustSourceConfigForDMCtlFunc = checkAndAdjustSourceConfig
}

func (t *testMasterSuite) SetupTest() {
	t.testEtcdCluster = integration.NewClusterV3(t.T(), &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.testEtcdCluster.RandClient()
	t.clearEtcdEnv()
}

func (t *testMasterSuite) TearDownTest() {
	t.clearEtcdEnv()
	t.testEtcdCluster.Terminate(t.T())
}

func (t *testMasterSuite) clearEtcdEnv() {
	require.NoError(t.T(), ha.ClearTestInfoOperation(t.etcdTestCli))
}

func (t *testMasterSuite) clearSchedulerEnv(cancel context.CancelFunc, wg *sync.WaitGroup) {
	cancel()
	wg.Wait()
	t.clearEtcdEnv()
}

func stageDeepEqualExcludeRev(t *testing.T, stage, expectStage ha.Stage) {
	t.Helper()

	expectStage.Revision = stage.Revision
	require.Equal(t, expectStage, stage)
}

func mockRevelantWorkerClient(mockWorkerClient *pbmock.MockWorkerClient, taskName, sourceID string, masterReq interface{}) {
	var expect pb.Stage
	switch req := masterReq.(type) {
	case *pb.OperateSourceRequest:
		switch req.Op {
		case pb.SourceOp_StartSource, pb.SourceOp_UpdateSource:
			expect = pb.Stage_Running
		case pb.SourceOp_StopSource:
			expect = pb.Stage_Stopped
		}
	case *pb.StartTaskRequest, *pb.UpdateTaskRequest:
		expect = pb.Stage_Running
	case *pb.OperateTaskRequest:
		switch req.Op {
		case pb.TaskOp_Resume:
			expect = pb.Stage_Running
		case pb.TaskOp_Pause:
			expect = pb.Stage_Paused
		case pb.TaskOp_Delete:
		}
	case *pb.OperateWorkerRelayRequest:
		switch req.Op {
		case pb.RelayOp_ResumeRelay:
			expect = pb.Stage_Running
		case pb.RelayOp_PauseRelay:
			expect = pb.Stage_Paused
		case pb.RelayOp_StopRelay:
			expect = pb.Stage_Stopped
		}
	}
	queryResp := &pb.QueryStatusResponse{
		Result:       true,
		SourceStatus: &pb.SourceStatus{},
	}

	switch masterReq.(type) {
	case *pb.OperateSourceRequest:
		switch expect {
		case pb.Stage_Running:
			queryResp.SourceStatus = &pb.SourceStatus{Source: sourceID}
		case pb.Stage_Stopped:
			queryResp.SourceStatus = &pb.SourceStatus{Source: ""}
		}
	case *pb.StartTaskRequest, *pb.UpdateTaskRequest, *pb.OperateTaskRequest:
		queryResp.SubTaskStatus = []*pb.SubTaskStatus{{}}
		if opTaskReq, ok := masterReq.(*pb.OperateTaskRequest); ok && opTaskReq.Op == pb.TaskOp_Delete {
			queryResp.SubTaskStatus[0].Status = &pb.SubTaskStatus_Msg{
				Msg: fmt.Sprintf("no sub task with name %s has started", taskName),
			}
		} else {
			queryResp.SubTaskStatus[0].Name = taskName
			queryResp.SubTaskStatus[0].Stage = expect
		}
	case *pb.OperateWorkerRelayRequest:
		queryResp.SourceStatus = &pb.SourceStatus{RelayStatus: &pb.RelayStatus{Stage: expect}}
	}

	mockWorkerClient.EXPECT().QueryStatus(
		gomock.Any(),
		&pb.QueryStatusRequest{
			Name: taskName,
		},
	).Return(queryResp, nil).MaxTimes(maxRetryNum)
}

func createTableInfo(t *testing.T, p *parser.Parser, se sessionctx.Context, tableID int64, sql string) *model.TableInfo {
	t.Helper()

	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	require.NoError(t, err)
	createStmtNode, ok := node.(*ast.CreateTableStmt)
	require.True(t, ok, "%s is not a CREATE TABLE statement", sql)
	info, err := tiddl.MockTableInfo(se, createStmtNode, tableID)
	require.NoError(t, err)
	return info
}

func newMockRPCClient(client pb.WorkerClient) workerrpc.Client {
	c, _ := workerrpc.NewGRPCClientWrap(nil, client)
	return c
}

func defaultWorkerSource() ([]string, []string) {
	return []string{
			"mysql-replica-01",
			"mysql-replica-02",
		}, []string{
			"127.0.0.1:8262",
			"127.0.0.1:8263",
		}
}

func makeNilWorkerClients(workers []string) map[string]workerrpc.Client {
	nilWorkerClients := make(map[string]workerrpc.Client, len(workers))
	for _, worker := range workers {
		nilWorkerClients[worker] = nil
	}
	return nilWorkerClients
}

func makeWorkerClientsForHandle(ctrl *gomock.Controller, taskName string, sources []string, workers []string, reqs ...interface{}) map[string]workerrpc.Client {
	workerClients := make(map[string]workerrpc.Client, len(workers))
	for i := range workers {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		for _, req := range reqs {
			mockRevelantWorkerClient(mockWorkerClient, taskName, sources[i], req)
		}
		workerClients[workers[i]] = newMockRPCClient(mockWorkerClient)
	}
	return workerClients
}

func testDefaultMasterServer(t *testing.T) *Server {
	t.Helper()

	cfg := NewConfig()
	err := cfg.FromContent(SampleConfig)
	require.NoError(t, err)
	cfg.DataDir = t.TempDir()
	server := NewServer(cfg)
	server.leader.Store(oneselfLeader)
	go server.ap.Start(context.Background())

	return server
}

func (t *testMasterSuite) testMockScheduler(
	ctx context.Context,
	wg *sync.WaitGroup,
	sources, workers []string,
	password string,
	workerClients map[string]workerrpc.Client,
) (*scheduler.Scheduler, []context.CancelFunc) {
	logger := log.L()
	scheduler2 := scheduler.NewScheduler(&logger, security.Security{})
	err := scheduler2.Start(ctx, t.etcdTestCli)
	require.NoError(t.T(), err)
	cancels := make([]context.CancelFunc, 0, 2)
	for i := range workers {
		// add worker to scheduler's workers map
		name := workers[i]
		require.NoError(t.T(), scheduler2.AddWorker(name, workers[i]))
		scheduler2.SetWorkerClientForTest(name, workerClients[workers[i]])
		// operate mysql config on this worker
		cfg := config.NewSourceConfig()
		cfg.SourceID = sources[i]
		cfg.From.Password = password
		require.NoError(t.T(), scheduler2.AddSourceCfg(cfg))
		wg.Add(1)
		ctx1, cancel1 := context.WithCancel(ctx)
		cancels = append(cancels, cancel1)
		go func(ctx context.Context, workerName string) {
			defer wg.Done()
			require.NoError(t.T(), ha.KeepAlive(ctx, t.etcdTestCli, workerName, keepAliveTTL))
		}(ctx1, name)
		idx := i
		require.Eventually(t.T(), func() bool {
			w := scheduler2.GetWorkerBySource(sources[idx])
			return w != nil && w.BaseInfo().Name == name
		}, 3*time.Second, 100*time.Millisecond)
	}
	return scheduler2, cancels
}

func (t *testMasterSuite) testMockSchedulerForRelay(
	ctx context.Context,
	wg *sync.WaitGroup,
	sources, workers []string,
	password string,
	workerClients map[string]workerrpc.Client,
) (*scheduler.Scheduler, []context.CancelFunc) {
	logger := log.L()
	scheduler2 := scheduler.NewScheduler(&logger, security.Security{})
	err := scheduler2.Start(ctx, t.etcdTestCli)
	require.NoError(t.T(), err)
	cancels := make([]context.CancelFunc, 0, 2)
	for i := range workers {
		// add worker to scheduler's workers map
		name := workers[i]
		require.NoError(t.T(), scheduler2.AddWorker(name, workers[i]))
		scheduler2.SetWorkerClientForTest(name, workerClients[workers[i]])
		// operate mysql config on this worker
		cfg := config.NewSourceConfig()
		cfg.SourceID = sources[i]
		cfg.From.Password = password
		require.NoError(t.T(), scheduler2.AddSourceCfg(cfg))
		wg.Add(1)
		ctx1, cancel1 := context.WithCancel(ctx)
		cancels = append(cancels, cancel1)
		go func(ctx context.Context, workerName string) {
			defer wg.Done()
			require.NoError(t.T(), ha.KeepAlive(ctx, t.etcdTestCli, workerName, keepAliveTTL))
		}(ctx1, name)

		// wait the mock worker has alive
		require.Eventually(t.T(), func() bool {
			resp, err2 := t.etcdTestCli.Get(ctx, common2.WorkerKeepAliveKeyAdapter.Encode(name))
			require.NoError(t.T(), err2)
			return resp.Count == 1
		}, 3*time.Second, 100*time.Millisecond)

		require.NoError(t.T(), scheduler2.StartRelay(sources[i], []string{workers[i]}))
		idx := i
		require.Eventually(t.T(), func() bool {
			relayWorkers, err2 := scheduler2.GetRelayWorkers(sources[idx])
			require.NoError(t.T(), err2)
			return len(relayWorkers) == 1 && relayWorkers[0].BaseInfo().Name == name
		}, 3*time.Second, 100*time.Millisecond)
	}
	return scheduler2, cancels
}

func generateServerConfig(t *testing.T, name string) *Config {
	t.Helper()

	// create a new cluster
	cfg1 := NewConfig()
	err := cfg1.FromContent(SampleConfig)
	require.NoError(t, err)
	cfg1.Name = name
	cfg1.DataDir = t.TempDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.AdvertiseAddr = cfg1.MasterAddr
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)
	return cfg1
}

func (t *testMasterSuite) TestQueryStatus() {
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()

	server := testDefaultMasterServer(t.T())
	sources, workers := defaultWorkerSource()
	var cancels []context.CancelFunc

	// test query all workers
	for _, worker := range workers {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{
			Result:       true,
			SourceStatus: &pb.SourceStatus{},
		}, nil)
		t.workerClients[worker] = newMockRPCClient(mockWorkerClient)
	}
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	server.scheduler, cancels = t.testMockScheduler(ctx, &wg, sources, workers, "", t.workerClients)
	for _, cancelFunc := range cancels {
		defer cancelFunc()
	}
	resp, err := server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{})
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	t.clearSchedulerEnv(cancel, &wg)

	// query specified sources
	for _, worker := range workers {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{
			Result:       true,
			SourceStatus: &pb.SourceStatus{},
		}, nil)
		t.workerClients[worker] = newMockRPCClient(mockWorkerClient)
	}
	ctx, cancel = context.WithCancel(context.Background())
	server.scheduler, cancels = t.testMockSchedulerForRelay(ctx, &wg, sources, workers, "passwd", t.workerClients)
	for _, cancelFunc := range cancels {
		defer cancelFunc()
	}
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Sources: sources,
	})
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)

	// query with invalid dm-worker[s]
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Sources: []string{"invalid-source1", "invalid-source2"},
	})
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Regexp(t.T(), "sources .* haven't been added", resp.Msg)

	// query with invalid task name
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Name: "invalid-task-name",
	})
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Regexp(t.T(), "task .* has no source or not exist", resp.Msg)
	t.clearSchedulerEnv(cancel, &wg)
	// TODO: test query with correct task name, this needs to add task first
}

func (t *testMasterSuite) TestWaitOperationOkRightResult() {
	cases := []struct {
		req              interface{}
		resp             *pb.QueryStatusResponse
		expectedOK       bool
		expectedEmptyMsg bool
	}{
		{
			&pb.OperateTaskRequest{
				Op:   pb.TaskOp_Pause,
				Name: "task-unittest",
			},
			&pb.QueryStatusResponse{
				SubTaskStatus: []*pb.SubTaskStatus{
					{Stage: pb.Stage_Paused},
				},
			},
			true,
			true,
		},
		{
			&pb.OperateTaskRequest{
				Op:   pb.TaskOp_Pause,
				Name: "task-unittest",
			},
			&pb.QueryStatusResponse{
				SubTaskStatus: []*pb.SubTaskStatus{
					{
						Stage:  pb.Stage_Paused,
						Result: &pb.ProcessResult{Errors: []*pb.ProcessError{{Message: "paused by previous error"}}},
					},
				},
			},
			true,
			false,
		},
	}

	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()
	ctx := context.Background()
	duration, _ := time.ParseDuration("1s")
	s := &Server{cfg: &Config{RPCTimeout: duration}}
	for _, ca := range cases {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			gomock.Any(),
		).Return(ca.resp, nil)
		mockWorker := scheduler.NewMockWorker(newMockRPCClient(mockWorkerClient))

		ok, msg, _, err := s.waitOperationOk(ctx, mockWorker, "", "", ca.req)
		require.NoError(t.T(), err)
		require.Equal(t.T(), ca.expectedOK, ok)
		if ca.expectedEmptyMsg {
			require.Empty(t.T(), msg)
		} else {
			require.NotEmpty(t.T(), msg)
		}
	}
}

func (t *testMasterSuite) TestStopTaskWithExceptRight() {
	taskName := "test-stop-task"
	responeses := [][]*pb.QueryStatusResponse{{
		&pb.QueryStatusResponse{
			SubTaskStatus: []*pb.SubTaskStatus{
				{
					Name: taskName,
					Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{
						UnresolvedGroups: []*pb.ShardingGroup{{Target: "`db`.`tbl`", Unsynced: []string{"table1"}}},
					}},
				},
			},
		},
		&pb.QueryStatusResponse{SubTaskStatus: []*pb.SubTaskStatus{}},
	}, {
		&pb.QueryStatusResponse{
			SubTaskStatus: []*pb.SubTaskStatus{
				{
					Name: taskName,
					Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{
						UnresolvedGroups: []*pb.ShardingGroup{{Target: "`db`.`tbl`", Unsynced: []string{"table1"}}},
					}},
				},
			},
		},
		&pb.QueryStatusResponse{SubTaskStatus: []*pb.SubTaskStatus{
			{
				Name:   taskName,
				Status: &pb.SubTaskStatus_Msg{Msg: common2.NoSubTaskMsg(taskName)},
			},
		}},
	}}
	req := &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Delete,
		Name: taskName,
	}
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()
	ctx := context.Background()
	s := &Server{cfg: &Config{RPCTimeout: time.Second}}

	for _, item := range responeses {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			gomock.Any(),
		).Return(item[0], nil).Return(item[1], nil).MaxTimes(2)
		mockWorker := scheduler.NewMockWorker(newMockRPCClient(mockWorkerClient))
		ok, msg, _, err := s.waitOperationOk(ctx, mockWorker, taskName, "", req)
		require.NoError(t.T(), err)
		require.True(t.T(), ok)
		require.Empty(t.T(), msg)
	}
}

func (t *testMasterSuite) TestFillUnsyncedStatus() {
	var (
		logger  = log.L()
		task1   = "task1"
		task2   = "task2"
		source1 = "source1"
		source2 = "source2"
		sources = []string{source1, source2}
	)
	cases := []struct {
		infos    []pessimism.Info
		input    []*pb.QueryStatusResponse
		expected []*pb.QueryStatusResponse
	}{
		// test it could work
		{
			[]pessimism.Info{
				{
					Task:   task1,
					Source: source1,
					Schema: "db",
					Table:  "tbl",
				},
			},
			[]*pb.QueryStatusResponse{
				{
					SourceStatus: &pb.SourceStatus{
						Source: source1,
					},
					SubTaskStatus: []*pb.SubTaskStatus{
						{
							Name: task1,
							Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{
								UnresolvedGroups: []*pb.ShardingGroup{{Target: "`db`.`tbl`", Unsynced: []string{"table1"}}},
							}},
						},
						{
							Name:   task2,
							Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{}},
						},
					},
				}, {
					SourceStatus: &pb.SourceStatus{
						Source: source2,
					},
					SubTaskStatus: []*pb.SubTaskStatus{
						{
							Name:   task1,
							Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{}},
						},
						{
							Name:   task2,
							Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{}},
						},
					},
				},
			},
			[]*pb.QueryStatusResponse{
				{
					SourceStatus: &pb.SourceStatus{
						Source: source1,
					},
					SubTaskStatus: []*pb.SubTaskStatus{
						{
							Name: task1,
							Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{
								UnresolvedGroups: []*pb.ShardingGroup{{Target: "`db`.`tbl`", Unsynced: []string{"table1"}}},
							}},
						},
						{
							Name:   task2,
							Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{}},
						},
					},
				}, {
					SourceStatus: &pb.SourceStatus{
						Source: source2,
					},
					SubTaskStatus: []*pb.SubTaskStatus{
						{
							Name: task1,
							Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{
								UnresolvedGroups: []*pb.ShardingGroup{{Target: "`db`.`tbl`", Unsynced: []string{"this DM-worker doesn't receive any shard DDL of this group"}}},
							}},
						},
						{
							Name:   task2,
							Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{}},
						},
					},
				},
			},
		},
		// test won't interfere not sync status
		{
			[]pessimism.Info{
				{
					Task:   task1,
					Source: source1,
					Schema: "db",
					Table:  "tbl",
				},
			},
			[]*pb.QueryStatusResponse{
				{
					SourceStatus: &pb.SourceStatus{
						Source: source1,
					},
					SubTaskStatus: []*pb.SubTaskStatus{
						{
							Name: task1,
							Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{
								UnresolvedGroups: []*pb.ShardingGroup{{Target: "`db`.`tbl`", Unsynced: []string{"table1"}}},
							}},
						},
					},
				}, {
					SourceStatus: &pb.SourceStatus{
						Source: source2,
					},
					SubTaskStatus: []*pb.SubTaskStatus{
						{
							Name:   task1,
							Status: &pb.SubTaskStatus_Load{Load: &pb.LoadStatus{}},
						},
					},
				},
			},
			[]*pb.QueryStatusResponse{
				{
					SourceStatus: &pb.SourceStatus{
						Source: source1,
					},
					SubTaskStatus: []*pb.SubTaskStatus{
						{
							Name: task1,
							Status: &pb.SubTaskStatus_Sync{Sync: &pb.SyncStatus{
								UnresolvedGroups: []*pb.ShardingGroup{{Target: "`db`.`tbl`", Unsynced: []string{"table1"}}},
							}},
						},
					},
				}, {
					SourceStatus: &pb.SourceStatus{
						Source: source2,
					},
					SubTaskStatus: []*pb.SubTaskStatus{
						{
							Name:   task1,
							Status: &pb.SubTaskStatus_Load{Load: &pb.LoadStatus{}},
						},
					},
				},
			},
		},
	}

	// test pessimistic mode
	for _, ca := range cases {
		s := &Server{}
		s.pessimist = shardddl.NewPessimist(&logger, func(task string) []string { return sources })
		require.NoError(t.T(), s.pessimist.Start(context.Background(), t.etcdTestCli))
		for _, i := range ca.infos {
			_, err := pessimism.PutInfo(t.etcdTestCli, i)
			require.NoError(t.T(), err)
		}
		if len(ca.infos) > 0 {
			utils.WaitSomething(20, 100*time.Millisecond, func() bool {
				return len(s.pessimist.ShowLocks("", nil)) > 0
			})
		}

		s.fillUnsyncedStatus(ca.input)
		require.Equal(t.T(), ca.expected, ca.input)
		_, err := pessimism.DeleteInfosOperations(t.etcdTestCli, ca.infos, nil)
		require.NoError(t.T(), err)
	}
}

func (t *testMasterSuite) TestCheckTask() {
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()

	server := testDefaultMasterServer(t.T())
	sources, workers := defaultWorkerSource()

	t.workerClients = makeNilWorkerClients(workers)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "", t.workerClients)
	mock := conn.InitVersionDB()
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err := server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)

	// decode task with error
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: "invalid toml config",
	})
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	t.clearSchedulerEnv(cancel, &wg)

	// simulate invalid password returned from scheduler, but config was supported plaintext mysql password, so cfg.SubTaskConfigs will success
	ctx, cancel = context.WithCancel(context.Background())
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "invalid-encrypt-password", t.workerClients)
	mock = conn.InitVersionDB()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	t.clearSchedulerEnv(cancel, &wg)
}

func (t *testMasterSuite) TestStartTask() {
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()

	server := testDefaultMasterServer(t.T())
	server.etcdClient = t.etcdTestCli
	sources, workers := defaultWorkerSource()

	// s.generateSubTask with error
	resp, err := server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task: "invalid toml config",
	})
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)

	// test start task successfully
	var wg sync.WaitGroup
	// taskName is relative to taskConfig
	taskName := "test"
	ctx, cancel := context.WithCancel(context.Background())
	req := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, req))
	mock := conn.InitVersionDB()
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err = server.StartTask(context.Background(), req)
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	for _, source := range sources {
		t.subTaskStageMatch(server.scheduler, taskName, source, pb.Stage_Running)
		tcm, _, err2 := ha.GetSubTaskCfg(t.etcdTestCli, source, taskName, 0)
		require.NoError(t.T(), err2)
		require.Contains(t.T(), tcm, taskName)
		require.Equal(t.T(), taskName, tcm[taskName].Name)
		require.Equal(t.T(), source, tcm[taskName].SourceID)
	}

	// check start-task with an invalid source
	invalidSource := "invalid-source"
	mock = conn.InitVersionDB()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: []string{invalidSource},
	})
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Len(t.T(), resp.Sources, 1)
	require.False(t.T(), resp.Sources[0].Result)
	require.Equal(t.T(), invalidSource, resp.Sources[0].Source)

	// test start task, but the first step check-task fails
	bakCheckSyncConfigFunc := checker.CheckSyncConfigFunc
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*config.SubTaskConfig, _, _ int64) (string, error) {
		return "", errors.New(errCheckSyncConfig)
	}
	defer func() {
		checker.CheckSyncConfigFunc = bakCheckSyncConfigFunc
	}()
	mock = conn.InitVersionDB()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	})
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Regexp(t.T(), errCheckSyncConfigReg, resp.CheckResult)
	t.clearSchedulerEnv(cancel, &wg)
}

func (t *testMasterSuite) TestStartTaskWithRemoveMeta() {
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()

	server := testDefaultMasterServer(t.T())
	sources, workers := defaultWorkerSource()
	server.etcdClient = t.etcdTestCli

	// test start task successfully
	var wg sync.WaitGroup
	// taskName is relative to taskConfig
	cfg := config.NewTaskConfig()
	err := cfg.Decode(taskConfig)
	require.NoError(t.T(), err)
	taskName := cfg.Name
	ctx, cancel := context.WithCancel(context.Background())
	logger := log.L()

	// test remove meta with pessimist
	cfg.ShardMode = config.ShardPessimistic
	req := &pb.StartTaskRequest{
		Task:       strings.ReplaceAll(taskConfig, `shard-mode: ""`, fmt.Sprintf(`shard-mode: "%s"`, cfg.ShardMode)),
		Sources:    sources,
		RemoveMeta: true,
	}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, req))
	server.pessimist = shardddl.NewPessimist(&logger, func(task string) []string { return sources })
	server.optimist = shardddl.NewOptimist(&logger, server.scheduler.GetDownstreamMetaByTask)

	var (
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		schema, table = "foo", "bar"
		ID            = fmt.Sprintf("%s-`%s`.`%s`", taskName, schema, table)
		i11           = pessimism.NewInfo(taskName, sources[0], schema, table, DDLs)
		op2           = pessimism.NewOperation(ID, taskName, sources[0], DDLs, true, false)
	)
	_, err = pessimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)
	_, succ, err := pessimism.PutOperations(t.etcdTestCli, false, op2)
	require.True(t.T(), succ)
	require.NoError(t.T(), err)

	require.NoError(t.T(), server.pessimist.Start(ctx, t.etcdTestCli))
	require.NoError(t.T(), server.optimist.Start(ctx, t.etcdTestCli))

	verMock := conn.InitVersionDB()
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	verMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	mock, err := conn.MockDefaultDBProvider()
	require.NoError(t.T(), err)
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.LoaderCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.LightningCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerShardMeta(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.ValidatorCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.ValidatorPendingChange(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.ValidatorErrorChange(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.ValidatorTableStatus(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", loader.GetTaskInfoSchemaName(cfg.MetaSchema, cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	require.Greater(t.T(), len(server.pessimist.Locks()), 0)

	resp, err := server.StartTask(context.Background(), req)
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Microsecond)
		// start another same task at the same time, should get err
		verMock2 := conn.InitVersionDB()
		verMock2.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("version", "5.7.25-TiDB-v4.0.2"))
		resp1, err1 := server.StartTask(context.Background(), req)
		require.NoError(t.T(), err1)
		require.False(t.T(), resp1.Result)
		require.Equal(t.T(), terror.Annotate(terror.ErrSchedulerSubTaskExist.Generate(cfg.Name, sources),
			"while remove-meta is true").Error(), resp1.Msg)
	}()
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result, "start task failed: %s", resp.Msg)
	for _, source := range sources {
		t.subTaskStageMatch(server.scheduler, taskName, source, pb.Stage_Running)
		tcm, _, err2 := ha.GetSubTaskCfg(t.etcdTestCli, source, taskName, 0)
		require.NoError(t.T(), err2)
		require.Contains(t.T(), tcm, taskName)
		require.Equal(t.T(), taskName, tcm[taskName].Name)
		require.Equal(t.T(), source, tcm[taskName].SourceID)
	}

	require.Len(t.T(), server.pessimist.Locks(), 0)
	require.NoError(t.T(), mock.ExpectationsWereMet())
	ifm, _, err := pessimism.GetAllInfo(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Len(t.T(), ifm, 0)
	opm, _, err := pessimism.GetAllOperations(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Len(t.T(), opm, 0)
	t.clearSchedulerEnv(cancel, &wg)

	// test remove meta with optimist
	ctx, cancel = context.WithCancel(context.Background())
	cfg.ShardMode = config.ShardOptimistic
	req = &pb.StartTaskRequest{
		Task:       strings.ReplaceAll(taskConfig, `shard-mode: ""`, fmt.Sprintf(`shard-mode: "%s"`, cfg.ShardMode)),
		Sources:    sources,
		RemoveMeta: true,
	}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, req))
	server.pessimist = shardddl.NewPessimist(&logger, func(task string) []string { return sources })
	server.optimist = shardddl.NewOptimist(&logger, server.scheduler.GetDownstreamMetaByTask)

	var (
		p           = parser.New()
		se          = tidbmock.NewContext()
		tblID int64 = 111

		st1      = optimism.NewSourceTables(taskName, sources[0])
		DDLs1    = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		tiBefore = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter1 = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		info1    = optimism.NewInfo(taskName, sources[0], "foo-1", "bar-1", schema, table, DDLs1, tiBefore, []*model.TableInfo{tiAfter1})
		op1      = optimism.NewOperation(ID, taskName, sources[0], info1.UpSchema, info1.UpTable, DDLs1, optimism.ConflictNone, "", false, []string{})
	)

	st1.AddTable("foo-1", "bar-1", schema, table)
	_, err = optimism.PutSourceTables(t.etcdTestCli, st1)
	require.NoError(t.T(), err)
	_, err = optimism.PutInfo(t.etcdTestCli, info1)
	require.NoError(t.T(), err)
	_, succ, err = optimism.PutOperation(t.etcdTestCli, false, op1, 0)
	require.True(t.T(), succ)
	require.NoError(t.T(), err)

	err = server.pessimist.Start(ctx, t.etcdTestCli)
	require.NoError(t.T(), err)
	err = server.optimist.Start(ctx, t.etcdTestCli)
	require.NoError(t.T(), err)

	verMock = conn.InitVersionDB()
	verMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	mock, err = conn.MockDefaultDBProvider()
	require.NoError(t.T(), err)
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.LoaderCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.LightningCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerShardMeta(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.ValidatorCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.ValidatorPendingChange(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.ValidatorErrorChange(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.ValidatorTableStatus(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", loader.GetTaskInfoSchemaName(cfg.MetaSchema, cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	require.Greater(t.T(), len(server.optimist.Locks()), 0)

	resp, err = server.StartTask(context.Background(), req)
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Microsecond)
		// start another same task at the same time, should get err
		vermock2 := conn.InitVersionDB()
		vermock2.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("version", "5.7.25-TiDB-v4.0.2"))
		resp1, err1 := server.StartTask(context.Background(), req)
		require.NoError(t.T(), err1)
		require.False(t.T(), resp1.Result)
		require.Equal(t.T(), terror.Annotate(terror.ErrSchedulerSubTaskExist.Generate(cfg.Name, sources),
			"while remove-meta is true").Error(), resp1.Msg)
	}()
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	for _, source := range sources {
		t.subTaskStageMatch(server.scheduler, taskName, source, pb.Stage_Running)
		tcm, _, err2 := ha.GetSubTaskCfg(t.etcdTestCli, source, taskName, 0)
		require.NoError(t.T(), err2)
		require.Contains(t.T(), tcm, taskName)
		require.Equal(t.T(), taskName, tcm[taskName].Name)
		require.Equal(t.T(), source, tcm[taskName].SourceID)
	}

	require.Len(t.T(), server.optimist.Locks(), 0)
	require.NoError(t.T(), mock.ExpectationsWereMet())
	ifm2, _, err := optimism.GetAllInfo(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Len(t.T(), ifm2, 0)
	opm2, _, err := optimism.GetAllOperations(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Len(t.T(), opm2, 0)
	tbm, _, err := optimism.GetAllSourceTables(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Len(t.T(), tbm, 0)

	t.clearSchedulerEnv(cancel, &wg)
}

func (t *testMasterSuite) TestOperateTask() {
	var (
		taskName = "unit-test-task"
		pauseOp  = pb.TaskOp_Pause
	)

	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()
	server := testDefaultMasterServer(t.T())
	server.etcdClient = t.etcdTestCli
	sources, workers := defaultWorkerSource()

	// test operate-task with invalid task name
	resp, err := server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pauseOp,
		Name: taskName,
	})
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Equal(t.T(), fmt.Sprintf("task %s has no source or not exist, please check the task name and status", taskName), resp.Msg)

	// 1. start task
	taskName = "test"
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	startReq := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	pauseReq := &pb.OperateTaskRequest{
		Op:   pauseOp,
		Name: taskName,
	}
	resumeReq := &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Resume,
		Name: taskName,
	}
	stopReq1 := &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Delete,
		Name:    taskName,
		Sources: []string{sources[0]},
	}
	stopReq2 := &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Delete,
		Name: taskName,
	}
	sourceResps := []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}, {Result: true, Source: sources[1]}}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, startReq, pauseReq, resumeReq, stopReq1, stopReq2))
	mock := conn.InitVersionDB()
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	stResp, err := server.StartTask(context.Background(), startReq)
	require.NoError(t.T(), err)
	require.True(t.T(), stResp.Result)
	for _, source := range sources {
		t.subTaskStageMatch(server.scheduler, taskName, source, pb.Stage_Running)
	}

	require.Equal(t.T(), sourceResps, stResp.Sources)
	// 2. pause task
	resp, err = server.OperateTask(context.Background(), pauseReq)
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	for _, source := range sources {
		t.subTaskStageMatch(server.scheduler, taskName, source, pb.Stage_Paused)
	}

	require.Equal(t.T(), sourceResps, resp.Sources)
	// 3. resume task
	resp, err = server.OperateTask(context.Background(), resumeReq)
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	for _, source := range sources {
		t.subTaskStageMatch(server.scheduler, taskName, source, pb.Stage_Running)
	}
	require.Equal(t.T(), sourceResps, resp.Sources)
	// 4. test stop task successfully, remove partial sources
	resp, err = server.OperateTask(context.Background(), stopReq1)
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	require.Equal(t.T(), []string{sources[1]}, server.getTaskSourceNameList(taskName))
	require.Equal(t.T(), []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}}, resp.Sources)
	// 5. test stop task successfully, remove all workers
	resp, err = server.OperateTask(context.Background(), stopReq2)
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	require.Len(t.T(), server.getTaskSourceNameList(taskName), 0)
	require.Equal(t.T(), []*pb.CommonWorkerResponse{{Result: true, Source: sources[1]}}, resp.Sources)
	t.clearSchedulerEnv(cancel, &wg)
}

func (t *testMasterSuite) TestPurgeWorkerRelay() {
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()

	server := testDefaultMasterServer(t.T())
	sources, workers := defaultWorkerSource()
	var (
		now      = time.Now().Unix()
		filename = "mysql-bin.000005"
	)

	// mock PurgeRelay request
	mockPurgeRelay := func(rpcSuccess bool) {
		for i, worker := range workers {
			rets := []interface{}{
				nil,
				errors.New(errGRPCFailed),
			}
			if rpcSuccess {
				rets = []interface{}{
					&pb.CommonWorkerResponse{
						Result: true,
						Source: sources[i],
					},
					nil,
				}
			}
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
			mockWorkerClient.EXPECT().PurgeRelay(
				gomock.Any(),
				&pb.PurgeRelayRequest{
					Time:     now,
					Filename: filename,
				},
			).Return(rets...)
			t.workerClients[worker] = newMockRPCClient(mockWorkerClient)
		}
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	server.scheduler, _ = t.testMockSchedulerForRelay(ctx, &wg, nil, nil, "", t.workerClients)

	// test PurgeWorkerRelay with invalid dm-worker[s]
	resp, err := server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Sources:  []string{"invalid-source1", "invalid-source2"},
		Time:     now,
		Filename: filename,
	})
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	require.Len(t.T(), resp.Sources, 2)
	for _, w := range resp.Sources {
		require.False(t.T(), w.Result)
		require.Regexp(t.T(), "relay worker for source .* not found.*", w.Msg)
	}
	t.clearSchedulerEnv(cancel, &wg)

	ctx, cancel = context.WithCancel(context.Background())
	// test PurgeWorkerRelay successfully
	mockPurgeRelay(true)
	server.scheduler, _ = t.testMockSchedulerForRelay(ctx, &wg, sources, workers, "", t.workerClients)
	resp, err = server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Sources:  sources,
		Time:     now,
		Filename: filename,
	})
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	require.Len(t.T(), resp.Sources, 2)
	for _, w := range resp.Sources {
		require.True(t.T(), w.Result)
	}
	t.clearSchedulerEnv(cancel, &wg)

	ctx, cancel = context.WithCancel(context.Background())
	// test PurgeWorkerRelay with error response
	mockPurgeRelay(false)
	server.scheduler, _ = t.testMockSchedulerForRelay(ctx, &wg, sources, workers, "", t.workerClients)
	resp, err = server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Sources:  sources,
		Time:     now,
		Filename: filename,
	})
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	require.Len(t.T(), resp.Sources, 2)
	for _, w := range resp.Sources {
		require.False(t.T(), w.Result)
		require.Regexp(t.T(), errGRPCFailedReg, w.Msg)
	}
	t.clearSchedulerEnv(cancel, &wg)
}

func (t *testMasterSuite) TestOperateWorkerRelayTask() {
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()

	server := testDefaultMasterServer(t.T())
	sources, workers := defaultWorkerSource()
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	pauseReq := &pb.OperateWorkerRelayRequest{
		Sources: sources,
		Op:      pb.RelayOp_PauseRelay,
	}
	resumeReq := &pb.OperateWorkerRelayRequest{
		Sources: sources,
		Op:      pb.RelayOp_ResumeRelay,
	}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, "", sources, workers, pauseReq, resumeReq))

	// test OperateWorkerRelayTask with invalid dm-worker[s]
	resp, err := server.OperateWorkerRelayTask(context.Background(), &pb.OperateWorkerRelayRequest{
		Sources: []string{"invalid-source1", "invalid-source2"},
		Op:      pb.RelayOp_PauseRelay,
	})
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Contains(t.T(), resp.Msg, "need to update expectant relay stage not exist")

	sourceResps := []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}, {Result: true, Source: sources[1]}}
	// 1. test pause-relay successfully
	resp, err = server.OperateWorkerRelayTask(context.Background(), pauseReq)
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	for _, source := range sources {
		t.relayStageMatch(server.scheduler, source, pb.Stage_Paused)
	}
	require.Equal(t.T(), sourceResps, resp.Sources)
	// 2. test resume-relay successfully
	resp, err = server.OperateWorkerRelayTask(context.Background(), resumeReq)
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	for _, source := range sources {
		t.relayStageMatch(server.scheduler, source, pb.Stage_Running)
	}
	require.Equal(t.T(), sourceResps, resp.Sources)
	t.clearSchedulerEnv(cancel, &wg)
}

func (t *testMasterSuite) TestServer() {
	var err error
	cfg := NewConfig()
	require.NoError(t.T(), cfg.FromContent(SampleConfig))
	cfg.PeerUrls = "http://127.0.0.1:8294"
	cfg.DataDir = t.T().TempDir()
	cfg.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg.AdvertiseAddr = cfg.MasterAddr

	basicServiceCheck := func(cfg *Config) {
		t.testHTTPInterface(fmt.Sprintf("http://%s/status", cfg.AdvertiseAddr), []byte(version.GetRawInfo()))
		t.testHTTPInterface(fmt.Sprintf("http://%s/debug/pprof/", cfg.AdvertiseAddr), []byte("Types of profiles available"))
		// HTTP API in this unit test is unstable, but we test it in `http_apis` in integration test.
		// t.testHTTPInterface( fmt.Sprintf("http://%s/apis/v1alpha1/status/test-task", cfg.AdvertiseAddr), []byte("task test-task has no source or not exist"))
	}
	t.testNormalServerLifecycle(cfg, func(cfg *Config) {
		basicServiceCheck(cfg)

		// try to start another server with the same address.  Expect it to fail
		// unset an etcd variable because it will cause checking on exit, and block forever
		err = os.Unsetenv(verify.ENV_VERIFY)
		require.NoError(t.T(), err)

		dupServer := NewServer(cfg)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err1 := dupServer.Start(ctx)
		require.True(t.T(), terror.ErrMasterStartEmbedEtcdFail.Equal(err1))
		require.Contains(t.T(), err1.Error(), "bind: address already in use")

		err = os.Setenv(verify.ENV_VERIFY, verify.ENV_VERIFY_ALL_VALUE)
		require.NoError(t.T(), err)
	})

	// test the listen address is 0.0.0.0
	masterAddrStr := tempurl.Alloc()[len("http://"):]
	_, masterPort, err := net.SplitHostPort(masterAddrStr)
	require.NoError(t.T(), err)
	cfg2 := NewConfig()
	*cfg2 = *cfg
	cfg2.MasterAddr = fmt.Sprintf("0.0.0.0:%s", masterPort)
	cfg2.AdvertiseAddr = masterAddrStr
	t.testNormalServerLifecycle(cfg2, basicServiceCheck)
}

func (t *testMasterSuite) TestMasterTLS() {
	var err error
	masterAddr := tempurl.Alloc()[len("http://"):]
	peerAddr := tempurl.Alloc()[len("http://"):]
	_, masterPort, err := net.SplitHostPort(masterAddr)
	require.NoError(t.T(), err)
	_, peerPort, err := net.SplitHostPort(peerAddr)
	require.NoError(t.T(), err)

	caPath := pwd + "/tls_for_test/ca.pem"
	certPath := pwd + "/tls_for_test/dm.pem"
	keyPath := pwd + "/tls_for_test/dm.key"

	// all with `https://` prefix
	cfg := NewConfig()
	err = cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", t.T().TempDir()),
		fmt.Sprintf("--master-addr=https://%s", masterAddr),
		fmt.Sprintf("--advertise-addr=https://%s", masterAddr),
		fmt.Sprintf("--peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=https://%s", peerAddr),
		"--ssl-ca=" + caPath,
		"--ssl-cert=" + certPath,
		"--ssl-key=" + keyPath,
	})
	require.NoError(t.T(), err)
	t.testTLSPrefix(cfg)
	require.Equal(t.T(), masterAddr, cfg.MasterAddr)
	require.Equal(t.T(), masterAddr, cfg.AdvertiseAddr)
	require.Equal(t.T(), "https://"+peerAddr, cfg.PeerUrls)
	require.Equal(t.T(), "https://"+peerAddr, cfg.AdvertisePeerUrls)
	require.Equal(t.T(), "master-tls=https://"+peerAddr, cfg.InitialCluster)

	// no `https://` prefix for `--master-addr`
	cfg = NewConfig()
	err = cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", t.T().TempDir()),
		fmt.Sprintf("--master-addr=%s", masterAddr),
		fmt.Sprintf("--advertise-addr=https://%s", masterAddr),
		fmt.Sprintf("--peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=https://%s", peerAddr),
		"--ssl-ca=" + caPath,
		"--ssl-cert=" + certPath,
		"--ssl-key=" + keyPath,
	})
	require.NoError(t.T(), err)
	t.testTLSPrefix(cfg)

	// no `https://` prefix for `--master-addr` and `--advertise-addr`
	cfg = NewConfig()
	err = cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", t.T().TempDir()),
		fmt.Sprintf("--master-addr=%s", masterAddr),
		fmt.Sprintf("--advertise-addr=%s", masterAddr),
		fmt.Sprintf("--peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=https://%s", peerAddr),
		"--ssl-ca=" + caPath,
		"--ssl-cert=" + certPath,
		"--ssl-key=" + keyPath,
	})
	require.NoError(t.T(), err)
	t.testTLSPrefix(cfg)

	// no `https://` prefix for `--master-addr`, `--advertise-addr` and `--peer-urls`
	cfg = NewConfig()
	err = cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", t.T().TempDir()),
		fmt.Sprintf("--master-addr=%s", masterAddr),
		fmt.Sprintf("--advertise-addr=%s", masterAddr),
		fmt.Sprintf("--peer-urls=%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=https://%s", peerAddr),
		"--ssl-ca=" + caPath,
		"--ssl-cert=" + certPath,
		"--ssl-key=" + keyPath,
	})
	require.NoError(t.T(), err)
	t.testTLSPrefix(cfg)

	// no `https://` prefix for `--master-addr`, `--advertise-addr`, `--peer-urls` and `--advertise-peer-urls`
	cfg = NewConfig()
	err = cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", t.T().TempDir()),
		fmt.Sprintf("--master-addr=%s", masterAddr),
		fmt.Sprintf("--advertise-addr=%s", masterAddr),
		fmt.Sprintf("--peer-urls=%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=https://%s", peerAddr),
		"--ssl-ca=" + caPath,
		"--ssl-cert=" + certPath,
		"--ssl-key=" + keyPath,
	})
	require.NoError(t.T(), err)
	t.testTLSPrefix(cfg)

	// all without `https://`/`http://` prefix
	cfg = NewConfig()
	err = cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", t.T().TempDir()),
		fmt.Sprintf("--master-addr=%s", masterAddr),
		fmt.Sprintf("--advertise-addr=%s", masterAddr),
		fmt.Sprintf("--peer-urls=%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=%s", peerAddr),
		"--ssl-ca=" + caPath,
		"--ssl-cert=" + certPath,
		"--ssl-key=" + keyPath,
	})
	require.NoError(t.T(), err)
	t.testTLSPrefix(cfg)
	require.Equal(t.T(), masterAddr, cfg.MasterAddr)
	require.Equal(t.T(), masterAddr, cfg.AdvertiseAddr)
	require.Equal(t.T(), "https://"+peerAddr, cfg.PeerUrls)
	require.Equal(t.T(), "https://"+peerAddr, cfg.AdvertisePeerUrls)
	require.Equal(t.T(), "master-tls=https://"+peerAddr, cfg.InitialCluster)

	// all with `http://` prefix, but with TLS enabled.
	cfg = NewConfig()
	err = cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", t.T().TempDir()),
		fmt.Sprintf("--master-addr=http://%s", masterAddr),
		fmt.Sprintf("--advertise-addr=http://%s", masterAddr),
		fmt.Sprintf("--peer-urls=http://%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=http://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=http://%s", peerAddr),
		"--ssl-ca=" + caPath,
		"--ssl-cert=" + certPath,
		"--ssl-key=" + keyPath,
	})
	require.NoError(t.T(), err)
	require.Equal(t.T(), masterAddr, cfg.MasterAddr)
	require.Equal(t.T(), masterAddr, cfg.AdvertiseAddr)
	require.Equal(t.T(), "https://"+peerAddr, cfg.PeerUrls)
	require.Equal(t.T(), "https://"+peerAddr, cfg.AdvertisePeerUrls)
	require.Equal(t.T(), "master-tls=https://"+peerAddr, cfg.InitialCluster)

	// different prefix for `--peer-urls` and `--initial-cluster`
	cfg = NewConfig()
	err = cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", t.T().TempDir()),
		fmt.Sprintf("--master-addr=https://%s", masterAddr),
		fmt.Sprintf("--advertise-addr=https://%s", masterAddr),
		fmt.Sprintf("--peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=http://%s", peerAddr),
		"--ssl-ca=" + caPath,
		"--ssl-cert=" + certPath,
		"--ssl-key=" + keyPath,
	})
	require.NoError(t.T(), err)
	require.Equal(t.T(), masterAddr, cfg.MasterAddr)
	require.Equal(t.T(), masterAddr, cfg.AdvertiseAddr)
	require.Equal(t.T(), "https://"+peerAddr, cfg.PeerUrls)
	require.Equal(t.T(), "https://"+peerAddr, cfg.AdvertisePeerUrls)
	require.Equal(t.T(), "master-tls=https://"+peerAddr, cfg.InitialCluster)
	t.testTLSPrefix(cfg)

	// listen address set to 0.0.0.0
	cfg = NewConfig()
	err = cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", t.T().TempDir()),
		fmt.Sprintf("--master-addr=0.0.0.0:%s", masterPort),
		fmt.Sprintf("--advertise-addr=https://%s", masterAddr),
		fmt.Sprintf("--peer-urls=0.0.0.0:%s", peerPort),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=https://%s", peerAddr),
		"--ssl-ca=" + caPath,
		"--ssl-cert=" + certPath,
		"--ssl-key=" + keyPath,
	})
	require.NoError(t.T(), err)
	t.testTLSPrefix(cfg)
}

func (t *testMasterSuite) testTLSPrefix(cfg *Config) {
	t.testNormalServerLifecycle(cfg, func(cfg *Config) {
		t.testHTTPInterface(fmt.Sprintf("https://%s/status", cfg.AdvertiseAddr), []byte(version.GetRawInfo()))
		t.testHTTPInterface(fmt.Sprintf("https://%s/debug/pprof/", cfg.AdvertiseAddr), []byte("Types of profiles available"))
	})
}

func (t *testMasterSuite) testNormalServerLifecycle(cfg *Config, checkLogic func(*Config)) {
	var err error
	s := NewServer(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	err = s.Start(ctx)
	require.NoError(t.T(), err)

	checkLogic(cfg)

	// close
	cancel()
	s.Close()

	require.Eventually(t.T(), func() bool {
		return s.closed.Load()
	}, 3*time.Second, 100*time.Millisecond)
}

func (t *testMasterSuite) testHTTPInterface(url string, contain []byte) {
	// we use HTTPS in some test cases.
	tlsConfig, err := toolutils.NewTLSConfig(
		toolutils.WithCAPath(pwd+"/tls_for_test/ca.pem"),
		toolutils.WithCertAndKeyPath(pwd+"/tls_for_test/dm.pem", pwd+"/tls_for_test/dm.key"),
	)
	require.NoError(t.T(), err)
	cli := toolutils.ClientWithTLS(tlsConfig)

	// nolint:noctx
	resp, err := cli.Get(url)
	require.NoError(t.T(), err)
	defer resp.Body.Close()
	require.Equal(t.T(), http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t.T(), err)
	require.True(t.T(), bytes.Contains(body, contain))
}

func (t *testMasterSuite) TestJoinMember() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	// create a new cluster
	cfg1 := NewConfig()
	require.NoError(t.T(), cfg1.FromContent(SampleConfig))
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = t.T().TempDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.AdvertiseAddr = cfg1.MasterAddr
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 := NewServer(cfg1)
	require.NoError(t.T(), s1.Start(ctx))
	defer s1.Close()

	// wait the first one become the leader
	require.Eventually(t.T(), func() bool {
		return s1.election.IsLeader()
	}, 3*time.Second, 100*time.Millisecond)

	// join to an existing cluster
	cfg2 := NewConfig()
	require.NoError(t.T(), cfg2.FromContent(SampleConfig))
	cfg2.Name = "dm-master-2"
	cfg2.DataDir = t.T().TempDir()
	cfg2.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg2.AdvertiseAddr = cfg2.MasterAddr
	cfg2.PeerUrls = tempurl.Alloc()
	cfg2.AdvertisePeerUrls = cfg2.PeerUrls
	cfg2.Join = cfg1.MasterAddr // join to an existing cluster

	s2 := NewServer(cfg2)
	require.NoError(t.T(), s2.Start(ctx))
	defer s2.Close()

	client, err := etcdutil.CreateClient(strings.Split(cfg1.AdvertisePeerUrls, ","), nil)
	require.NoError(t.T(), err)
	defer client.Close()

	// verify members
	listResp, err := etcdutil.ListMembers(client)
	require.NoError(t.T(), err)
	require.Len(t.T(), listResp.Members, 2)
	names := make(map[string]struct{}, len(listResp.Members))
	for _, m := range listResp.Members {
		names[m.Name] = struct{}{}
	}
	require.Contains(t.T(), names, cfg1.Name)
	require.Contains(t.T(), names, cfg2.Name)

	// s1 is still the leader
	_, leaderID, _, err := s2.election.LeaderInfo(ctx)

	require.NoError(t.T(), err)
	require.Equal(t.T(), leaderID, cfg1.Name)

	cfg3 := NewConfig()
	require.NoError(t.T(), cfg3.FromContent(SampleConfig))
	cfg3.Name = "dm-master-3"
	cfg3.DataDir = t.T().TempDir()
	cfg3.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg3.AdvertiseAddr = cfg3.MasterAddr
	cfg3.PeerUrls = tempurl.Alloc()
	cfg3.AdvertisePeerUrls = cfg3.PeerUrls
	cfg3.Join = cfg1.MasterAddr // join to an existing cluster

	// mock join master without wal dir
	require.NoError(t.T(), os.Mkdir(filepath.Join(cfg3.DataDir, "member"), privateDirMode))
	require.NoError(t.T(), os.Mkdir(filepath.Join(cfg3.DataDir, "member", "join"), privateDirMode))
	s3 := NewServer(cfg3)
	// avoid join a unhealthy cluster
	require.Eventually(t.T(), func() bool {
		return s3.Start(ctx) == nil
	}, 30*time.Second, time.Second)
	defer s3.Close()

	// verify members
	listResp, err = etcdutil.ListMembers(client)
	require.NoError(t.T(), err)
	require.Len(t.T(), listResp.Members, 3)
	names = make(map[string]struct{}, len(listResp.Members))
	for _, m := range listResp.Members {
		names[m.Name] = struct{}{}
	}
	require.Contains(t.T(), names, cfg1.Name)
	require.Contains(t.T(), names, cfg2.Name)
	require.Contains(t.T(), names, cfg3.Name)

	cancel()
	t.clearEtcdEnv()
}

func (t *testMasterSuite) TestOperateSource() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()

	// create a new cluster
	cfg1 := NewConfig()
	require.NoError(t.T(), cfg1.FromContent(SampleConfig))
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = t.T().TempDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.AdvertiseAddr = cfg1.MasterAddr
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 := NewServer(cfg1)
	s1.leader.Store(oneselfLeader)
	require.NoError(t.T(), s1.Start(ctx))
	defer s1.Close()
	mysqlCfg, err := config.ParseYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t.T(), err)
	mysqlCfg.From.Password = os.Getenv("MYSQL_PSWD")
	task, err := mysqlCfg.Yaml()
	require.NoError(t.T(), err)
	sourceID := mysqlCfg.SourceID
	// 1. wait for scheduler to start
	time.Sleep(3 * time.Second)

	// 2. try to add a new mysql source
	req := &pb.OperateSourceRequest{Op: pb.SourceOp_StartSource, Config: []string{task}}
	resp, err := s1.OperateSource(ctx, req)
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	require.Equal(t.T(), []*pb.CommonWorkerResponse{{
		Result: true,
		Msg:    "source is added but there is no free worker to bound",
		Source: sourceID,
	}}, resp.Sources)
	unBoundSources := s1.scheduler.UnboundSources()
	require.Len(t.T(), unBoundSources, 1)
	require.Equal(t.T(), sourceID, unBoundSources[0])

	// 3. try to add multiple source
	// 3.1 duplicated source id
	sourceID2 := "mysql-replica-02"
	mysqlCfg.SourceID = sourceID2
	task2, err := mysqlCfg.Yaml()
	require.NoError(t.T(), err)
	req = &pb.OperateSourceRequest{Op: pb.SourceOp_StartSource, Config: []string{task2, task2}}
	resp, err = s1.OperateSource(ctx, req)
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Contains(t.T(), resp.Msg, "source config with ID "+sourceID2+" already exists")
	// 3.2 run same command after correction
	sourceID3 := "mysql-replica-03"
	mysqlCfg.SourceID = sourceID3
	task3, err := mysqlCfg.Yaml()
	require.NoError(t.T(), err)
	req = &pb.OperateSourceRequest{Op: pb.SourceOp_StartSource, Config: []string{task2, task3}}
	resp, err = s1.OperateSource(ctx, req)
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	sort.Slice(resp.Sources, func(i, j int) bool {
		return resp.Sources[i].Source < resp.Sources[j].Source
	})
	require.Equal(t.T(), []*pb.CommonWorkerResponse{{
		Result: true,
		Msg:    "source is added but there is no free worker to bound",
		Source: sourceID2,
	}, {
		Result: true,
		Msg:    "source is added but there is no free worker to bound",
		Source: sourceID3,
	}}, resp.Sources)
	unBoundSources = s1.scheduler.UnboundSources()
	require.Len(t.T(), unBoundSources, 3)
	require.Equal(t.T(), sourceID, unBoundSources[0])
	require.Equal(t.T(), sourceID2, unBoundSources[1])
	require.Equal(t.T(), sourceID3, unBoundSources[2])

	// 4. try to stop a non-exist-source
	req.Op = pb.SourceOp_StopSource
	mysqlCfg.SourceID = "not-exist-source"
	task4, err := mysqlCfg.Yaml()
	require.NoError(t.T(), err)
	req.Config = []string{task4}
	resp, err = s1.OperateSource(ctx, req)
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Contains(t.T(), resp.Msg, "source config with ID "+mysqlCfg.SourceID+" not exists")

	// 5. start workers, the unbounded sources should be bounded
	var wg sync.WaitGroup
	workerName1 := "worker1"
	workerName2 := "worker2"
	workerName3 := "worker3"
	defer func() {
		t.clearSchedulerEnv(cancel, &wg)
	}()
	require.NoError(t.T(), s1.scheduler.AddWorker(workerName1, "172.16.10.72:8262"))
	wg.Add(1)
	go func(ctx context.Context, workerName string) {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL))
	}(ctx, workerName1)
	require.NoError(t.T(), s1.scheduler.AddWorker(workerName2, "172.16.10.72:8263"))
	wg.Add(1)
	go func(ctx context.Context, workerName string) {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL))
	}(ctx, workerName2)
	require.NoError(t.T(), s1.scheduler.AddWorker(workerName3, "172.16.10.72:8264"))
	wg.Add(1)
	go func(ctx context.Context, workerName string) {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL))
	}(ctx, workerName3)
	require.Eventually(t.T(), func() bool {
		w := s1.scheduler.GetWorkerBySource(sourceID)
		return w != nil
	}, 3*time.Second, 100*time.Millisecond)

	// 6. stop sources
	req.Config = []string{task, task2, task3}
	req.Op = pb.SourceOp_StopSource

	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	mockRevelantWorkerClient(mockWorkerClient, "", sourceID, req)
	s1.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	mockWorkerClient2 := pbmock.NewMockWorkerClient(ctrl)
	mockRevelantWorkerClient(mockWorkerClient2, "", sourceID2, req)
	s1.scheduler.SetWorkerClientForTest(workerName2, newMockRPCClient(mockWorkerClient2))
	mockWorkerClient3 := pbmock.NewMockWorkerClient(ctrl)
	mockRevelantWorkerClient(mockWorkerClient3, "", sourceID3, req)
	s1.scheduler.SetWorkerClientForTest(workerName3, newMockRPCClient(mockWorkerClient3))
	resp, err = s1.OperateSource(ctx, req)
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)
	require.Equal(t.T(), []*pb.CommonWorkerResponse{{
		Result: true,
		Source: sourceID,
	}, {
		Result: true,
		Source: sourceID2,
	}, {
		Result: true,
		Source: sourceID3,
	}}, resp.Sources)
	scm, _, err := ha.GetSourceCfg(t.etcdTestCli, sourceID, 0)
	require.NoError(t.T(), err)
	require.Len(t.T(), scm, 0)
	t.clearSchedulerEnv(cancel, &wg)

	cancel()
}

func (t *testMasterSuite) TestOfflineMember() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)

	cfg1 := generateServerConfig(t.T(), "dm-master-1")
	cfg2 := generateServerConfig(t.T(), "dm-master-2")
	cfg3 := generateServerConfig(t.T(), "dm-master-3")

	initialCluster := fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls) + "," +
		fmt.Sprintf("%s=%s", cfg2.Name, cfg2.AdvertisePeerUrls) + "," +
		fmt.Sprintf("%s=%s", cfg3.Name, cfg3.AdvertisePeerUrls)
	cfg1.InitialCluster = initialCluster
	cfg2.InitialCluster = initialCluster
	cfg3.InitialCluster = initialCluster

	var wg sync.WaitGroup
	s1 := NewServer(cfg1)
	defer func() {
		cancel()
		s1.Close()
	}()
	wg.Add(1)
	go func() {
		require.NoError(t.T(), s1.Start(ctx))
		wg.Done()
	}()

	s2 := NewServer(cfg2)
	defer func() {
		cancel()
		s2.Close()
	}()
	wg.Add(1)
	go func() {
		require.NoError(t.T(), s2.Start(ctx))
		wg.Done()
	}()

	ctx3, cancel3 := context.WithCancel(ctx)
	s3 := NewServer(cfg3)
	require.NoError(t.T(), s3.Start(ctx3))
	defer func() {
		cancel3()
		s3.Close()
	}()

	wg.Wait()

	var leaderID string
	// ensure s2 has got the right leader info, because it will be used to `OfflineMember`.
	require.Eventually(t.T(), func() bool {
		s2.RLock()
		leader := s2.leader.Load()
		s2.RUnlock()
		if leader == "" {
			return false
		}
		if leader == oneselfLeader {
			leaderID = s2.cfg.Name
		} else {
			leaderID = s2.leader.Load()
		}
		return true
	}, 3*time.Second, 100*time.Millisecond)

	// master related operations
	req := &pb.OfflineMemberRequest{
		Type: "masters",
		Name: "xixi",
	}
	// test offline member with wrong type
	resp, err := s2.OfflineMember(ctx, req)
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Contains(t.T(), resp.Msg, terror.ErrMasterInvalidOfflineType.Generate(req.Type).Error())
	// test offline member with invalid master name
	req.Type = common.Master
	resp, err = s2.OfflineMember(ctx, req)
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Contains(t.T(), resp.Msg, `dm-master with name `+req.Name+` not exists`)
	// test offline member with correct master name
	cli := s2.etcdClient
	listResp, err := etcdutil.ListMembers(cli)
	require.NoError(t.T(), err)
	require.Len(t.T(), listResp.Members, 3)

	// make sure s3 is not the leader, otherwise it will take some time to campaign a new leader after close s3, and it may cause timeout
	require.Eventually(t.T(), func() bool {
		_, leaderID, _, err = s1.election.LeaderInfo(ctx)
		if err != nil {
			return false
		}

		if leaderID == s3.cfg.Name {
			_, err = s3.OperateLeader(ctx, &pb.OperateLeaderRequest{
				Op: pb.LeaderOp_EvictLeaderOp,
			})
			require.NoError(t.T(), err)
		}
		return leaderID != s3.cfg.Name
	}, 10*time.Second, 500*time.Millisecond)

	cancel3()
	s3.Close()

	req.Name = s3.cfg.Name
	resp, err = s2.OfflineMember(ctx, req)
	require.NoError(t.T(), err)
	require.Equal(t.T(), "", resp.Msg)
	require.True(t.T(), resp.Result)

	listResp, err = etcdutil.ListMembers(cli)
	require.NoError(t.T(), err)
	require.Len(t.T(), listResp.Members, 2)
	if listResp.Members[0].Name == cfg2.Name {
		listResp.Members[0], listResp.Members[1] = listResp.Members[1], listResp.Members[0]
	}
	require.Equal(t.T(), cfg1.Name, listResp.Members[0].Name)
	require.Equal(t.T(), cfg2.Name, listResp.Members[1].Name)

	_, leaderID2, _, err := s1.election.LeaderInfo(ctx)
	require.NoError(t.T(), err)

	if leaderID == cfg3.Name {
		// s3 is leader before, leader should re-campaign
		require.False(t.T(), leaderID != leaderID2)
	} else {
		// s3 isn't leader before, leader should keep the same
		require.Equal(t.T(), leaderID, leaderID2)
	}

	// worker related operations
	ectx, canc := context.WithTimeout(ctx, time.Second)
	defer canc()
	req1 := &pb.RegisterWorkerRequest{
		Name:    "xixi",
		Address: "127.0.0.1:1000",
	}
	regReq, err := s1.RegisterWorker(ectx, req1)
	require.NoError(t.T(), err)
	require.True(t.T(), regReq.Result)

	req2 := &pb.OfflineMemberRequest{
		Type: common.Worker,
		Name: "haha",
	}
	{
		res, err := s1.OfflineMember(ectx, req2)
		require.NoError(t.T(), err)
		require.False(t.T(), res.Result)
		require.Contains(t.T(), res.Msg, `dm-worker with name `+req2.Name+` not exists`)
	}
	{
		req2.Name = "xixi"
		res, err := s1.OfflineMember(ectx, req2)
		require.NoError(t.T(), err)
		require.True(t.T(), res.Result)
	}
	{
		// register offline worker again. TICASE-962, 963
		resp, err := s1.RegisterWorker(ectx, req1)
		require.NoError(t.T(), err)
		require.True(t.T(), resp.Result)
	}
	t.clearSchedulerEnv(cancel, &wg)
}

func (t *testMasterSuite) TestGetCfg() {
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()

	server := testDefaultMasterServer(t.T())
	sources, workers := defaultWorkerSource()

	var wg sync.WaitGroup
	taskName := "test"
	ctx, cancel := context.WithCancel(context.Background())
	req := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, req))
	server.etcdClient = t.etcdTestCli

	// start task
	mock := conn.InitVersionDB()
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err := server.StartTask(context.Background(), req)
	require.NoError(t.T(), err)
	require.True(t.T(), resp.Result)

	// get task config
	req1 := &pb.GetCfgRequest{
		Name: taskName,
		Type: pb.CfgType_TaskType,
	}
	resp1, err := server.GetCfg(context.Background(), req1)
	require.NoError(t.T(), err)
	require.True(t.T(), resp1.Result)
	require.Contains(t.T(), resp1.Cfg, "name: test")

	// not exist task name
	taskName2 := "wrong"
	req2 := &pb.GetCfgRequest{
		Name: taskName2,
		Type: pb.CfgType_TaskType,
	}
	resp2, err := server.GetCfg(context.Background(), req2)
	require.NoError(t.T(), err)
	require.False(t.T(), resp2.Result)
	require.Contains(t.T(), resp2.Msg, "task not found")

	// generate a template named `wrong`, test get this task template
	openapiTask, err := fixtures.GenNoShardOpenAPITaskForTest()
	require.NoError(t.T(), err)
	openapiTask.Name = taskName2
	require.NoError(t.T(), ha.PutOpenAPITaskTemplate(t.etcdTestCli, openapiTask, true))
	require.NoError(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/master/MockSkipAdjustTargetDB", `return(true)`))
	resp2, err = server.GetCfg(context.Background(), &pb.GetCfgRequest{Name: taskName2, Type: pb.CfgType_TaskTemplateType})
	require.NoError(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/master/MockSkipAdjustTargetDB"))
	require.NoError(t.T(), err)
	require.True(t.T(), resp2.Result)
	require.Contains(t.T(), resp2.Cfg, "name: "+taskName2)

	// test restart master
	server.scheduler.Close()
	require.NoError(t.T(), server.scheduler.Start(ctx, t.etcdTestCli))

	resp3, err := server.GetCfg(context.Background(), req1)
	require.NoError(t.T(), err)
	require.True(t.T(), resp3.Result)
	require.Equal(t.T(), resp1.Cfg, resp3.Cfg)

	req3 := &pb.GetCfgRequest{
		Name: "dm-master",
		Type: pb.CfgType_MasterType,
	}
	resp4, err := server.GetCfg(context.Background(), req3)
	require.NoError(t.T(), err)
	require.True(t.T(), resp4.Result)
	require.Contains(t.T(), resp4.Cfg, `name = "dm-master"`)

	req4 := &pb.GetCfgRequest{
		Name: "haha",
		Type: pb.CfgType_MasterType,
	}
	resp5, err := server.GetCfg(context.Background(), req4)
	require.NoError(t.T(), err)
	require.False(t.T(), resp5.Result)
	require.Contains(t.T(), resp5.Msg, "master not found")

	req5 := &pb.GetCfgRequest{
		Name: "haha",
		Type: pb.CfgType_WorkerType,
	}
	resp6, err := server.GetCfg(context.Background(), req5)
	require.NoError(t.T(), err)
	require.False(t.T(), resp6.Result)
	require.Contains(t.T(), resp6.Msg, "worker not found")

	req6 := &pb.GetCfgRequest{
		Name: "mysql-replica-01",
		Type: pb.CfgType_SourceType,
	}
	resp7, err := server.GetCfg(context.Background(), req6)
	require.NoError(t.T(), err)
	require.True(t.T(), resp7.Result)
	require.Contains(t.T(), resp7.Cfg, `source-id: mysql-replica-01`)

	req7 := &pb.GetCfgRequest{
		Name: "haha",
		Type: pb.CfgType_SourceType,
	}
	resp8, err := server.GetCfg(context.Background(), req7)
	require.NoError(t.T(), err)
	require.False(t.T(), resp8.Result)
	require.Equal(t.T(), resp8.Msg, "source not found")

	t.clearSchedulerEnv(cancel, &wg)
}

func (t *testMasterSuite) relayStageMatch(s *scheduler.Scheduler, source string, expectStage pb.Stage) {
	stage := ha.NewRelayStage(expectStage, source)
	stageDeepEqualExcludeRev(t.T(), s.GetExpectRelayStage(source), stage)

	eStage, _, err := ha.GetRelayStage(t.etcdTestCli, source)
	require.NoError(t.T(), err)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		stageDeepEqualExcludeRev(t.T(), eStage, stage)
	}
}

func (t *testMasterSuite) subTaskStageMatch(s *scheduler.Scheduler, task, source string, expectStage pb.Stage) {
	stage := ha.NewSubTaskStage(expectStage, source, task)
	require.Equal(t.T(), s.GetExpectSubTaskStage(task, source), stage)

	eStageM, _, err := ha.GetSubTaskStage(t.etcdTestCli, source, task)
	require.NoError(t.T(), err)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		require.Len(t.T(), eStageM, 1)
		stageDeepEqualExcludeRev(t.T(), eStageM[task], stage)
	default:
		require.Len(t.T(), eStageM, 0)
	}
}

func (t *testMasterSuite) TestGRPCLongResponse() {
	require.NoError(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/master/LongRPCResponse", `return()`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/master/LongRPCResponse")
	require.NoError(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/ctl/common/SkipUpdateMasterClient", `return()`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/ctl/common/SkipUpdateMasterClient")

	masterAddr := tempurl.Alloc()[len("http://"):]
	lis, err := net.Listen("tcp", masterAddr)
	require.NoError(t.T(), err)
	defer lis.Close()
	server := grpc.NewServer()
	pb.RegisterMasterServer(server, &Server{})
	//nolint:errcheck
	go server.Serve(lis)

	conn, err := grpc.Dial(utils.UnwrapScheme(masterAddr),
		grpc.WithInsecure(),
		grpc.WithBlock())
	require.NoError(t.T(), err)
	defer conn.Close()

	common.GlobalCtlClient.MasterClient = pb.NewMasterClient(conn)
	ctx := context.Background()
	resp := &pb.StartTaskResponse{}
	err = common.SendRequest(ctx, "StartTask", &pb.StartTaskRequest{}, &resp)
	require.NoError(t.T(), err)
}

func (t *testMasterSuite) TestStartStopValidation() {
	var (
		wg       sync.WaitGroup
		taskName = "test"
	)
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()
	server := testDefaultMasterServer(t.T())
	server.etcdClient = t.etcdTestCli
	sources, workers := defaultWorkerSource()
	ctx, cancel := context.WithCancel(context.Background())
	defer t.clearSchedulerEnv(cancel, &wg)
	// start task without validation
	startReq := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	sourceResps := []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}, {Result: true, Source: sources[1]}}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, startReq))
	mock := conn.InitVersionDB()
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	stResp, err := server.StartTask(context.Background(), startReq)
	require.NoError(t.T(), err)
	require.True(t.T(), stResp.Result)

	for _, source := range sources {
		t.subTaskStageMatch(server.scheduler, taskName, source, pb.Stage_Running)
	}
	require.Equal(t.T(), sourceResps, stResp.Sources)

	// (fail) start all validator of the task with explicit but invalid mode
	validatorStartReq := &pb.StartValidationRequest{
		Mode:     &pb.StartValidationRequest_ModeValue{ModeValue: "invalid-mode"},
		TaskName: taskName,
	}
	startResp, err := server.StartValidation(context.Background(), validatorStartReq)
	require.NoError(t.T(), err)
	require.False(t.T(), startResp.Result)
	require.Contains(t.T(), startResp.Msg, "validation mode should be either `full` or `fast`")
	t.validatorStageMatch(taskName, sources[0], pb.Stage_InvalidStage)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_InvalidStage)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationNone, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationNone, "")

	// (fail) start with explicit but invalid start-time
	validatorStartReq = &pb.StartValidationRequest{
		StartTime: &pb.StartValidationRequest_StartTimeValue{StartTimeValue: "xxx"},
		TaskName:  taskName,
	}
	startResp, err = server.StartValidation(context.Background(), validatorStartReq)
	require.NoError(t.T(), err)
	require.False(t.T(), startResp.Result)
	require.Contains(t.T(), startResp.Msg, "start-time should be in the format like")
	t.validatorStageMatch(taskName, sources[0], pb.Stage_InvalidStage)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_InvalidStage)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationNone, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationNone, "")

	// (fail) start for non-existed subtask
	validatorStartReq = &pb.StartValidationRequest{
		TaskName: "not-exist-name",
	}
	startResp, err = server.StartValidation(context.Background(), validatorStartReq)
	require.NoError(t.T(), err)
	require.False(t.T(), startResp.Result)
	require.Contains(t.T(), startResp.Msg, "cannot get subtask by task name")
	t.validatorStageMatch(taskName, sources[0], pb.Stage_InvalidStage)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_InvalidStage)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationNone, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationNone, "")

	// (fail) start for non-exist source
	validatorStartReq = &pb.StartValidationRequest{
		Sources: []string{"xxx"},
	}
	startResp, err = server.StartValidation(context.Background(), validatorStartReq)
	require.NoError(t.T(), err)
	require.False(t.T(), startResp.Result)
	require.Contains(t.T(), startResp.Msg, "cannot get subtask by sources")
	t.validatorStageMatch(taskName, sources[0], pb.Stage_InvalidStage)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_InvalidStage)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationNone, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationNone, "")

	// (success) start validation without explicit mode for source 0
	validatorStartReq = &pb.StartValidationRequest{
		TaskName: taskName,
		Sources:  []string{sources[0]},
	}
	startResp, err = server.StartValidation(context.Background(), validatorStartReq)
	require.NoError(t.T(), err)
	require.True(t.T(), startResp.Result)
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Running)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_InvalidStage)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationNone, "")

	// (fail) start all validator with explicit mode
	validatorStartReq = &pb.StartValidationRequest{
		Mode:     &pb.StartValidationRequest_ModeValue{ModeValue: config.ValidationFull},
		TaskName: taskName,
	}
	startResp, err = server.StartValidation(context.Background(), validatorStartReq)
	require.NoError(t.T(), err)
	require.False(t.T(), startResp.Result)
	require.Regexp(t.T(), ".*some of target validator.* has already enabled.*", startResp.Msg)
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Running)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_InvalidStage)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationNone, "")

	// (fail) start validation with explicit mode for source 0 again
	validatorStartReq = &pb.StartValidationRequest{
		Mode:     &pb.StartValidationRequest_ModeValue{ModeValue: config.ValidationFull},
		TaskName: taskName,
		Sources:  []string{sources[0]},
	}
	startResp, err = server.StartValidation(context.Background(), validatorStartReq)
	require.NoError(t.T(), err)
	require.False(t.T(), startResp.Result)
	require.Contains(t.T(), startResp.Msg, "all target validator has enabled, cannot do 'validation start' with explicit mode or start-time")
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Running)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_InvalidStage)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationNone, "")

	// (fail) start all validator without explicit mode
	validatorStartReq = &pb.StartValidationRequest{
		TaskName: taskName,
	}
	startResp, err = server.StartValidation(context.Background(), validatorStartReq)
	require.NoError(t.T(), err)
	require.False(t.T(), startResp.Result)
	require.Regexp(t.T(), ".*some of target validator.* has already enabled.*", startResp.Msg)
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Running)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_InvalidStage)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationNone, "")

	// (fail) stop validator of source 1
	validatorStopReq := &pb.StopValidationRequest{
		TaskName: taskName,
		Sources:  sources[1:],
	}
	stopResp, err := server.StopValidation(context.Background(), validatorStopReq)
	require.NoError(t.T(), err)
	require.False(t.T(), stopResp.Result)
	require.Regexp(t.T(), ".*some target validator.* is not enabled.*", stopResp.Msg)
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Running)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_InvalidStage)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationNone, "")

	// (fail) stop all validator
	validatorStopReq = &pb.StopValidationRequest{
		TaskName: taskName,
	}
	stopResp, err = server.StopValidation(context.Background(), validatorStopReq)
	require.NoError(t.T(), err)
	require.False(t.T(), stopResp.Result)
	require.Regexp(t.T(), ".*some target validator.* is not enabled.*", stopResp.Msg)
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Running)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_InvalidStage)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationNone, "")

	// (success) start validation with fast mode and start-time for source 1
	validatorStartReq = &pb.StartValidationRequest{
		Mode:      &pb.StartValidationRequest_ModeValue{ModeValue: config.ValidationFast},
		StartTime: &pb.StartValidationRequest_StartTimeValue{StartTimeValue: "2006-01-02 15:04:05"},
		TaskName:  taskName,
		Sources:   []string{sources[1]},
	}
	startResp, err = server.StartValidation(context.Background(), validatorStartReq)
	require.NoError(t.T(), err)
	require.True(t.T(), startResp.Result)
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Running)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_Running)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationFast, "2006-01-02 15:04:05")

	// now validator of the 2 subtask is enabled(running)

	// (success) start all validator of the task without explicit param again, i.e. resuming
	validatorStartReq = &pb.StartValidationRequest{
		TaskName: taskName,
	}
	startResp, err = server.StartValidation(context.Background(), validatorStartReq)
	require.NoError(t.T(), err)
	require.True(t.T(), startResp.Result)
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Running)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_Running)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationFast, "2006-01-02 15:04:05")

	// (fail) stop non-existed subtask's validator
	validatorStopReq = &pb.StopValidationRequest{
		TaskName: "not-exist-name",
	}
	stopResp, err = server.StopValidation(context.Background(), validatorStopReq)
	require.NoError(t.T(), err)
	require.False(t.T(), stopResp.Result)
	require.Contains(t.T(), stopResp.Msg, "cannot get subtask by task name")
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Running)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_Running)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationFast, "2006-01-02 15:04:05")

	// (fail) stop all task but with non-exist source
	validatorStopReq = &pb.StopValidationRequest{
		Sources: []string{"xxx"},
	}
	stopResp, err = server.StopValidation(context.Background(), validatorStopReq)
	require.NoError(t.T(), err)
	require.False(t.T(), stopResp.Result)
	require.Contains(t.T(), stopResp.Msg, "cannot get subtask by source")
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Running)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_Running)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationFast, "2006-01-02 15:04:05")

	// (success) stop validation of source 0
	validatorStopReq = &pb.StopValidationRequest{
		TaskName: taskName,
		Sources:  []string{sources[0]},
	}
	stopResp, err = server.StopValidation(context.Background(), validatorStopReq)
	require.NoError(t.T(), err)
	require.True(t.T(), stopResp.Result)
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Stopped)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_Running)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationFast, "2006-01-02 15:04:05")

	// (success) stop all
	validatorStopReq = &pb.StopValidationRequest{
		TaskName: "",
	}
	stopResp, err = server.StopValidation(context.Background(), validatorStopReq)
	require.NoError(t.T(), err)
	require.True(t.T(), stopResp.Result)
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Stopped)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_Stopped)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationFast, "2006-01-02 15:04:05")

	// (success) stop all again
	validatorStopReq = &pb.StopValidationRequest{
		TaskName: "",
	}
	stopResp, err = server.StopValidation(context.Background(), validatorStopReq)
	require.NoError(t.T(), err)
	require.True(t.T(), stopResp.Result)
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Stopped)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_Stopped)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationFast, "2006-01-02 15:04:05")

	// (success) start all tasks
	validatorStartReq = &pb.StartValidationRequest{
		TaskName: "",
	}
	startResp, err = server.StartValidation(context.Background(), validatorStartReq)
	require.NoError(t.T(), err)
	require.True(t.T(), startResp.Result)
	t.validatorStageMatch(taskName, sources[0], pb.Stage_Running)
	t.validatorStageMatch(taskName, sources[1], pb.Stage_Running)
	t.validatorModeMatch(server.scheduler, taskName, sources[0], config.ValidationFull, "")
	t.validatorModeMatch(server.scheduler, taskName, sources[1], config.ValidationFast, "2006-01-02 15:04:05")
}

//nolint:unparam
func (t *testMasterSuite) validatorStageMatch(taskName, source string, expectStage pb.Stage) {
	stage := ha.NewValidatorStage(expectStage, source, taskName)

	stageM, _, err := ha.GetValidatorStage(t.etcdTestCli, source, taskName, 0)
	require.NoError(t.T(), err)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Stopped:
		require.Len(t.T(), stageM, 1)
		stageDeepEqualExcludeRev(t.T(), stageM[taskName], stage)
	default:
		require.Len(t.T(), stageM, 0)
	}
}

//nolint:unparam
func (t *testMasterSuite) validatorModeMatch(s *scheduler.Scheduler, task, source string,
	expectMode, expectedStartTime string,
) {
	cfgs := s.GetSubTaskCfgsByTaskAndSource(task, []string{source})
	v, ok := cfgs[task]
	require.True(t.T(), ok)
	cfg, ok := v[source]
	require.True(t.T(), ok)
	require.Equal(t.T(), expectMode, cfg.ValidatorCfg.Mode)
	require.Equal(t.T(), expectedStartTime, cfg.ValidatorCfg.StartTime)
}

func (t *testMasterSuite) TestGetValidatorStatus() {
	var (
		wg       sync.WaitGroup
		taskName = "test"
	)
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()
	server := testDefaultMasterServer(t.T())
	server.etcdClient = t.etcdTestCli
	sources, workers := defaultWorkerSource()
	startReq := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	// test query all workers
	for idx, worker := range workers {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().GetWorkerValidatorStatus(
			gomock.Any(),
			gomock.Any(),
		).Return(&pb.GetValidationStatusResponse{
			Result: true,
			TableStatuses: []*pb.ValidationTableStatus{
				{
					SrcTable: "tbl1",
				},
			},
		}, nil)
		mockWorkerClient.EXPECT().GetWorkerValidatorStatus(
			gomock.Any(),
			gomock.Any(),
		).Return(&pb.GetValidationStatusResponse{
			Result: false,
			Msg:    "something wrong in worker",
		}, nil)
		mockWorkerClient.EXPECT().GetWorkerValidatorStatus(
			gomock.Any(),
			gomock.Any(),
		).Return(&pb.GetValidationStatusResponse{}, errors.New("grpc error"))
		mockRevelantWorkerClient(mockWorkerClient, taskName, sources[idx], startReq)
		t.workerClients[worker] = newMockRPCClient(mockWorkerClient)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer t.clearSchedulerEnv(cancel, &wg)
	// start task without validation
	sourceResps := []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}, {Result: true, Source: sources[1]}}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "", t.workerClients)
	mock := conn.InitVersionDB()
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	stResp, err := server.StartTask(context.Background(), startReq)
	require.NoError(t.T(), err)
	require.True(t.T(), stResp.Result)

	for _, source := range sources {
		t.subTaskStageMatch(server.scheduler, taskName, source, pb.Stage_Running)
	}
	require.Equal(t.T(), sourceResps, stResp.Sources)
	// 1. query existing task's status
	statusReq := &pb.GetValidationStatusRequest{
		TaskName: taskName,
	}
	resp, err := server.GetValidationStatus(context.Background(), statusReq)
	require.NoError(t.T(), err)
	require.Equal(t.T(), "", resp.Msg)
	require.True(t.T(), resp.Result)
	require.Equal(t.T(), 2, len(resp.TableStatuses))
	// 2. query invalid task's status
	statusReq.TaskName = "invalid-task"
	resp, err = server.GetValidationStatus(context.Background(), statusReq)
	require.NoError(t.T(), err)
	require.Contains(t.T(), resp.Msg, "cannot get subtask by task name")
	require.False(t.T(), resp.Result)
	// 3. query invalid stage
	statusReq.TaskName = taskName
	statusReq.FilterStatus = pb.Stage_Paused // invalid stage
	resp, err = server.GetValidationStatus(context.Background(), statusReq)
	require.NoError(t.T(), err)
	require.Contains(t.T(), resp.Msg, "filtering stage should be either")
	require.False(t.T(), resp.Result)
	// 4. worker error
	statusReq.FilterStatus = pb.Stage_Running
	resp, err = server.GetValidationStatus(context.Background(), statusReq)
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Contains(t.T(), resp.Msg, "something wrong in worker")
	// 5. grpc error
	statusReq.FilterStatus = pb.Stage_Running
	resp, err = server.GetValidationStatus(context.Background(), statusReq)
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Contains(t.T(), resp.Msg, "grpc error")
}

func (t *testMasterSuite) TestGetValidationError() {
	var (
		wg       sync.WaitGroup
		taskName = "test"
	)
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()
	server := testDefaultMasterServer(t.T())
	server.etcdClient = t.etcdTestCli
	sources, workers := defaultWorkerSource()
	startReq := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	// test query all workers
	for idx, worker := range workers {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().GetValidatorError(
			gomock.Any(),
			gomock.Any(),
		).Return(&pb.GetValidationErrorResponse{
			Result: true,
			Error: []*pb.ValidationError{
				{
					Id: "1",
				},
			},
		}, nil)
		mockWorkerClient.EXPECT().GetValidatorError(
			gomock.Any(),
			gomock.Any(),
		).Return(&pb.GetValidationErrorResponse{
			Result: false,
			Msg:    "something wrong in worker",
			Error:  []*pb.ValidationError{},
		}, nil)
		mockWorkerClient.EXPECT().GetValidatorError(
			gomock.Any(),
			gomock.Any(),
		).Return(&pb.GetValidationErrorResponse{}, errors.New("grpc error"))
		mockRevelantWorkerClient(mockWorkerClient, taskName, sources[idx], startReq)
		t.workerClients[worker] = newMockRPCClient(mockWorkerClient)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer t.clearSchedulerEnv(cancel, &wg)
	// start task without validation
	sourceResps := []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}, {Result: true, Source: sources[1]}}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "", t.workerClients)
	mock := conn.InitVersionDB()
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	stResp, err := server.StartTask(context.Background(), startReq)
	require.NoError(t.T(), err)
	require.True(t.T(), stResp.Result)

	for _, source := range sources {
		t.subTaskStageMatch(server.scheduler, taskName, source, pb.Stage_Running)
	}
	require.Equal(t.T(), sourceResps, stResp.Sources)
	// 1. query existing task's error
	errReq := &pb.GetValidationErrorRequest{
		TaskName: taskName,
		ErrState: pb.ValidateErrorState_InvalidErr,
	}
	resp, err := server.GetValidationError(context.Background(), errReq)
	require.NoError(t.T(), err)
	require.Equal(t.T(), "", resp.Msg)
	require.True(t.T(), resp.Result)
	require.Len(t.T(), resp.Error, 2)
	// 2. query invalid task's error
	errReq.TaskName = "invalid-task"
	resp, err = server.GetValidationError(context.Background(), errReq)
	require.NoError(t.T(), err)
	require.Contains(t.T(), resp.Msg, "cannot get subtask by task name")
	require.False(t.T(), resp.Result)
	// 3. query invalid state
	errReq.TaskName = taskName
	errReq.ErrState = pb.ValidateErrorState_ResolvedErr // invalid state
	resp, err = server.GetValidationError(context.Background(), errReq)
	require.NoError(t.T(), err)
	require.Contains(t.T(), resp.Msg, "only support querying `all`, `unprocessed`, and `ignored` error")
	require.False(t.T(), resp.Result)
	// 4. worker error
	errReq.TaskName = taskName
	errReq.ErrState = pb.ValidateErrorState_InvalidErr
	resp, err = server.GetValidationError(context.Background(), errReq)
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Contains(t.T(), resp.Msg, "something wrong in worker")
	// 5. grpc error
	resp, err = server.GetValidationError(context.Background(), errReq)
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Contains(t.T(), resp.Msg, "grpc error")
}

func (t *testMasterSuite) TestOperateValidationError() {
	var (
		wg       sync.WaitGroup
		taskName = "test"
	)
	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()
	server := testDefaultMasterServer(t.T())
	server.etcdClient = t.etcdTestCli
	sources, workers := defaultWorkerSource()
	startReq := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	// test query all workers
	for idx, worker := range workers {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().OperateValidatorError(
			gomock.Any(),
			gomock.Any(),
		).Return(&pb.OperateValidationErrorResponse{
			Result: true,
			Msg:    "",
		}, nil)
		mockWorkerClient.EXPECT().OperateValidatorError(
			gomock.Any(),
			gomock.Any(),
		).Return(&pb.OperateValidationErrorResponse{
			Result: false,
			Msg:    "something wrong in worker",
		}, nil)
		mockWorkerClient.EXPECT().OperateValidatorError(
			gomock.Any(),
			gomock.Any(),
		).Return(&pb.OperateValidationErrorResponse{}, errors.New("grpc error"))
		mockRevelantWorkerClient(mockWorkerClient, taskName, sources[idx], startReq)
		t.workerClients[worker] = newMockRPCClient(mockWorkerClient)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer t.clearSchedulerEnv(cancel, &wg)
	// start task without validation
	sourceResps := []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}, {Result: true, Source: sources[1]}}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "", t.workerClients)
	mock := conn.InitVersionDB()
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	stResp, err := server.StartTask(context.Background(), startReq)
	require.NoError(t.T(), err)
	require.True(t.T(), stResp.Result)

	for _, source := range sources {
		t.subTaskStageMatch(server.scheduler, taskName, source, pb.Stage_Running)
	}
	require.Equal(t.T(), sourceResps, stResp.Sources)
	// 1. query existing task's error
	opReq := &pb.OperateValidationErrorRequest{
		TaskName:   taskName,
		IsAllError: true,
	}
	resp, err := server.OperateValidationError(context.Background(), opReq)
	require.NoError(t.T(), err)
	require.Equal(t.T(), resp.Msg, "")
	require.True(t.T(), resp.Result)
	// 2. query invalid task's error
	opReq.TaskName = "invalid-task"
	resp, err = server.OperateValidationError(context.Background(), opReq)
	require.NoError(t.T(), err)
	require.Contains(t.T(), resp.Msg, "cannot get subtask by task name")
	require.False(t.T(), resp.Result)
	// 3. worker error
	opReq.TaskName = taskName
	resp, err = server.OperateValidationError(context.Background(), opReq)
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Contains(t.T(), resp.Msg, "something wrong in worker")
	// 4. grpc error
	opReq.TaskName = taskName
	resp, err = server.OperateValidationError(context.Background(), opReq)
	require.NoError(t.T(), err)
	require.False(t.T(), resp.Result)
	require.Contains(t.T(), resp.Msg, "grpc error")
}

func (t *testMasterSuite) TestDashboardAddress() {
	// Temp file for test log output
	file, err := ioutil.TempFile(t.T().TempDir(), "*")
	require.NoError(t.T(), err)
	defer os.Remove(file.Name())

	cfg := NewConfig()
	err = cfg.FromContent(SampleConfig)
	require.NoError(t.T(), err)

	err = log.InitLogger(&log.Config{
		File: file.Name(),
	})
	require.NoError(t.T(), err)
	defer func() {
		err = log.InitLogger(&log.Config{})
		require.NoError(t.T(), err)
	}()

	cfg.OpenAPI = true
	cfg.LogFile = file.Name()
	cfg.DataDir = t.T().TempDir()

	server := NewServer(cfg)
	server.leader.Store(oneselfLeader)
	ctx, cancel := context.WithCancel(context.Background())
	go server.ap.Start(ctx)
	go func() {
		err2 := server.Start(ctx)
		require.NoError(t.T(), err2)
	}()
	defer server.Close()
	defer cancel()

	// Wait server bootstraped.
	time.Sleep(time.Second * 3)

	content, err := ioutil.ReadFile(file.Name())
	require.NoError(t.T(), err)
	require.Contains(t.T(), string(content), "Web UI enabled")
}

func (t *testMasterSuite) TestGetLatestMeta() {
	_, mockDB, err := conn.InitMockDBFull()
	require.NoError(t.T(), err)
	getMasterStatusError := errors.New("failed to get master status")
	mockDB.ExpectQuery(`SHOW MASTER STATUS`).WillReturnError(getMasterStatusError)
	meta, err := GetLatestMeta(context.Background(), "", &dbconfig.DBConfig{})
	require.Contains(t.T(), err.Error(), getMasterStatusError.Error())
	require.Nil(t.T(), meta)

	_, mockDB, err = conn.InitMockDBFull()
	require.NoError(t.T(), err)
	rows := mockDB.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"})
	mockDB.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)
	meta, err = GetLatestMeta(context.Background(), "", &dbconfig.DBConfig{})
	require.True(t.T(), terror.ErrNoMasterStatus.Equal(err))
	require.Nil(t.T(), meta)

	_, mockDB, err = conn.InitMockDBFull()
	require.NoError(t.T(), err)
	// 5 columns for MySQL
	rows = mockDB.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).AddRow(
		"mysql-bin.000009", 11232, "do_db", "ignore_db", "",
	)
	mockDB.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)
	meta, err = GetLatestMeta(context.Background(), mysql.MySQLFlavor, &dbconfig.DBConfig{})
	require.NoError(t.T(), err)
	require.Equal(t.T(), meta.BinLogName, "mysql-bin.000009")
	require.Equal(t.T(), meta.BinLogPos, uint32(11232))
	require.Equal(t.T(), meta.BinLogGTID, "")

	_, mockDB, err = conn.InitMockDBFull()
	require.NoError(t.T(), err)
	// 4 columns for MariaDB
	rows = mockDB.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).AddRow(
		"mysql-bin.000009", 11232, "do_db", "ignore_db",
	)
	mockDB.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)
	rows = mockDB.NewRows([]string{"Variable_name", "Value"}).AddRow("gtid_binlog_pos", "1-2-100")
	mockDB.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'gtid_binlog_pos'`).WillReturnRows(rows)
	meta, err = GetLatestMeta(context.Background(), mysql.MariaDBFlavor, &dbconfig.DBConfig{})
	require.NoError(t.T(), err)
	require.Equal(t.T(), meta.BinLogName, "mysql-bin.000009")
	require.Equal(t.T(), meta.BinLogPos, uint32(11232))
	require.Equal(t.T(), meta.BinLogGTID, "1-2-100")
}
