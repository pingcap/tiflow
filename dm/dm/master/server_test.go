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
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	tidbmock "github.com/pingcap/tidb/util/mock"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"
	"google.golang.org/grpc"

	"github.com/pingcap/tiflow/dm/checker"
	common2 "github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/master/scheduler"
	"github.com/pingcap/tiflow/dm/dm/master/shardddl"
	"github.com/pingcap/tiflow/dm/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/pbmock"
	"github.com/pingcap/tiflow/dm/openapi/fixtures"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
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
    column-mapping-rules: ["instance-1"]
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

  - source-id: "mysql-replica-02"
    block-allow-list:  "instance"
    route-rules: ["sharding-route-rules-table", "sharding-route-rules-schema"]
    column-mapping-rules: ["instance-2"]
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

column-mappings:
  instance-1:
    schema-pattern: "sharding*"
    table-pattern: "t*"
    expression: "partition id"
    source-column: "id"
    target-column: "id"
    arguments: ["1", "sharding", "t"]

  instance-2:
    schema-pattern: "sharding*"
    table-pattern: "t*"
    expression: "partition id"
    source-column: "id"
    target-column: "id"
    arguments: ["2", "sharding", "t"]

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

type testMaster struct {
	workerClients   map[string]workerrpc.Client
	saveMaxRetryNum int
	testT           *testing.T

	testEtcdCluster *integration.ClusterV3
	etcdTestCli     *clientv3.Client
}

var testSuite = check.Suite(&testMaster{})

func TestMaster(t *testing.T) {
	err := log.InitLogger(&log.Config{})
	if err != nil {
		t.Fatal(err)
	}
	// inject *testing.T to testMaster
	s := testSuite.(*testMaster)
	s.testT = t

	// inject *testing.T to openAPISuite
	os := openAPITestSuite.(*openAPISuite)
	os.testT = t

	check.TestingT(t)
}

func (t *testMaster) SetUpSuite(c *check.C) {
	err := log.InitLogger(&log.Config{})
	c.Assert(err, check.IsNil)
	t.workerClients = make(map[string]workerrpc.Client)
	t.saveMaxRetryNum = maxRetryNum
	maxRetryNum = 2
	checkAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
}

func (t *testMaster) TearDownSuite(c *check.C) {
	maxRetryNum = t.saveMaxRetryNum
	checkAndAdjustSourceConfigFunc = checkAndAdjustSourceConfig
}

func (t *testMaster) SetUpTest(c *check.C) {
	t.testEtcdCluster = integration.NewClusterV3(t.testT, &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.testEtcdCluster.RandClient()
	t.clearEtcdEnv(c)
}

func (t *testMaster) TearDownTest(c *check.C) {
	t.clearEtcdEnv(c)
	t.testEtcdCluster.Terminate(t.testT)
}

func (t *testMaster) clearEtcdEnv(c *check.C) {
	c.Assert(ha.ClearTestInfoOperation(t.etcdTestCli), check.IsNil)
}

func (t testMaster) clearSchedulerEnv(c *check.C, cancel context.CancelFunc, wg *sync.WaitGroup) {
	cancel()
	wg.Wait()
	t.clearEtcdEnv(c)
}

func stageDeepEqualExcludeRev(c *check.C, stage, expectStage ha.Stage) {
	expectStage.Revision = stage.Revision
	c.Assert(stage, check.DeepEquals, expectStage)
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
		case pb.TaskOp_Stop:
			expect = pb.Stage_Stopped
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
		if expect == pb.Stage_Stopped {
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

func createTableInfo(c *check.C, p *parser.Parser, se sessionctx.Context, tableID int64, sql string) *model.TableInfo {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		c.Fatalf("fail to parse stmt, %v", err)
	}
	createStmtNode, ok := node.(*ast.CreateTableStmt)
	if !ok {
		c.Fatalf("%s is not a CREATE TABLE statement", sql)
	}
	info, err := tiddl.MockTableInfo(se, createStmtNode, tableID)
	if err != nil {
		c.Fatalf("fail to create table info, %v", err)
	}
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

func testDefaultMasterServer(c *check.C) *Server {
	cfg := NewConfig()
	err := cfg.Parse([]string{"-config=./dm-master.toml"})
	c.Assert(err, check.IsNil)
	cfg.DataDir = c.MkDir()
	server := NewServer(cfg)
	server.leader.Store(oneselfLeader)
	go server.ap.Start(context.Background())

	return server
}

func (t *testMaster) testMockScheduler(ctx context.Context, wg *sync.WaitGroup, c *check.C, sources, workers []string, password string, workerClients map[string]workerrpc.Client) (*scheduler.Scheduler, []context.CancelFunc) {
	logger := log.L()
	scheduler2 := scheduler.NewScheduler(&logger, config.Security{})
	err := scheduler2.Start(ctx, t.etcdTestCli)
	c.Assert(err, check.IsNil)
	cancels := make([]context.CancelFunc, 0, 2)
	for i := range workers {
		// add worker to scheduler's workers map
		name := workers[i]
		c.Assert(scheduler2.AddWorker(name, workers[i]), check.IsNil)
		scheduler2.SetWorkerClientForTest(name, workerClients[workers[i]])
		// operate mysql config on this worker
		cfg := config.NewSourceConfig()
		cfg.SourceID = sources[i]
		cfg.From.Password = password
		c.Assert(scheduler2.AddSourceCfg(cfg), check.IsNil, check.Commentf("all sources: %v", sources))
		wg.Add(1)
		ctx1, cancel1 := context.WithCancel(ctx)
		cancels = append(cancels, cancel1)
		go func(ctx context.Context, workerName string) {
			defer wg.Done()
			c.Assert(ha.KeepAlive(ctx, t.etcdTestCli, workerName, keepAliveTTL), check.IsNil)
		}(ctx1, name)
		idx := i
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			w := scheduler2.GetWorkerBySource(sources[idx])
			return w != nil && w.BaseInfo().Name == name
		}), check.IsTrue)
	}
	return scheduler2, cancels
}

func (t *testMaster) testMockSchedulerForRelay(ctx context.Context, wg *sync.WaitGroup, c *check.C, sources, workers []string, password string, workerClients map[string]workerrpc.Client) (*scheduler.Scheduler, []context.CancelFunc) {
	logger := log.L()
	scheduler2 := scheduler.NewScheduler(&logger, config.Security{})
	err := scheduler2.Start(ctx, t.etcdTestCli)
	c.Assert(err, check.IsNil)
	cancels := make([]context.CancelFunc, 0, 2)
	for i := range workers {
		// add worker to scheduler's workers map
		name := workers[i]
		c.Assert(scheduler2.AddWorker(name, workers[i]), check.IsNil)
		scheduler2.SetWorkerClientForTest(name, workerClients[workers[i]])
		// operate mysql config on this worker
		cfg := config.NewSourceConfig()
		cfg.SourceID = sources[i]
		cfg.From.Password = password
		c.Assert(scheduler2.AddSourceCfg(cfg), check.IsNil, check.Commentf("all sources: %v", sources))
		wg.Add(1)
		ctx1, cancel1 := context.WithCancel(ctx)
		cancels = append(cancels, cancel1)
		go func(ctx context.Context, workerName string) {
			defer wg.Done()
			c.Assert(ha.KeepAlive(ctx, t.etcdTestCli, workerName, keepAliveTTL), check.IsNil)
		}(ctx1, name)

		// wait the mock worker has alive
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			resp, err2 := t.etcdTestCli.Get(ctx, common2.WorkerKeepAliveKeyAdapter.Encode(name))
			c.Assert(err2, check.IsNil)
			return resp.Count == 1
		}), check.IsTrue)

		c.Assert(scheduler2.StartRelay(sources[i], []string{workers[i]}), check.IsNil)
		idx := i
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			relayWorkers, err2 := scheduler2.GetRelayWorkers(sources[idx])
			c.Assert(err2, check.IsNil)
			return len(relayWorkers) == 1 && relayWorkers[0].BaseInfo().Name == name
		}), check.IsTrue)
	}
	return scheduler2, cancels
}

func generateServerConfig(c *check.C, name string) *Config {
	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg1.Name = name
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.AdvertiseAddr = cfg1.MasterAddr
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)
	return cfg1
}

func (t *testMaster) TestQueryStatus(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
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
	server.scheduler, cancels = t.testMockScheduler(ctx, &wg, c, sources, workers, "", t.workerClients)
	for _, cancelFunc := range cancels {
		defer cancelFunc()
	}
	resp, err := server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	t.clearSchedulerEnv(c, cancel, &wg)

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
	server.scheduler, cancels = t.testMockSchedulerForRelay(ctx, &wg, c, sources, workers, "passwd", t.workerClients)
	for _, cancelFunc := range cancels {
		defer cancelFunc()
	}
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Sources: sources,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// query with invalid dm-worker[s]
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Sources: []string{"invalid-source1", "invalid-source2"},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, "sources .* haven't been added")

	// query with invalid task name
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Name: "invalid-task-name",
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, "task .* has no source or not exist")
	t.clearSchedulerEnv(c, cancel, &wg)
	// TODO: test query with correct task name, this needs to add task first
}

func (t *testMaster) TestWaitOperationOkRightResult(c *check.C) {
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

	ctrl := gomock.NewController(c)
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
		c.Assert(err, check.IsNil)
		c.Assert(ok, check.Equals, ca.expectedOK)
		c.Assert(msg == "", check.Equals, ca.expectedEmptyMsg)
	}
}

func (t *testMaster) TestStopTaskWithExceptRight(c *check.C) {
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
		Op:   pb.TaskOp_Stop,
		Name: taskName,
	}
	ctrl := gomock.NewController(c)
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
		c.Assert(err, check.IsNil)
		c.Assert(ok, check.IsTrue)
		c.Assert(msg, check.HasLen, 0)
	}
}

func (t *testMaster) TestFillUnsyncedStatus(c *check.C) {
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
		c.Assert(s.pessimist.Start(context.Background(), t.etcdTestCli), check.IsNil)
		for _, i := range ca.infos {
			_, err := pessimism.PutInfo(t.etcdTestCli, i)
			c.Assert(err, check.IsNil)
		}
		if len(ca.infos) > 0 {
			utils.WaitSomething(20, 100*time.Millisecond, func() bool {
				return len(s.pessimist.ShowLocks("", nil)) > 0
			})
		}

		s.fillUnsyncedStatus(ca.input)
		c.Assert(ca.input, check.DeepEquals, ca.expected)
		_, err := pessimism.DeleteInfosOperations(t.etcdTestCli, ca.infos, nil)
		c.Assert(err, check.IsNil)
	}
}

func (t *testMaster) TestCheckTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	sources, workers := defaultWorkerSource()

	t.workerClients = makeNilWorkerClients(workers)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, c, sources, workers, "", t.workerClients)
	mock := conn.InitVersionDB(c)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err := server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// decode task with error
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: "invalid toml config",
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	t.clearSchedulerEnv(c, cancel, &wg)

	// simulate invalid password returned from scheduler, but config was supported plaintext mysql password, so cfg.SubTaskConfigs will success
	ctx, cancel = context.WithCancel(context.Background())
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, c, sources, workers, "invalid-encrypt-password", t.workerClients)
	mock = conn.InitVersionDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	t.clearSchedulerEnv(c, cancel, &wg)
}

func (t *testMaster) TestStartTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	server.etcdClient = t.etcdTestCli
	sources, workers := defaultWorkerSource()

	// s.generateSubTask with error
	resp, err := server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task: "invalid toml config",
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)

	// test start task successfully
	var wg sync.WaitGroup
	// taskName is relative to taskConfig
	taskName := "test"
	ctx, cancel := context.WithCancel(context.Background())
	req := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, c, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, req))
	mock := conn.InitVersionDB(c)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err = server.StartTask(context.Background(), req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	for _, source := range sources {
		t.subTaskStageMatch(c, server.scheduler, taskName, source, pb.Stage_Running)
		tcm, _, err2 := ha.GetSubTaskCfg(t.etcdTestCli, source, taskName, 0)
		c.Assert(err2, check.IsNil)
		c.Assert(tcm, check.HasKey, taskName)
		c.Assert(tcm[taskName].Name, check.Equals, taskName)
		c.Assert(tcm[taskName].SourceID, check.Equals, source)
	}

	// check start-task with an invalid source
	invalidSource := "invalid-source"
	mock = conn.InitVersionDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: []string{invalidSource},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Sources, check.HasLen, 1)
	c.Assert(resp.Sources[0].Result, check.IsFalse)
	c.Assert(resp.Sources[0].Source, check.Equals, invalidSource)

	// test start task, but the first step check-task fails
	bakCheckSyncConfigFunc := checker.CheckSyncConfigFunc
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*config.SubTaskConfig, _, _ int64) (string, error) {
		return "", errors.New(errCheckSyncConfig)
	}
	defer func() {
		checker.CheckSyncConfigFunc = bakCheckSyncConfigFunc
	}()
	mock = conn.InitVersionDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, errCheckSyncConfigReg)
	t.clearSchedulerEnv(c, cancel, &wg)
}

func (t *testMaster) TestStartTaskWithRemoveMeta(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	sources, workers := defaultWorkerSource()
	server.etcdClient = t.etcdTestCli

	// test start task successfully
	var wg sync.WaitGroup
	// taskName is relative to taskConfig
	cfg := config.NewTaskConfig()
	err := cfg.Decode(taskConfig)
	c.Assert(err, check.IsNil)
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
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, c, sources, workers, "",
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
	c.Assert(err, check.IsNil)
	_, succ, err := pessimism.PutOperations(t.etcdTestCli, false, op2)
	c.Assert(succ, check.IsTrue)
	c.Assert(err, check.IsNil)

	c.Assert(server.pessimist.Start(ctx, t.etcdTestCli), check.IsNil)
	c.Assert(server.optimist.Start(ctx, t.etcdTestCli), check.IsNil)

	verMock := conn.InitVersionDB(c)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	verMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	mock := conn.InitMockDB(c)
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.LoaderCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.LightningCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerShardMeta(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	c.Assert(len(server.pessimist.Locks()), check.Greater, 0)

	resp, err := server.StartTask(context.Background(), req)
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Microsecond)
		// start another same task at the same time, should get err
		verMock2 := conn.InitVersionDB(c)
		verMock2.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("version", "5.7.25-TiDB-v4.0.2"))
		resp1, err1 := server.StartTask(context.Background(), req)
		c.Assert(err1, check.IsNil)
		c.Assert(resp1.Result, check.IsFalse)
		c.Assert(resp1.Msg, check.Equals, terror.Annotate(terror.ErrSchedulerSubTaskExist.Generate(cfg.Name, sources),
			"while remove-meta is true").Error())
	}()
	c.Assert(err, check.IsNil)
	if !resp.Result {
		c.Errorf("start task failed: %s", resp.Msg)
	}
	for _, source := range sources {
		t.subTaskStageMatch(c, server.scheduler, taskName, source, pb.Stage_Running)
		tcm, _, err2 := ha.GetSubTaskCfg(t.etcdTestCli, source, taskName, 0)
		c.Assert(err2, check.IsNil)
		c.Assert(tcm, check.HasKey, taskName)
		c.Assert(tcm[taskName].Name, check.Equals, taskName)
		c.Assert(tcm[taskName].SourceID, check.Equals, source)
	}

	c.Assert(server.pessimist.Locks(), check.HasLen, 0)
	if err = mock.ExpectationsWereMet(); err != nil {
		c.Errorf("db unfulfilled expectations: %s", err)
	}
	ifm, _, err := pessimism.GetAllInfo(t.etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(ifm, check.HasLen, 0)
	opm, _, err := pessimism.GetAllOperations(t.etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(opm, check.HasLen, 0)
	t.clearSchedulerEnv(c, cancel, &wg)

	// test remove meta with optimist
	ctx, cancel = context.WithCancel(context.Background())
	cfg.ShardMode = config.ShardOptimistic
	req = &pb.StartTaskRequest{
		Task:       strings.ReplaceAll(taskConfig, `shard-mode: ""`, fmt.Sprintf(`shard-mode: "%s"`, cfg.ShardMode)),
		Sources:    sources,
		RemoveMeta: true,
	}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, c, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, req))
	server.pessimist = shardddl.NewPessimist(&logger, func(task string) []string { return sources })
	server.optimist = shardddl.NewOptimist(&logger, server.scheduler.GetDownstreamMetaByTask)

	var (
		p           = parser.New()
		se          = tidbmock.NewContext()
		tblID int64 = 111

		st1      = optimism.NewSourceTables(taskName, sources[0])
		DDLs1    = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		tiBefore = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter1 = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		info1    = optimism.NewInfo(taskName, sources[0], "foo-1", "bar-1", schema, table, DDLs1, tiBefore, []*model.TableInfo{tiAfter1})
		op1      = optimism.NewOperation(ID, taskName, sources[0], info1.UpSchema, info1.UpTable, DDLs1, optimism.ConflictNone, "", false, []string{})
	)

	st1.AddTable("foo-1", "bar-1", schema, table)
	_, err = optimism.PutSourceTables(t.etcdTestCli, st1)
	c.Assert(err, check.IsNil)
	_, err = optimism.PutInfo(t.etcdTestCli, info1)
	c.Assert(err, check.IsNil)
	_, succ, err = optimism.PutOperation(t.etcdTestCli, false, op1, 0)
	c.Assert(succ, check.IsTrue)
	c.Assert(err, check.IsNil)

	err = server.pessimist.Start(ctx, t.etcdTestCli)
	c.Assert(err, check.IsNil)
	err = server.optimist.Start(ctx, t.etcdTestCli)
	c.Assert(err, check.IsNil)

	verMock = conn.InitVersionDB(c)
	verMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	mock = conn.InitMockDB(c)
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.LoaderCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.LightningCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerShardMeta(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	c.Assert(len(server.optimist.Locks()), check.Greater, 0)

	resp, err = server.StartTask(context.Background(), req)
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Microsecond)
		// start another same task at the same time, should get err
		vermock2 := conn.InitVersionDB(c)
		vermock2.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("version", "5.7.25-TiDB-v4.0.2"))
		resp1, err1 := server.StartTask(context.Background(), req)
		c.Assert(err1, check.IsNil)
		c.Assert(resp1.Result, check.IsFalse)
		c.Assert(resp1.Msg, check.Equals, terror.Annotate(terror.ErrSchedulerSubTaskExist.Generate(cfg.Name, sources),
			"while remove-meta is true").Error())
	}()
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	for _, source := range sources {
		t.subTaskStageMatch(c, server.scheduler, taskName, source, pb.Stage_Running)
		tcm, _, err2 := ha.GetSubTaskCfg(t.etcdTestCli, source, taskName, 0)
		c.Assert(err2, check.IsNil)
		c.Assert(tcm, check.HasKey, taskName)
		c.Assert(tcm[taskName].Name, check.Equals, taskName)
		c.Assert(tcm[taskName].SourceID, check.Equals, source)
	}

	c.Assert(server.optimist.Locks(), check.HasLen, 0)
	if err = mock.ExpectationsWereMet(); err != nil {
		c.Errorf("db unfulfilled expectations: %s", err)
	}
	ifm2, _, err := optimism.GetAllInfo(t.etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(ifm2, check.HasLen, 0)
	opm2, _, err := optimism.GetAllOperations(t.etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(opm2, check.HasLen, 0)
	tbm, _, err := optimism.GetAllSourceTables(t.etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(tbm, check.HasLen, 0)

	t.clearSchedulerEnv(c, cancel, &wg)
}

func (t *testMaster) TestOperateTask(c *check.C) {
	var (
		taskName = "unit-test-task"
		pauseOp  = pb.TaskOp_Pause
	)

	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	server := testDefaultMasterServer(c)
	server.etcdClient = t.etcdTestCli
	sources, workers := defaultWorkerSource()

	// test operate-task with invalid task name
	resp, err := server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pauseOp,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Equals, fmt.Sprintf("task %s has no source or not exist, please check the task name and status", taskName))

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
		Op:      pb.TaskOp_Stop,
		Name:    taskName,
		Sources: []string{sources[0]},
	}
	stopReq2 := &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Stop,
		Name: taskName,
	}
	sourceResps := []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}, {Result: true, Source: sources[1]}}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, c, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, startReq, pauseReq, resumeReq, stopReq1, stopReq2))
	mock := conn.InitVersionDB(c)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	stResp, err := server.StartTask(context.Background(), startReq)
	c.Assert(err, check.IsNil)
	c.Assert(stResp.Result, check.IsTrue)
	for _, source := range sources {
		t.subTaskStageMatch(c, server.scheduler, taskName, source, pb.Stage_Running)
	}
	c.Assert(stResp.Sources, check.DeepEquals, sourceResps)
	// 2. pause task
	resp, err = server.OperateTask(context.Background(), pauseReq)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	for _, source := range sources {
		t.subTaskStageMatch(c, server.scheduler, taskName, source, pb.Stage_Paused)
	}
	c.Assert(resp.Sources, check.DeepEquals, sourceResps)
	// 3. resume task
	resp, err = server.OperateTask(context.Background(), resumeReq)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	for _, source := range sources {
		t.subTaskStageMatch(c, server.scheduler, taskName, source, pb.Stage_Running)
	}
	c.Assert(resp.Sources, check.DeepEquals, sourceResps)
	// 4. test stop task successfully, remove partial sources
	resp, err = server.OperateTask(context.Background(), stopReq1)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(server.getTaskResources(taskName), check.DeepEquals, []string{sources[1]})
	c.Assert(resp.Sources, check.DeepEquals, []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}})
	// 5. test stop task successfully, remove all workers
	resp, err = server.OperateTask(context.Background(), stopReq2)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(len(server.getTaskResources(taskName)), check.Equals, 0)
	c.Assert(resp.Sources, check.DeepEquals, []*pb.CommonWorkerResponse{{Result: true, Source: sources[1]}})
	t.clearSchedulerEnv(c, cancel, &wg)
}

func (t *testMaster) TestPurgeWorkerRelay(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
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
	server.scheduler, _ = t.testMockSchedulerForRelay(ctx, &wg, c, nil, nil, "", t.workerClients)

	// test PurgeWorkerRelay with invalid dm-worker[s]
	resp, err := server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Sources:  []string{"invalid-source1", "invalid-source2"},
		Time:     now,
		Filename: filename,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, "relay worker for source .* not found.*")
	}
	t.clearSchedulerEnv(c, cancel, &wg)

	ctx, cancel = context.WithCancel(context.Background())
	// test PurgeWorkerRelay successfully
	mockPurgeRelay(true)
	server.scheduler, _ = t.testMockSchedulerForRelay(ctx, &wg, c, sources, workers, "", t.workerClients)
	resp, err = server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Sources:  sources,
		Time:     now,
		Filename: filename,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
		c.Assert(w.Result, check.IsTrue)
	}
	t.clearSchedulerEnv(c, cancel, &wg)

	ctx, cancel = context.WithCancel(context.Background())
	// test PurgeWorkerRelay with error response
	mockPurgeRelay(false)
	server.scheduler, _ = t.testMockSchedulerForRelay(ctx, &wg, c, sources, workers, "", t.workerClients)
	resp, err = server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Sources:  sources,
		Time:     now,
		Filename: filename,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, errGRPCFailedReg)
	}
	t.clearSchedulerEnv(c, cancel, &wg)
}

func (t *testMaster) TestOperateWorkerRelayTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
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
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, c, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, "", sources, workers, pauseReq, resumeReq))

	// test OperateWorkerRelayTask with invalid dm-worker[s]
	resp, err := server.OperateWorkerRelayTask(context.Background(), &pb.OperateWorkerRelayRequest{
		Sources: []string{"invalid-source1", "invalid-source2"},
		Op:      pb.RelayOp_PauseRelay,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, `[\s\S]*need to update expectant relay stage not exist[\s\S]*`)

	sourceResps := []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}, {Result: true, Source: sources[1]}}
	// 1. test pause-relay successfully
	resp, err = server.OperateWorkerRelayTask(context.Background(), pauseReq)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	for _, source := range sources {
		t.relayStageMatch(c, server.scheduler, source, pb.Stage_Paused)
	}
	c.Assert(resp.Sources, check.DeepEquals, sourceResps)
	// 2. test resume-relay successfully
	resp, err = server.OperateWorkerRelayTask(context.Background(), resumeReq)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	for _, source := range sources {
		t.relayStageMatch(c, server.scheduler, source, pb.Stage_Running)
	}
	c.Assert(resp.Sources, check.DeepEquals, sourceResps)
	t.clearSchedulerEnv(c, cancel, &wg)
}

func (t *testMaster) TestServer(c *check.C) {
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg.PeerUrls = "http://127.0.0.1:8294"
	cfg.DataDir = c.MkDir()
	cfg.MasterAddr = tempurl.Alloc()[len("http://"):]

	s := NewServer(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	err1 := s.Start(ctx)
	c.Assert(err1, check.IsNil)

	t.testHTTPInterface(c, fmt.Sprintf("http://%s/status", cfg.MasterAddr), []byte(utils.GetRawInfo()))
	t.testHTTPInterface(c, fmt.Sprintf("http://%s/debug/pprof/", cfg.MasterAddr), []byte("Types of profiles available"))
	// HTTP API in this unit test is unstable, but we test it in `http_apis` in integration test.
	// t.testHTTPInterface(c, fmt.Sprintf("http://%s/apis/v1alpha1/status/test-task", cfg.MasterAddr), []byte("task test-task has no source or not exist"))

	dupServer := NewServer(cfg)
	err := dupServer.Start(ctx)
	c.Assert(terror.ErrMasterStartEmbedEtcdFail.Equal(err), check.IsTrue)
	c.Assert(err.Error(), check.Matches, ".*bind: address already in use.*")

	// close
	cancel()
	s.Close()

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.closed.Load()
	}), check.IsTrue)
}

func (t *testMaster) TestMasterTLS(c *check.C) {
	masterAddr := tempurl.Alloc()[len("http://"):]
	peerAddr := tempurl.Alloc()[len("http://"):]

	// all with `https://` prefix
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", c.MkDir()),
		fmt.Sprintf("--master-addr=https://%s", masterAddr),
		fmt.Sprintf("--advertise-addr=https://%s", masterAddr),
		fmt.Sprintf("--peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=https://%s", peerAddr),
		"--ssl-ca=./tls_for_test/ca.pem",
		"--ssl-cert=./tls_for_test/dm.pem",
		"--ssl-key=./tls_for_test/dm.key",
	}), check.IsNil)
	t.testTLSPrefix(c, cfg)
	c.Assert(cfg.MasterAddr, check.Equals, masterAddr)
	c.Assert(cfg.AdvertiseAddr, check.Equals, masterAddr)
	c.Assert(cfg.PeerUrls, check.Equals, "https://"+peerAddr)
	c.Assert(cfg.AdvertisePeerUrls, check.Equals, "https://"+peerAddr)
	c.Assert(cfg.InitialCluster, check.Equals, "master-tls=https://"+peerAddr)

	// no `https://` prefix for `--master-addr`
	cfg = NewConfig()
	c.Assert(cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", c.MkDir()),
		fmt.Sprintf("--master-addr=%s", masterAddr),
		fmt.Sprintf("--advertise-addr=https://%s", masterAddr),
		fmt.Sprintf("--peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=https://%s", peerAddr),
		"--ssl-ca=./tls_for_test/ca.pem",
		"--ssl-cert=./tls_for_test/dm.pem",
		"--ssl-key=./tls_for_test/dm.key",
	}), check.IsNil)
	t.testTLSPrefix(c, cfg)

	// no `https://` prefix for `--master-addr` and `--advertise-addr`
	cfg = NewConfig()
	c.Assert(cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", c.MkDir()),
		fmt.Sprintf("--master-addr=%s", masterAddr),
		fmt.Sprintf("--advertise-addr=%s", masterAddr),
		fmt.Sprintf("--peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=https://%s", peerAddr),
		"--ssl-ca=./tls_for_test/ca.pem",
		"--ssl-cert=./tls_for_test/dm.pem",
		"--ssl-key=./tls_for_test/dm.key",
	}), check.IsNil)
	t.testTLSPrefix(c, cfg)

	// no `https://` prefix for `--master-addr`, `--advertise-addr` and `--peer-urls`
	cfg = NewConfig()
	c.Assert(cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", c.MkDir()),
		fmt.Sprintf("--master-addr=%s", masterAddr),
		fmt.Sprintf("--advertise-addr=%s", masterAddr),
		fmt.Sprintf("--peer-urls=%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=https://%s", peerAddr),
		"--ssl-ca=./tls_for_test/ca.pem",
		"--ssl-cert=./tls_for_test/dm.pem",
		"--ssl-key=./tls_for_test/dm.key",
	}), check.IsNil)
	t.testTLSPrefix(c, cfg)

	// no `https://` prefix for `--master-addr`, `--advertise-addr`, `--peer-urls` and `--advertise-peer-urls`
	cfg = NewConfig()
	c.Assert(cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", c.MkDir()),
		fmt.Sprintf("--master-addr=%s", masterAddr),
		fmt.Sprintf("--advertise-addr=%s", masterAddr),
		fmt.Sprintf("--peer-urls=%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=https://%s", peerAddr),
		"--ssl-ca=./tls_for_test/ca.pem",
		"--ssl-cert=./tls_for_test/dm.pem",
		"--ssl-key=./tls_for_test/dm.key",
	}), check.IsNil)
	t.testTLSPrefix(c, cfg)

	// all without `https://`/`http://` prefix
	cfg = NewConfig()
	c.Assert(cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", c.MkDir()),
		fmt.Sprintf("--master-addr=%s", masterAddr),
		fmt.Sprintf("--advertise-addr=%s", masterAddr),
		fmt.Sprintf("--peer-urls=%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=%s", peerAddr),
		"--ssl-ca=./tls_for_test/ca.pem",
		"--ssl-cert=./tls_for_test/dm.pem",
		"--ssl-key=./tls_for_test/dm.key",
	}), check.IsNil)
	t.testTLSPrefix(c, cfg)
	c.Assert(cfg.MasterAddr, check.Equals, masterAddr)
	c.Assert(cfg.AdvertiseAddr, check.Equals, masterAddr)
	c.Assert(cfg.PeerUrls, check.Equals, "https://"+peerAddr)
	c.Assert(cfg.AdvertisePeerUrls, check.Equals, "https://"+peerAddr)
	c.Assert(cfg.InitialCluster, check.Equals, "master-tls=https://"+peerAddr)

	// all with `http://` prefix, but with TLS enabled.
	cfg = NewConfig()
	c.Assert(cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", c.MkDir()),
		fmt.Sprintf("--master-addr=http://%s", masterAddr),
		fmt.Sprintf("--advertise-addr=http://%s", masterAddr),
		fmt.Sprintf("--peer-urls=http://%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=http://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=http://%s", peerAddr),
		"--ssl-ca=./tls_for_test/ca.pem",
		"--ssl-cert=./tls_for_test/dm.pem",
		"--ssl-key=./tls_for_test/dm.key",
	}), check.IsNil)
	c.Assert(cfg.MasterAddr, check.Equals, masterAddr)
	c.Assert(cfg.AdvertiseAddr, check.Equals, masterAddr)
	c.Assert(cfg.PeerUrls, check.Equals, "https://"+peerAddr)
	c.Assert(cfg.AdvertisePeerUrls, check.Equals, "https://"+peerAddr)
	c.Assert(cfg.InitialCluster, check.Equals, "master-tls=https://"+peerAddr)

	// different prefix for `--peer-urls` and `--initial-cluster`
	cfg = NewConfig()
	c.Assert(cfg.Parse([]string{
		"--name=master-tls",
		fmt.Sprintf("--data-dir=%s", c.MkDir()),
		fmt.Sprintf("--master-addr=https://%s", masterAddr),
		fmt.Sprintf("--advertise-addr=https://%s", masterAddr),
		fmt.Sprintf("--peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--advertise-peer-urls=https://%s", peerAddr),
		fmt.Sprintf("--initial-cluster=master-tls=http://%s", peerAddr),
		"--ssl-ca=./tls_for_test/ca.pem",
		"--ssl-cert=./tls_for_test/dm.pem",
		"--ssl-key=./tls_for_test/dm.key",
	}), check.IsNil)
	c.Assert(cfg.MasterAddr, check.Equals, masterAddr)
	c.Assert(cfg.AdvertiseAddr, check.Equals, masterAddr)
	c.Assert(cfg.PeerUrls, check.Equals, "https://"+peerAddr)
	c.Assert(cfg.AdvertisePeerUrls, check.Equals, "https://"+peerAddr)
	c.Assert(cfg.InitialCluster, check.Equals, "master-tls=https://"+peerAddr)
	t.testTLSPrefix(c, cfg)
}

func (t *testMaster) testTLSPrefix(c *check.C, cfg *Config) {
	s := NewServer(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	err1 := s.Start(ctx)
	c.Assert(err1, check.IsNil)

	t.testHTTPInterface(c, fmt.Sprintf("https://%s/status", cfg.AdvertiseAddr), []byte(utils.GetRawInfo()))
	t.testHTTPInterface(c, fmt.Sprintf("https://%s/debug/pprof/", cfg.AdvertiseAddr), []byte("Types of profiles available"))

	// close
	cancel()
	s.Close()

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.closed.Load()
	}), check.IsTrue)
}

func (t *testMaster) testHTTPInterface(c *check.C, url string, contain []byte) {
	// we use HTTPS in some test cases.
	tls, err := toolutils.NewTLS("./tls_for_test/ca.pem", "./tls_for_test/dm.pem", "./tls_for_test/dm.key", url, []string{})
	c.Assert(err, check.IsNil)
	cli := toolutils.ClientWithTLS(tls.TLSConfig())

	// nolint:noctx
	resp, err := cli.Get(url)
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.Equals, 200)

	body, err := io.ReadAll(resp.Body)
	c.Assert(err, check.IsNil)
	c.Assert(bytes.Contains(body, contain), check.IsTrue)
}

func (t *testMaster) TestJoinMember(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

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

	client, err := etcdutil.CreateClient(strings.Split(cfg1.AdvertisePeerUrls, ","), nil)
	c.Assert(err, check.IsNil)
	defer client.Close()

	// verify members
	listResp, err := etcdutil.ListMembers(client)
	c.Assert(err, check.IsNil)
	c.Assert(listResp.Members, check.HasLen, 2)
	names := make(map[string]struct{}, len(listResp.Members))
	for _, m := range listResp.Members {
		names[m.Name] = struct{}{}
	}
	c.Assert(names, check.HasKey, cfg1.Name)
	c.Assert(names, check.HasKey, cfg2.Name)

	// s1 is still the leader
	_, leaderID, _, err := s2.election.LeaderInfo(ctx)

	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, cfg1.Name)

	cfg3 := NewConfig()
	c.Assert(cfg3.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg3.Name = "dm-master-3"
	cfg3.DataDir = c.MkDir()
	cfg3.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg3.PeerUrls = tempurl.Alloc()
	cfg3.AdvertisePeerUrls = cfg3.PeerUrls
	cfg3.Join = cfg1.MasterAddr // join to an existing cluster

	// mock join master without wal dir
	c.Assert(os.Mkdir(filepath.Join(cfg3.DataDir, "member"), privateDirMode), check.IsNil)
	c.Assert(os.Mkdir(filepath.Join(cfg3.DataDir, "member", "join"), privateDirMode), check.IsNil)
	s3 := NewServer(cfg3)
	// avoid join a unhealthy cluster
	c.Assert(utils.WaitSomething(30, 1000*time.Millisecond, func() bool {
		return s3.Start(ctx) == nil
	}), check.IsTrue)
	defer s3.Close()

	// verify members
	listResp, err = etcdutil.ListMembers(client)
	c.Assert(err, check.IsNil)
	c.Assert(listResp.Members, check.HasLen, 3)
	names = make(map[string]struct{}, len(listResp.Members))
	for _, m := range listResp.Members {
		names[m.Name] = struct{}{}
	}
	c.Assert(names, check.HasKey, cfg1.Name)
	c.Assert(names, check.HasKey, cfg2.Name)
	c.Assert(names, check.HasKey, cfg3.Name)

	cancel()
	t.clearEtcdEnv(c)
}

func (t *testMaster) TestOperateSource(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.AdvertiseAddr = tempurl.Alloc()[len("http://"):]
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 := NewServer(cfg1)
	s1.leader.Store(oneselfLeader)
	c.Assert(s1.Start(ctx), check.IsNil)
	defer s1.Close()
	mysqlCfg, err := config.LoadFromFile("./source.yaml")
	c.Assert(err, check.IsNil)
	mysqlCfg.From.Password = os.Getenv("MYSQL_PSWD")
	task, err := mysqlCfg.Yaml()
	c.Assert(err, check.IsNil)
	sourceID := mysqlCfg.SourceID
	// 1. wait for scheduler to start
	time.Sleep(3 * time.Second)

	// 2. try to add a new mysql source
	req := &pb.OperateSourceRequest{Op: pb.SourceOp_StartSource, Config: []string{task}}
	resp, err := s1.OperateSource(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, true)
	c.Assert(resp.Sources, check.DeepEquals, []*pb.CommonWorkerResponse{{
		Result: true,
		Msg:    "source is added but there is no free worker to bound",
		Source: sourceID,
	}})
	unBoundSources := s1.scheduler.UnboundSources()
	c.Assert(unBoundSources, check.HasLen, 1)
	c.Assert(unBoundSources[0], check.Equals, sourceID)

	// 3. try to add multiple source
	// 3.1 duplicated source id
	sourceID2 := "mysql-replica-02"
	mysqlCfg.SourceID = sourceID2
	task2, err := mysqlCfg.Yaml()
	c.Assert(err, check.IsNil)
	req = &pb.OperateSourceRequest{Op: pb.SourceOp_StartSource, Config: []string{task2, task2}}
	resp, err = s1.OperateSource(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, false)
	c.Assert(resp.Msg, check.Matches, ".*source config with ID "+sourceID2+" already exists.*")
	// 3.2 run same command after correction
	sourceID3 := "mysql-replica-03"
	mysqlCfg.SourceID = sourceID3
	task3, err := mysqlCfg.Yaml()
	c.Assert(err, check.IsNil)
	req = &pb.OperateSourceRequest{Op: pb.SourceOp_StartSource, Config: []string{task2, task3}}
	resp, err = s1.OperateSource(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, true)
	sort.Slice(resp.Sources, func(i, j int) bool {
		return resp.Sources[i].Source < resp.Sources[j].Source
	})
	c.Assert(resp.Sources, check.DeepEquals, []*pb.CommonWorkerResponse{{
		Result: true,
		Msg:    "source is added but there is no free worker to bound",
		Source: sourceID2,
	}, {
		Result: true,
		Msg:    "source is added but there is no free worker to bound",
		Source: sourceID3,
	}})
	unBoundSources = s1.scheduler.UnboundSources()
	c.Assert(unBoundSources, check.HasLen, 3)
	c.Assert(unBoundSources[0], check.Equals, sourceID)
	c.Assert(unBoundSources[1], check.Equals, sourceID2)
	c.Assert(unBoundSources[2], check.Equals, sourceID3)

	// 4. try to stop a non-exist-source
	req.Op = pb.SourceOp_StopSource
	mysqlCfg.SourceID = "not-exist-source"
	task4, err := mysqlCfg.Yaml()
	c.Assert(err, check.IsNil)
	req.Config = []string{task4}
	resp, err = s1.OperateSource(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, false)
	c.Assert(resp.Msg, check.Matches, `[\s\S]*source config with ID `+mysqlCfg.SourceID+` not exists[\s\S]*`)

	// 5. start workers, the unbounded sources should be bounded
	var wg sync.WaitGroup
	workerName1 := "worker1"
	workerName2 := "worker2"
	workerName3 := "worker3"
	defer func() {
		t.clearSchedulerEnv(c, cancel, &wg)
	}()
	c.Assert(s1.scheduler.AddWorker(workerName1, "172.16.10.72:8262"), check.IsNil)
	wg.Add(1)
	go func(ctx context.Context, workerName string) {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL), check.IsNil)
	}(ctx, workerName1)
	c.Assert(s1.scheduler.AddWorker(workerName2, "172.16.10.72:8263"), check.IsNil)
	wg.Add(1)
	go func(ctx context.Context, workerName string) {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL), check.IsNil)
	}(ctx, workerName2)
	c.Assert(s1.scheduler.AddWorker(workerName3, "172.16.10.72:8264"), check.IsNil)
	wg.Add(1)
	go func(ctx context.Context, workerName string) {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL), check.IsNil)
	}(ctx, workerName3)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		w := s1.scheduler.GetWorkerBySource(sourceID)
		return w != nil
	}), check.IsTrue)

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
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, true)
	c.Assert(resp.Sources, check.DeepEquals, []*pb.CommonWorkerResponse{{
		Result: true,
		Source: sourceID,
	}, {
		Result: true,
		Source: sourceID2,
	}, {
		Result: true,
		Source: sourceID3,
	}})
	scm, _, err := ha.GetSourceCfg(t.etcdTestCli, sourceID, 0)
	c.Assert(err, check.IsNil)
	c.Assert(scm, check.HasLen, 0)
	t.clearSchedulerEnv(c, cancel, &wg)

	cancel()
}

func (t *testMaster) TestOfflineMember(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg1 := generateServerConfig(c, "dm-master-1")
	cfg2 := generateServerConfig(c, "dm-master-2")
	cfg3 := generateServerConfig(c, "dm-master-3")

	initialCluster := fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls) + "," +
		fmt.Sprintf("%s=%s", cfg2.Name, cfg2.AdvertisePeerUrls) + "," +
		fmt.Sprintf("%s=%s", cfg3.Name, cfg3.AdvertisePeerUrls)
	cfg1.InitialCluster = initialCluster
	cfg2.InitialCluster = initialCluster
	cfg3.InitialCluster = initialCluster

	var wg sync.WaitGroup
	s1 := NewServer(cfg1)
	defer s1.Close()
	wg.Add(1)
	go func() {
		c.Assert(s1.Start(ctx), check.IsNil)
		wg.Done()
	}()

	s2 := NewServer(cfg2)
	defer s2.Close()
	wg.Add(1)
	go func() {
		c.Assert(s2.Start(ctx), check.IsNil)
		wg.Done()
	}()

	ctx3, cancel3 := context.WithCancel(ctx)
	s3 := NewServer(cfg3)
	c.Assert(s3.Start(ctx3), check.IsNil)
	defer s3.Close()
	defer cancel3()

	wg.Wait()

	var leaderID string
	// ensure s2 has got the right leader info, because it will be used to `OfflineMember`.
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
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
	}), check.IsTrue)

	// master related operations
	req := &pb.OfflineMemberRequest{
		Type: "masters",
		Name: "xixi",
	}
	// test offline member with wrong type
	resp, err := s2.OfflineMember(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Equals, terror.ErrMasterInvalidOfflineType.Generate(req.Type).Error())
	// test offline member with invalid master name
	req.Type = common.Master
	resp, err = s2.OfflineMember(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, `[\s\S]*dm-master with name `+req.Name+` not exists[\s\S]*`)
	// test offline member with correct master name
	cli := s2.etcdClient
	listResp, err := etcdutil.ListMembers(cli)
	c.Assert(err, check.IsNil)
	c.Assert(listResp.Members, check.HasLen, 3)

	// make sure s3 is not the leader, otherwise it will take some time to campaign a new leader after close s3, and it may cause timeout
	c.Assert(utils.WaitSomething(20, 500*time.Millisecond, func() bool {
		_, leaderID, _, err = s1.election.LeaderInfo(ctx)
		if err != nil {
			return false
		}

		if leaderID == s3.cfg.Name {
			_, err = s3.OperateLeader(ctx, &pb.OperateLeaderRequest{
				Op: pb.LeaderOp_EvictLeaderOp,
			})
			c.Assert(err, check.IsNil)
		}
		return leaderID != s3.cfg.Name
	}), check.IsTrue)

	cancel3()
	s3.Close()

	req.Name = s3.cfg.Name
	resp, err = s2.OfflineMember(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Msg, check.Equals, "")
	c.Assert(resp.Result, check.IsTrue)

	listResp, err = etcdutil.ListMembers(cli)
	c.Assert(err, check.IsNil)
	c.Assert(listResp.Members, check.HasLen, 2)
	if listResp.Members[0].Name == cfg2.Name {
		listResp.Members[0], listResp.Members[1] = listResp.Members[1], listResp.Members[0]
	}
	c.Assert(listResp.Members[0].Name, check.Equals, cfg1.Name)
	c.Assert(listResp.Members[1].Name, check.Equals, cfg2.Name)

	_, leaderID2, _, err := s1.election.LeaderInfo(ctx)
	c.Assert(err, check.IsNil)

	if leaderID == cfg3.Name {
		// s3 is leader before, leader should re-campaign
		c.Assert(leaderID != leaderID2, check.IsTrue)
	} else {
		// s3 isn't leader before, leader should keep the same
		c.Assert(leaderID, check.Equals, leaderID2)
	}

	// worker related operations
	ectx, canc := context.WithTimeout(ctx, time.Second)
	defer canc()
	req1 := &pb.RegisterWorkerRequest{
		Name:    "xixi",
		Address: "127.0.0.1:1000",
	}
	regReq, err := s1.RegisterWorker(ectx, req1)
	c.Assert(err, check.IsNil)
	c.Assert(regReq.Result, check.IsTrue)

	req2 := &pb.OfflineMemberRequest{
		Type: common.Worker,
		Name: "haha",
	}
	{
		res, err := s1.OfflineMember(ectx, req2)
		c.Assert(err, check.IsNil)
		c.Assert(res.Result, check.IsFalse)
		c.Assert(res.Msg, check.Matches, `[\s\S]*dm-worker with name `+req2.Name+` not exists[\s\S]*`)
	}
	{
		req2.Name = "xixi"
		res, err := s1.OfflineMember(ectx, req2)
		c.Assert(err, check.IsNil)
		c.Assert(res.Result, check.IsTrue)
	}
	{
		// register offline worker again. TICASE-962, 963
		resp, err := s1.RegisterWorker(ectx, req1)
		c.Assert(err, check.IsNil)
		c.Assert(resp.Result, check.IsTrue)
	}
	t.clearSchedulerEnv(c, cancel, &wg)
}

func (t *testMaster) TestGetCfg(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	sources, workers := defaultWorkerSource()

	var wg sync.WaitGroup
	taskName := "test"
	ctx, cancel := context.WithCancel(context.Background())
	req := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, c, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, req))
	server.etcdClient = t.etcdTestCli

	// start task
	mock := conn.InitVersionDB(c)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v4.0.2"))
	resp, err := server.StartTask(context.Background(), req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// get task config
	req1 := &pb.GetCfgRequest{
		Name: taskName,
		Type: pb.CfgType_TaskType,
	}
	resp1, err := server.GetCfg(context.Background(), req1)
	c.Assert(err, check.IsNil)
	c.Assert(resp1.Result, check.IsTrue)
	c.Assert(strings.Contains(resp1.Cfg, "name: test"), check.IsTrue)

	// not exist task name
	taskName2 := "wrong"
	req2 := &pb.GetCfgRequest{
		Name: taskName2,
		Type: pb.CfgType_TaskType,
	}
	resp2, err := server.GetCfg(context.Background(), req2)
	c.Assert(err, check.IsNil)
	c.Assert(resp2.Result, check.IsFalse)
	c.Assert(resp2.Msg, check.Equals, "task not found")

	// generate a template named `wrong`, test get this task template
	openapiTask, err := fixtures.GenNoShardOpenAPITaskForTest()
	c.Assert(err, check.IsNil)
	openapiTask.Name = taskName2
	c.Assert(ha.PutOpenAPITaskTemplate(t.etcdTestCli, openapiTask, true), check.IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB", `return(true)`), check.IsNil)
	resp2, err = server.GetCfg(context.Background(), &pb.GetCfgRequest{Name: taskName2, Type: pb.CfgType_TaskTemplateType})
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/MockSkipAdjustTargetDB"), check.IsNil)
	c.Assert(err, check.IsNil)
	c.Assert(resp2.Result, check.IsTrue)
	c.Assert(strings.Contains(resp2.Cfg, fmt.Sprintf("name: %s", taskName2)), check.IsTrue)

	// test restart master
	server.scheduler.Close()
	c.Assert(server.scheduler.Start(ctx, t.etcdTestCli), check.IsNil)

	resp3, err := server.GetCfg(context.Background(), req1)
	c.Assert(err, check.IsNil)
	c.Assert(resp3.Result, check.IsTrue)
	c.Assert(resp3.Cfg, check.Equals, resp1.Cfg)

	req3 := &pb.GetCfgRequest{
		Name: "dm-master",
		Type: pb.CfgType_MasterType,
	}
	resp4, err := server.GetCfg(context.Background(), req3)
	c.Assert(err, check.IsNil)
	c.Assert(resp4.Result, check.IsTrue)
	c.Assert(strings.Contains(resp4.Cfg, "name = \"dm-master\""), check.IsTrue)

	req4 := &pb.GetCfgRequest{
		Name: "haha",
		Type: pb.CfgType_MasterType,
	}
	resp5, err := server.GetCfg(context.Background(), req4)
	c.Assert(err, check.IsNil)
	c.Assert(resp5.Result, check.IsFalse)
	c.Assert(resp5.Msg, check.Equals, "master not found")

	req5 := &pb.GetCfgRequest{
		Name: "haha",
		Type: pb.CfgType_WorkerType,
	}
	resp6, err := server.GetCfg(context.Background(), req5)
	c.Assert(err, check.IsNil)
	c.Assert(resp6.Result, check.IsFalse)
	c.Assert(resp6.Msg, check.Equals, "worker not found")

	req6 := &pb.GetCfgRequest{
		Name: "mysql-replica-01",
		Type: pb.CfgType_SourceType,
	}
	resp7, err := server.GetCfg(context.Background(), req6)
	c.Assert(err, check.IsNil)
	c.Assert(resp7.Result, check.IsTrue)
	c.Assert(strings.Contains(resp7.Cfg, "source-id: mysql-replica-01"), check.IsTrue, check.Commentf(resp7.Cfg))

	req7 := &pb.GetCfgRequest{
		Name: "haha",
		Type: pb.CfgType_SourceType,
	}
	resp8, err := server.GetCfg(context.Background(), req7)
	c.Assert(err, check.IsNil)
	c.Assert(resp8.Result, check.IsFalse)
	c.Assert(resp8.Msg, check.Equals, "source not found")

	t.clearSchedulerEnv(c, cancel, &wg)
}

func (t *testMaster) relayStageMatch(c *check.C, s *scheduler.Scheduler, source string, expectStage pb.Stage) {
	stage := ha.NewRelayStage(expectStage, source)
	stageDeepEqualExcludeRev(c, s.GetExpectRelayStage(source), stage)

	eStage, _, err := ha.GetRelayStage(t.etcdTestCli, source)
	c.Assert(err, check.IsNil)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		stageDeepEqualExcludeRev(c, eStage, stage)
	}
}

func (t *testMaster) subTaskStageMatch(c *check.C, s *scheduler.Scheduler, task, source string, expectStage pb.Stage) {
	stage := ha.NewSubTaskStage(expectStage, source, task)
	c.Assert(s.GetExpectSubTaskStage(task, source), check.DeepEquals, stage)

	eStageM, _, err := ha.GetSubTaskStage(t.etcdTestCli, source, task)
	c.Assert(err, check.IsNil)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		c.Assert(eStageM, check.HasLen, 1)
		stageDeepEqualExcludeRev(c, eStageM[task], stage)
	default:
		c.Assert(eStageM, check.HasLen, 0)
	}
}

func (t *testMaster) TestGRPCLongResponse(c *check.C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/LongRPCResponse", `return()`), check.IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/LongRPCResponse")
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/ctl/common/SkipUpdateMasterClient", `return()`), check.IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dm/ctl/common/SkipUpdateMasterClient")

	masterAddr := tempurl.Alloc()[len("http://"):]
	lis, err := net.Listen("tcp", masterAddr)
	c.Assert(err, check.IsNil)
	defer lis.Close()
	server := grpc.NewServer()
	pb.RegisterMasterServer(server, &Server{})
	//nolint:errcheck
	go server.Serve(lis)

	conn, err := grpc.Dial(utils.UnwrapScheme(masterAddr),
		grpc.WithInsecure(),
		grpc.WithBlock())
	c.Assert(err, check.IsNil)
	defer conn.Close()

	common.GlobalCtlClient.MasterClient = pb.NewMasterClient(conn)
	ctx := context.Background()
	resp := &pb.StartTaskResponse{}
	err = common.SendRequest(ctx, "StartTask", &pb.StartTaskRequest{}, &resp)
	c.Assert(err, check.IsNil)
}
