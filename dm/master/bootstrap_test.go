// Copyright 2020 PingCAP, Inc.
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
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pbmock"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	filter "github.com/pingcap/tiflow/pkg/binlog-filter"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

type testMaster struct {
	workerClients     map[string]workerrpc.Client
	saveMaxRetryNum   int
	electionTTLBackup int
	testT             *testing.T

	testEtcdCluster *integration.ClusterV3
	etcdTestCli     *clientv3.Client
}

var testSuite = check.SerialSuites(&testMaster{})

func TestMaster(t *testing.T) {
	err := log.InitLogger(&log.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pwd, err = os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	integration.BeforeTestExternal(t)
	// inject *testing.T to testMaster
	s := testSuite.(*testMaster)
	s.testT = t

	check.TestingT(t)
}

func (t *testMaster) SetUpSuite(c *check.C) {
	c.Assert(log.InitLogger(&log.Config{}), check.IsNil)
	t.workerClients = make(map[string]workerrpc.Client)
	t.saveMaxRetryNum = maxRetryNum
	t.electionTTLBackup = electionTTL
	electionTTL = 3
	maxRetryNum = 2
	checkAndAdjustSourceConfigForDMCtlFunc = checkAndNoAdjustSourceConfigMock
}

func (t *testMaster) TearDownSuite(c *check.C) {
	maxRetryNum = t.saveMaxRetryNum
	electionTTL = t.electionTTLBackup
	checkAndAdjustSourceConfigForDMCtlFunc = checkAndAdjustSourceConfig
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

func testDefaultMasterServerWithC(c *check.C) *Server {
	cfg := NewConfig()
	err := cfg.FromContent(SampleConfig)
	c.Assert(err, check.IsNil)
	cfg.DataDir = c.MkDir()
	server := NewServer(cfg)
	server.leader.Store(oneselfLeader)
	go server.ap.Start(context.Background())

	return server
}

func (t *testMaster) TestCollectSourceConfigFilesV1Import(c *check.C) {
	s := testDefaultMasterServerWithC(c)
	defer s.Close()
	s.cfg.V1SourcesPath = c.MkDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tctx := tcontext.NewContext(ctx, log.L())

	// no source file exist.
	cfgs, err := s.collectSourceConfigFilesV1Import(tctx)
	c.Assert(err, check.IsNil)
	c.Assert(cfgs, check.HasLen, 0)

	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	password := os.Getenv("MYSQL_PSWD")

	cfg1, err := config.SourceCfgFromYaml(config.SampleSourceConfig)
	c.Assert(err, check.IsNil)
	// fix empty map after marshal/unmarshal becomes nil
	cfg1.From.Adjust()
	cfg1.Tracer = map[string]interface{}{}
	cfg1.Filters = []*filter.BinlogEventRule{}
	cfg1.From.Host = host
	cfg1.From.Port = port
	cfg1.From.User = user
	cfg1.From.Password = password
	cfg1.RelayDir = "relay-dir"
	c.Assert(checkAndAdjustSourceConfigForDMCtlFunc(ctx, cfg1), check.IsNil) // adjust source config.
	cfg2 := cfg1.Clone()
	cfg2.SourceID = "mysql-replica-02"

	// write into source files.
	data1, err := cfg1.Yaml()
	c.Assert(err, check.IsNil)
	c.Assert(os.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "source1.yaml"), []byte(data1), 0o644), check.IsNil)
	data2, err := cfg2.Yaml()
	c.Assert(err, check.IsNil)
	c.Assert(os.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "source2.yaml"), []byte(data2), 0o644), check.IsNil)

	// collect again, two configs exist.
	cfgs, err = s.collectSourceConfigFilesV1Import(tctx)
	c.Assert(err, check.IsNil)
	for _, cfg := range cfgs {
		cfg.From.Session = nil
	}
	c.Assert(cfgs, check.HasLen, 2)
	c.Assert(cfgs[cfg1.SourceID], check.DeepEquals, cfg1)
	c.Assert(cfgs[cfg2.SourceID], check.DeepEquals, cfg2)

	// put a invalid source file.
	c.Assert(os.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "invalid.yaml"), []byte("invalid-source-data"), 0o644), check.IsNil)
	cfgs, err = s.collectSourceConfigFilesV1Import(tctx)
	c.Assert(terror.ErrConfigYamlTransform.Equal(err), check.IsTrue)
	c.Assert(cfgs, check.HasLen, 0)
}

func (t *testMaster) TestWaitWorkersReadyV1Import(c *check.C) {
	oldWaitWorkerV1Timeout := waitWorkerV1Timeout
	defer func() {
		waitWorkerV1Timeout = oldWaitWorkerV1Timeout
	}()
	waitWorkerV1Timeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tctx := tcontext.NewContext(ctx, log.L())

	s := testDefaultMasterServerWithC(c)
	defer s.Close()
	s.cfg.V1SourcesPath = c.MkDir()
	c.Assert(s.scheduler.Start(ctx, t.etcdTestCli), check.IsNil)

	cfg1, err := config.SourceCfgFromYaml(config.SampleSourceConfig)
	c.Assert(err, check.IsNil)
	cfg2 := cfg1.Clone()
	cfg2.SourceID = "mysql-replica-02"
	cfgs := map[string]*config.SourceConfig{
		cfg1.SourceID: cfg1,
		cfg2.SourceID: cfg2,
	}

	// no worker registered, timeout.
	err = s.waitWorkersReadyV1Import(tctx, cfgs)
	c.Assert(err, check.ErrorMatches, ".*wait for DM-worker instances timeout.*")

	// register one worker.
	req1 := &pb.RegisterWorkerRequest{
		Name:    "worker-1",
		Address: "127.0.0.1:8262",
	}
	resp1, err := s.RegisterWorker(ctx, req1)
	c.Assert(err, check.IsNil)
	c.Assert(resp1.Result, check.IsTrue)

	// still timeout because no enough workers.
	err = s.waitWorkersReadyV1Import(tctx, cfgs)
	c.Assert(err, check.ErrorMatches, ".*wait for DM-worker instances timeout.*")

	// register another worker.
	go func() {
		time.Sleep(1500 * time.Millisecond)
		req2 := &pb.RegisterWorkerRequest{
			Name:    "worker-2",
			Address: "127.0.0.1:8263",
		}
		resp2, err2 := s.RegisterWorker(ctx, req2)
		c.Assert(err2, check.IsNil)
		c.Assert(resp2.Result, check.IsTrue)
	}()

	err = s.waitWorkersReadyV1Import(tctx, cfgs)
	c.Assert(err, check.IsNil)
}

func (t *testMaster) TestSubtaskCfgsStagesV1Import(c *check.C) {
	var (
		worker1Name = "worker-1"
		worker1Addr = "127.0.0.1:8262"
		worker2Name = "worker-2"
		worker2Addr = "127.0.0.1:8263"
		taskName1   = "task-1"
		taskName2   = "task-2"
		sourceID1   = "mysql-replica-01"
		sourceID2   = "mysql-replica-02"
	)

	cfg11 := config.NewSubTaskConfig()
	c.Assert(cfg11.Decode(config.SampleSubtaskConfig, true), check.IsNil)
	cfg11.Dir = "./dump_data"
	cfg11.ChunkFilesize = "64"
	cfg11.Name = taskName1
	cfg11.SourceID = sourceID1
	c.Assert(cfg11.Adjust(true), check.IsNil) // adjust again after manually modified some items.
	data11, err := cfg11.Toml()
	c.Assert(err, check.IsNil)
	data11 = strings.ReplaceAll(data11, `chunk-filesize = "64"`, `chunk-filesize = 64`) // different type between v1.0.x and v2.0.x.

	cfg12, err := cfg11.Clone()
	c.Assert(err, check.IsNil)
	cfg12.SourceID = sourceID2
	data12, err := cfg12.Toml()
	c.Assert(err, check.IsNil)
	data12 = strings.ReplaceAll(data12, `chunk-filesize = "64"`, `chunk-filesize = 64`)

	cfg21, err := cfg11.Clone()
	c.Assert(err, check.IsNil)
	cfg21.Dir = "./dump_data"
	cfg21.Name = taskName2
	c.Assert(cfg21.Adjust(true), check.IsNil)
	data21, err := cfg21.Toml()
	c.Assert(err, check.IsNil)
	data21 = strings.ReplaceAll(data21, `chunk-filesize = "64"`, `chunk-filesize = 64`)

	cfg22, err := cfg21.Clone()
	c.Assert(err, check.IsNil)
	cfg22.SourceID = sourceID2
	data22, err := cfg22.Toml()
	c.Assert(err, check.IsNil)
	data22 = strings.ReplaceAll(data22, `chunk-filesize = "64"`, `chunk-filesize = 64`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tctx := tcontext.NewContext(ctx, log.L())

	s := testDefaultMasterServerWithC(c)
	defer s.Close()
	s.cfg.V1SourcesPath = c.MkDir()
	c.Assert(s.scheduler.Start(ctx, t.etcdTestCli), check.IsNil)

	// no workers exist, no config and status need to get.
	cfgs, stages, err := s.getSubtaskCfgsStagesV1Import(tctx)
	c.Assert(err, check.IsNil)
	c.Assert(cfgs, check.HasLen, 0)
	c.Assert(stages, check.HasLen, 0)

	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	mockWCli1 := pbmock.NewMockWorkerClient(ctrl)
	mockWCli2 := pbmock.NewMockWorkerClient(ctrl)
	c.Assert(s.scheduler.AddWorker(worker1Name, worker1Addr), check.IsNil)
	c.Assert(s.scheduler.AddWorker(worker2Name, worker2Addr), check.IsNil)
	s.scheduler.SetWorkerClientForTest(worker1Name, newMockRPCClient(mockWCli1))
	s.scheduler.SetWorkerClientForTest(worker2Name, newMockRPCClient(mockWCli2))

	mockWCli1.EXPECT().OperateV1Meta(
		gomock.Any(),
		&pb.OperateV1MetaRequest{
			Op: pb.V1MetaOp_GetV1Meta,
		},
	).Return(&pb.OperateV1MetaResponse{
		Result: true,
		Meta: map[string]*pb.V1SubTaskMeta{
			taskName1: {
				Op:    pb.TaskOp_Start,
				Stage: pb.Stage_Running,
				Name:  taskName1,
				Task:  []byte(data11),
			},
			taskName2: {
				Op:    pb.TaskOp_Pause,
				Stage: pb.Stage_Paused,
				Name:  taskName2,
				Task:  []byte(data21),
			},
		},
	}, nil)

	mockWCli2.EXPECT().OperateV1Meta(
		gomock.Any(),
		&pb.OperateV1MetaRequest{
			Op: pb.V1MetaOp_GetV1Meta,
		},
	).Return(&pb.OperateV1MetaResponse{
		Result: true,
		Meta: map[string]*pb.V1SubTaskMeta{
			taskName1: {
				Op:    pb.TaskOp_Resume,
				Stage: pb.Stage_Running,
				Name:  taskName1,
				Task:  []byte(data12),
			},
			taskName2: {
				Op:    pb.TaskOp_Start,
				Stage: pb.Stage_Running,
				Name:  taskName2,
				Task:  []byte(data22),
			},
		},
	}, nil)

	// all workers return valid config and stage.
	cfgs, stages, err = s.getSubtaskCfgsStagesV1Import(tctx)
	c.Assert(err, check.IsNil)
	c.Assert(cfgs, check.HasLen, 2)
	c.Assert(stages, check.HasLen, 2)
	c.Assert(cfgs[taskName1], check.HasLen, 2)
	c.Assert(cfgs[taskName2], check.HasLen, 2)
	c.Assert(cfgs[taskName1][sourceID1], check.DeepEquals, *cfg11)
	c.Assert(cfgs[taskName1][sourceID2], check.DeepEquals, *cfg12)
	c.Assert(cfgs[taskName2][sourceID1], check.DeepEquals, *cfg21)
	c.Assert(cfgs[taskName2][sourceID2], check.DeepEquals, *cfg22)
	c.Assert(stages[taskName1], check.HasLen, 2)
	c.Assert(stages[taskName2], check.HasLen, 2)
	c.Assert(stages[taskName1][sourceID1], check.Equals, pb.Stage_Running)
	c.Assert(stages[taskName1][sourceID2], check.Equals, pb.Stage_Running)
	c.Assert(stages[taskName2][sourceID1], check.Equals, pb.Stage_Paused)
	c.Assert(stages[taskName2][sourceID2], check.Equals, pb.Stage_Running)

	// one of workers return invalid config.
	mockWCli1.EXPECT().OperateV1Meta(
		gomock.Any(),
		&pb.OperateV1MetaRequest{
			Op: pb.V1MetaOp_GetV1Meta,
		},
	).Return(&pb.OperateV1MetaResponse{
		Result: true,
		Meta: map[string]*pb.V1SubTaskMeta{
			taskName1: {
				Op:    pb.TaskOp_Start,
				Stage: pb.Stage_Running,
				Name:  taskName1,
				Task:  []byte(data11),
			},
			taskName2: {
				Op:    pb.TaskOp_Pause,
				Stage: pb.Stage_Paused,
				Name:  taskName2,
				Task:  []byte(data21),
			},
		},
	}, nil)
	mockWCli2.EXPECT().OperateV1Meta(
		gomock.Any(),
		&pb.OperateV1MetaRequest{
			Op: pb.V1MetaOp_GetV1Meta,
		},
	).Return(&pb.OperateV1MetaResponse{
		Result: true,
		Meta: map[string]*pb.V1SubTaskMeta{
			taskName1: {
				Op:    pb.TaskOp_Resume,
				Stage: pb.Stage_Running,
				Name:  taskName1,
				Task:  []byte("invalid subtask data"),
			},
			taskName2: {
				Op:    pb.TaskOp_Start,
				Stage: pb.Stage_Running,
				Name:  taskName2,
				Task:  []byte(data22),
			},
		},
	}, nil)
	cfgs, stages, err = s.getSubtaskCfgsStagesV1Import(tctx)
	c.Assert(err, check.ErrorMatches, ".*fail to get subtask config and stage.*")
	c.Assert(cfgs, check.HasLen, 0)
	c.Assert(stages, check.HasLen, 0)
}

func checkAndNoAdjustSourceConfigMock(ctx context.Context, cfg *config.SourceConfig) error {
	if _, err := cfg.Yaml(); err != nil {
		return err
	}
	return cfg.Verify()
}
