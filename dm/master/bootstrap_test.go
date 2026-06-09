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
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pbmock"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	filter "github.com/pingcap/tiflow/pkg/binlog-filter"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

type testMaster struct {
	suite.Suite

	workerClients     map[string]workerrpc.Client
	saveMaxRetryNum   int
	electionTTLBackup int

	testEtcdCluster *integration.ClusterV3
	etcdTestCli     *clientv3.Client
}

func TestMaster(t *testing.T) {
	suite.Run(t, new(testMaster))
	suite.Run(t, new(testEtcdSuite))
	suite.Run(t, new(testConfigSuite))
}

func (t *testMaster) SetupSuite() {
	t.Require().NoError(log.InitLogger(&log.Config{}))
	var err error
	pwd, err = os.Getwd()
	t.Require().NoError(err)
	integration.BeforeTestExternal(t.T())
	t.workerClients = make(map[string]workerrpc.Client)
	t.saveMaxRetryNum = maxRetryNum
	t.electionTTLBackup = electionTTL
	electionTTL = 3
	maxRetryNum = 2
	checkAndAdjustSourceConfigForDMCtlFunc = checkAndNoAdjustSourceConfigMock
}

func (t *testMaster) TearDownSuite() {
	maxRetryNum = t.saveMaxRetryNum
	electionTTL = t.electionTTLBackup
	checkAndAdjustSourceConfigForDMCtlFunc = checkAndAdjustSourceConfig
}

func (t *testMaster) SetupTest() {
	t.testEtcdCluster = integration.NewClusterV3(t.T(), &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.testEtcdCluster.RandClient()
	t.clearEtcdEnv()
}

func (t *testMaster) TearDownTest() {
	t.clearEtcdEnv()
	t.testEtcdCluster.Terminate(t.T())
}

func (t *testMaster) clearEtcdEnv() {
	t.Require().NoError(ha.ClearTestInfoOperation(t.etcdTestCli))
}

func (t *testMaster) TestCollectSourceConfigFilesV1Import() {
	s := testDefaultMasterServer(t.T())
	defer s.Close()
	s.cfg.V1SourcesPath = t.T().TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tctx := tcontext.NewContext(ctx, log.L())

	// no source file exist.
	cfgs, err := s.collectSourceConfigFilesV1Import(tctx)
	t.Require().NoError(err)
	t.Require().Len(cfgs, 0)

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
	t.Require().NoError(err)
	// fix empty map after marshal/unmarshal becomes nil
	cfg1.From.Adjust()
	cfg1.Tracer = map[string]interface{}{}
	cfg1.Filters = []*filter.BinlogEventRule{}
	cfg1.From.Host = host
	cfg1.From.Port = port
	cfg1.From.User = user
	cfg1.From.Password = password
	cfg1.RelayDir = "relay-dir"
	t.Require().NoError(checkAndAdjustSourceConfigForDMCtlFunc(ctx, cfg1)) // adjust source config.
	cfg2 := cfg1.Clone()
	cfg2.SourceID = "mysql-replica-02"

	// write into source files.
	data1, err := cfg1.Yaml()
	t.Require().NoError(err)
	t.Require().NoError(os.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "source1.yaml"), []byte(data1), 0o644))
	data2, err := cfg2.Yaml()
	t.Require().NoError(err)
	t.Require().NoError(os.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "source2.yaml"), []byte(data2), 0o644))

	// collect again, two configs exist.
	cfgs, err = s.collectSourceConfigFilesV1Import(tctx)
	t.Require().NoError(err)
	for _, cfg := range cfgs {
		cfg.From.Session = nil
	}
	t.Require().Len(cfgs, 2)
	t.Require().Equal(cfg1, cfgs[cfg1.SourceID])
	t.Require().Equal(cfg2, cfgs[cfg2.SourceID])

	// put a invalid source file.
	t.Require().NoError(os.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "invalid.yaml"), []byte("invalid-source-data"), 0o644))
	cfgs, err = s.collectSourceConfigFilesV1Import(tctx)
	t.Require().True(terror.ErrConfigYamlTransform.Equal(err))
	t.Require().Len(cfgs, 0)
}

func (t *testMaster) TestWaitWorkersReadyV1Import() {
	oldWaitWorkerV1Timeout := waitWorkerV1Timeout
	defer func() {
		waitWorkerV1Timeout = oldWaitWorkerV1Timeout
	}()
	waitWorkerV1Timeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tctx := tcontext.NewContext(ctx, log.L())

	s := testDefaultMasterServer(t.T())
	defer s.Close()
	s.cfg.V1SourcesPath = t.T().TempDir()
	t.Require().NoError(s.scheduler.Start(ctx, t.etcdTestCli))

	cfg1, err := config.SourceCfgFromYaml(config.SampleSourceConfig)
	t.Require().NoError(err)
	cfg2 := cfg1.Clone()
	cfg2.SourceID = "mysql-replica-02"
	cfgs := map[string]*config.SourceConfig{
		cfg1.SourceID: cfg1,
		cfg2.SourceID: cfg2,
	}

	// no worker registered, timeout.
	err = s.waitWorkersReadyV1Import(tctx, cfgs)
	t.Require().Error(err)
	t.Require().Regexp(".*wait for DM-worker instances timeout.*", err.Error())

	// register one worker.
	req1 := &pb.RegisterWorkerRequest{
		Name:    "worker-1",
		Address: "127.0.0.1:8262",
	}
	resp1, err := s.RegisterWorker(ctx, req1)
	t.Require().NoError(err)
	t.Require().True(resp1.Result)

	// still timeout because no enough workers.
	err = s.waitWorkersReadyV1Import(tctx, cfgs)
	t.Require().Error(err)
	t.Require().Regexp(".*wait for DM-worker instances timeout.*", err.Error())

	// register another worker.
	go func() {
		time.Sleep(1500 * time.Millisecond)
		req2 := &pb.RegisterWorkerRequest{
			Name:    "worker-2",
			Address: "127.0.0.1:8263",
		}
		resp2, err2 := s.RegisterWorker(ctx, req2)
		t.Require().NoError(err2)
		t.Require().True(resp2.Result)
	}()

	err = s.waitWorkersReadyV1Import(tctx, cfgs)
	t.Require().NoError(err)
}

func (t *testMaster) TestSubtaskCfgsStagesV1Import() {
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
	t.Require().NoError(cfg11.Decode(config.SampleSubtaskConfig, true))
	cfg11.Dir = "./dump_data"
	cfg11.ChunkFilesize = "64"
	cfg11.Name = taskName1
	cfg11.SourceID = sourceID1
	t.Require().NoError(cfg11.Adjust(true)) // adjust again after manually modified some items.
	data11, err := cfg11.Toml()
	t.Require().NoError(err)
	data11 = strings.ReplaceAll(data11, `chunk-filesize = "64"`, `chunk-filesize = 64`) // different type between v1.0.x and v2.0.x.

	cfg12, err := cfg11.Clone()
	t.Require().NoError(err)
	cfg12.SourceID = sourceID2
	data12, err := cfg12.Toml()
	t.Require().NoError(err)
	data12 = strings.ReplaceAll(data12, `chunk-filesize = "64"`, `chunk-filesize = 64`)

	cfg21, err := cfg11.Clone()
	t.Require().NoError(err)
	cfg21.Dir = "./dump_data"
	cfg21.Name = taskName2
	t.Require().NoError(cfg21.Adjust(true))
	data21, err := cfg21.Toml()
	t.Require().NoError(err)
	data21 = strings.ReplaceAll(data21, `chunk-filesize = "64"`, `chunk-filesize = 64`)

	cfg22, err := cfg21.Clone()
	t.Require().NoError(err)
	cfg22.SourceID = sourceID2
	data22, err := cfg22.Toml()
	t.Require().NoError(err)
	data22 = strings.ReplaceAll(data22, `chunk-filesize = "64"`, `chunk-filesize = 64`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tctx := tcontext.NewContext(ctx, log.L())

	s := testDefaultMasterServer(t.T())
	defer s.Close()
	s.cfg.V1SourcesPath = t.T().TempDir()
	t.Require().NoError(s.scheduler.Start(ctx, t.etcdTestCli))

	// no workers exist, no config and status need to get.
	cfgs, stages, err := s.getSubtaskCfgsStagesV1Import(tctx)
	t.Require().NoError(err)
	t.Require().Len(cfgs, 0)
	t.Require().Len(stages, 0)

	ctrl := gomock.NewController(t.T())
	defer ctrl.Finish()
	mockWCli1 := pbmock.NewMockWorkerClient(ctrl)
	mockWCli2 := pbmock.NewMockWorkerClient(ctrl)
	t.Require().NoError(s.scheduler.AddWorker(worker1Name, worker1Addr))
	t.Require().NoError(s.scheduler.AddWorker(worker2Name, worker2Addr))
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
	t.Require().NoError(err)
	t.Require().Len(cfgs, 2)
	t.Require().Len(stages, 2)
	t.Require().Len(cfgs[taskName1], 2)
	t.Require().Len(cfgs[taskName2], 2)
	t.Require().Equal(*cfg11, cfgs[taskName1][sourceID1])
	t.Require().Equal(*cfg12, cfgs[taskName1][sourceID2])
	t.Require().Equal(*cfg21, cfgs[taskName2][sourceID1])
	t.Require().Equal(*cfg22, cfgs[taskName2][sourceID2])
	t.Require().Len(stages[taskName1], 2)
	t.Require().Len(stages[taskName2], 2)
	t.Require().Equal(pb.Stage_Running, stages[taskName1][sourceID1])
	t.Require().Equal(pb.Stage_Running, stages[taskName1][sourceID2])
	t.Require().Equal(pb.Stage_Paused, stages[taskName2][sourceID1])
	t.Require().Equal(pb.Stage_Running, stages[taskName2][sourceID2])

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
	t.Require().Error(err)
	t.Require().Regexp(".*fail to get subtask config and stage.*", err.Error())
	t.Require().Len(cfgs, 0)
	t.Require().Len(stages, 0)
}

func checkAndNoAdjustSourceConfigMock(ctx context.Context, cfg *config.SourceConfig) error {
	if _, err := cfg.Yaml(); err != nil {
		return err
	}
	return cfg.Verify()
}
