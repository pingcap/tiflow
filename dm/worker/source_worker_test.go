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

package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/dm/unit"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/tempurl"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var emptyWorkerStatusInfoJSONLength = 25

func mockShowMasterStatus(mockDB sqlmock.Sqlmock) {
	rows := mockDB.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).AddRow(
		"mysql-bin.000009", 11232, nil, nil, "074be7f4-f0f1-11ea-95bd-0242ac120002:1-699",
	)
	mockDB.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)
}

func mockShowMasterStatusNoRows(mockDB sqlmock.Sqlmock) {
	rows := mockDB.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"})
	mockDB.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)
}

type testServer2 struct {
	suite.Suite
}

func (t *testServer2) SetupSuite() {
	err := log.InitLogger(&log.Config{})
	t.Require().NoError(err)

	getMinLocForSubTaskFunc = getFakeLocForSubTask
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD", `return(true)`))
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/SkipRefreshFromETCDInUT", `return()`))
}

func (t *testServer2) TearDownSuite() {
	getMinLocForSubTaskFunc = getMinLocForSubTask
	t.Require().NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD"))
	t.Require().NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/worker/SkipRefreshFromETCDInUT"))
}

func (t *testServer2) TestTaskAutoResume() {
	var (
		taskName = "sub-task-name"
		port     = 8263
	)
	hostName := "127.0.0.1:18261"
	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+hostName)
	t.Require().NoError(err)
	defer ETCD.Close()

	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = hostName
	sourceConfig := loadSourceConfigWithoutPassword(t.T())
	sourceConfig.Checker.CheckEnable = true
	sourceConfig.Checker.CheckInterval = config.Duration{Duration: 40 * time.Millisecond}
	sourceConfig.Checker.BackoffMin = config.Duration{Duration: 20 * time.Millisecond}
	sourceConfig.Checker.BackoffMax = config.Duration{Duration: 1 * time.Second}

	cfg.WorkerAddr = fmt.Sprintf(":%d", port)

	dir := t.T().TempDir()
	sourceConfig.RelayDir = dir
	sourceConfig.MetaDir = dir
	sourceConfig.EnableRelay = true

	NewRelayHolder = NewDummyRelayHolder
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()

	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessForever", `return()`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessForever")
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/mockCreateUnitsDumpOnly", `return(true)`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/worker/mockCreateUnitsDumpOnly")
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/loader/ignoreLoadCheckpointErr", `return()`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/loader/ignoreLoadCheckpointErr")
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError", `return("test auto resume inject error")`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError")

	s := NewServer(cfg)
	defer s.Close()
	go func() {
		t.Require().NoError(s.Start())
	}()
	t.Require().True(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		if s.closed.Load() {
			return false
		}
		w, err2 := s.getOrStartWorker(sourceConfig, true)
		t.Require().NoError(err2)
		// we set sourceConfig.EnableRelay = true above
		t.Require().NoError(w.EnableRelay(false))
		t.Require().NoError(w.EnableHandleSubtasks())
		return true
	}))
	// start task
	var subtaskCfg config.SubTaskConfig
	t.Require().NoError(subtaskCfg.Decode(config.SampleSubtaskConfig, true))
	t.Require().NoError(err)
	subtaskCfg.Mode = "full"
	subtaskCfg.Timezone = "UTC"
	t.Require().NoError(s.getSourceWorker(true).StartSubTask(&subtaskCfg, pb.Stage_Running, pb.Stage_Stopped, true))

	// check task in paused state
	t.Require().True(utils.WaitSomething(100, 100*time.Millisecond, func() bool {
		subtaskStatus, _, _ := s.getSourceWorker(true).QueryStatus(context.Background(), taskName)
		for _, st := range subtaskStatus {
			if st.Name == taskName && st.Stage == pb.Stage_Paused {
				return true
			}
		}
		return false
	}))
	//nolint:errcheck
	failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError")

	rtsc, ok := s.getSourceWorker(true).taskStatusChecker.(*realTaskStatusChecker)
	t.Require().True(ok)
	defer func() {
		// close multiple time
		rtsc.Close()
		rtsc.Close()
	}()

	// check task will be auto resumed
	t.Require().True(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		sts, _, _ := s.getSourceWorker(true).QueryStatus(context.Background(), taskName)
		for _, st := range sts {
			if st.Name == taskName && st.Stage == pb.Stage_Running {
				return true
			}
		}
		t.T().Log(sts)
		return false
	}))
}

type testWorkerFunctionalities struct {
	suite.Suite

	createUnitCount         int32
	expectedCreateUnitCount int32
}

func (t *testWorkerFunctionalities) SetupSuite() {
	NewRelayHolder = NewDummyRelayHolder
	NewSubTask = NewRealSubTask
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		atomic.AddInt32(&t.createUnitCount, 1)
		mockDumper := NewMockUnit(pb.UnitType_Dump)
		mockLoader := NewMockUnit(pb.UnitType_Load)
		mockSync := NewMockUnit(pb.UnitType_Sync)
		return []unit.Unit{mockDumper, mockLoader, mockSync}
	}
	getMinLocForSubTaskFunc = getFakeLocForSubTask
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD", `return(true)`))
}

func (t *testWorkerFunctionalities) TearDownSuite() {
	NewRelayHolder = NewRealRelayHolder
	NewSubTask = NewRealSubTask
	createUnits = createRealUnits
	getMinLocForSubTaskFunc = getMinLocForSubTask
	t.Require().NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD"))
}

func (t *testWorkerFunctionalities) TestWorkerFunctionalities() {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	defer ETCD.Close()
	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	t.Require().NoError(err)
	sourceCfg := loadSourceConfigWithoutPassword(t.T())
	sourceCfg.EnableRelay = false

	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.Decode(config.SampleSubtaskConfig, true)
	t.Require().NoError(err)

	// start worker
	w, err := NewSourceWorker(sourceCfg, etcdCli, "", "")
	t.Require().NoError(err)
	defer w.Stop(true)
	go func() {
		w.Start()
	}()
	t.Require().True(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	}))

	// test 1: when subTaskEnabled is false, switch on relay
	t.Require().False(w.subTaskEnabled.Load())
	t.testEnableRelay(w, etcdCli, sourceCfg, cfg)

	// test2: when subTaskEnabled is false, switch off relay
	t.Require().False(w.subTaskEnabled.Load())
	t.testDisableRelay(w)

	// test3: when relayEnabled is false, switch on subtask
	t.Require().False(w.relayEnabled.Load())

	t.testEnableHandleSubtasks(w, etcdCli, subtaskCfg, sourceCfg)

	// test4: when subTaskEnabled is true, switch on relay
	t.Require().True(w.subTaskEnabled.Load())

	t.testEnableRelay(w, etcdCli, sourceCfg, cfg)
	t.Require().True(w.subTaskHolder.findSubTask(subtaskCfg.Name).cfg.UseRelay)
	t.expectedCreateUnitCount++
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return atomic.LoadInt32(&t.createUnitCount) == t.expectedCreateUnitCount
	}))

	// test5: when subTaskEnabled is true, switch off relay
	t.Require().True(w.subTaskEnabled.Load())
	t.testDisableRelay(w)

	t.Require().False(w.subTaskHolder.findSubTask(subtaskCfg.Name).cfg.UseRelay)
	t.expectedCreateUnitCount++
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return atomic.LoadInt32(&t.createUnitCount) == t.expectedCreateUnitCount
	}))

	// test6: when relayEnabled is false, switch off subtask
	t.Require().False(w.relayEnabled.Load())

	w.DisableHandleSubtasks()
	t.Require().False(w.subTaskEnabled.Load())

	// prepare for test7 & 8
	t.testEnableRelay(w, etcdCli, sourceCfg, cfg)
	// test7: when relayEnabled is true, switch on subtask
	t.Require().True(w.relayEnabled.Load())

	subtaskCfg2 := subtaskCfg
	subtaskCfg2.Name = "sub-task-name-2"
	// we already added subtaskCfg, so below EnableHandleSubtasks will find an extra subtask
	t.expectedCreateUnitCount++
	t.testEnableHandleSubtasks(w, etcdCli, subtaskCfg2, sourceCfg)
	t.Require().True(w.subTaskHolder.findSubTask(subtaskCfg.Name).cfg.UseRelay)
	t.Require().True(w.subTaskHolder.findSubTask(subtaskCfg2.Name).cfg.UseRelay)

	// test8: when relayEnabled is true, switch off subtask
	t.Require().True(w.relayEnabled.Load())

	w.DisableHandleSubtasks()
	t.Require().False(w.subTaskEnabled.Load())
}

func (t *testWorkerFunctionalities) testEnableRelay(w *SourceWorker, etcdCli *clientv3.Client,
	sourceCfg *config.SourceConfig, cfg *Config,
) {
	t.Require().NoError(w.EnableRelay(false))

	t.Require().True(w.relayEnabled.Load())
	t.Require().Equal(pb.Stage_New, w.relayHolder.Stage())

	_, err := ha.PutSourceCfg(etcdCli, sourceCfg)
	t.Require().NoError(err)
	_, err = ha.PutRelayStageRelayConfigSourceBound(etcdCli, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID),
		ha.NewSourceBound(sourceCfg.SourceID, cfg.Name))
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Running
	}))

	_, err = ha.DeleteSourceCfgRelayStageSourceBound(etcdCli, sourceCfg.SourceID, cfg.Name)
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Stopped
	}))
}

func (t *testWorkerFunctionalities) testDisableRelay(w *SourceWorker) {
	w.DisableRelay()

	t.Require().False(w.relayEnabled.Load())
	t.Require().Nil(w.relayHolder)
}

func (t *testWorkerFunctionalities) testEnableHandleSubtasks(w *SourceWorker, etcdCli *clientv3.Client,
	subtaskCfg config.SubTaskConfig, sourceCfg *config.SourceConfig,
) {
	t.Require().NoError(w.EnableHandleSubtasks())
	t.Require().True(w.subTaskEnabled.Load())

	_, err := ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) != nil
	}))
	t.expectedCreateUnitCount++
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return atomic.LoadInt32(&t.createUnitCount) == t.expectedCreateUnitCount
	}))
}

type testWorkerEtcdCompact struct {
	suite.Suite
}

func (t *testWorkerEtcdCompact) SetupSuite() {
	NewRelayHolder = NewDummyRelayHolder
	NewSubTask = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string) *SubTask {
		cfg.UseRelay = false
		return NewRealSubTask(cfg, etcdClient, worker)
	}
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		mockDumper := NewMockUnit(pb.UnitType_Dump)
		mockLoader := NewMockUnit(pb.UnitType_Load)
		mockSync := NewMockUnit(pb.UnitType_Sync)
		return []unit.Unit{mockDumper, mockLoader, mockSync}
	}
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD", `return(true)`))
}

func (t *testWorkerEtcdCompact) TearDownSuite() {
	NewRelayHolder = NewRealRelayHolder
	NewSubTask = NewRealSubTask
	createUnits = createRealUnits
	t.Require().NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD"))
}

func (t *testWorkerEtcdCompact) TestWatchSubtaskStageEtcdCompact() {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)

	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	defer ETCD.Close()
	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	t.Require().NoError(err)
	sourceCfg := loadSourceConfigWithoutPassword(t.T())
	sourceCfg.From = config.GetDBConfigForTest()
	sourceCfg.EnableRelay = false

	// step 1: start worker
	w, err := NewSourceWorker(sourceCfg, etcdCli, "", "")
	t.Require().NoError(err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer w.Stop(true)
	go func() {
		w.Start()
	}()
	t.Require().True(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	}))
	// step 2: Put a subtask config and subtask stage to this source, then delete it
	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.Decode(config.SampleSubtaskConfig, true)
	t.Require().NoError(err)
	subtaskCfg.MydumperPath = mydumperPath

	_, err = ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	t.Require().NoError(err)
	rev, err := ha.DeleteSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg},
		[]ha.Stage{ha.NewSubTaskStage(pb.Stage_Stopped, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	t.Require().NoError(err)
	// step 2.1: start a subtask manually
	t.Require().NoError(w.StartSubTask(&subtaskCfg, pb.Stage_Running, pb.Stage_Stopped, true))
	// step 3: trigger etcd compaction and check whether we can receive it through watcher
	_, err = etcdCli.Compact(ctx, rev)
	t.Require().NoError(err)
	subTaskStageCh := make(chan ha.Stage, 10)
	subTaskErrCh := make(chan error, 10)
	ha.WatchSubTaskStage(ctx, etcdCli, sourceCfg.SourceID, startRev, subTaskStageCh, subTaskErrCh)
	select {
	case err = <-subTaskErrCh:
		t.Require().Equal(etcdErrCompacted, errors.Cause(err))
	case <-time.After(300 * time.Millisecond):
		t.T().Fatal("fail to get etcd error compacted")
	}
	// step 4: watch subtask stage from startRev
	t.Require().NotNil(w.subTaskHolder.findSubTask(subtaskCfg.Name))
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Require().NoError(w.observeSubtaskStage(ctx1, etcdCli, startRev))
	}()
	time.Sleep(time.Second)
	// step 4.1: after observe, invalid subtask should be removed
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) == nil
	}))
	// step 4.2: add a new subtask stage, worker should receive and start it
	_, err = ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) != nil
	}))
	mockDB := conn.InitMockDB(t.T())
	mockShowMasterStatus(mockDB)
	status, _, err := w.QueryStatus(ctx1, subtaskCfg.Name)
	t.Require().NoError(err)
	t.Require().Len(status, 1)
	t.Require().Equal(subtaskCfg.Name, status[0].Name)
	t.Require().Equal(pb.Stage_Running, status[0].Stage)
	cancel1()
	wg.Wait()
	w.subTaskHolder.closeAllSubTasks()
	// step 5: restart observe and start from startRev, this subtask should be added
	ctx2, cancel2 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Require().NoError(w.observeSubtaskStage(ctx2, etcdCli, startRev))
	}()
	time.Sleep(time.Second)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) != nil
	}))
	mockShowMasterStatus(mockDB)
	status, _, err = w.QueryStatus(ctx2, subtaskCfg.Name)
	t.Require().NoError(err)
	t.Require().Len(status, 1)
	t.Require().Equal(subtaskCfg.Name, status[0].Name)
	t.Require().Equal(pb.Stage_Running, status[0].Stage)
	w.Stop(true)
	cancel2()
	wg.Wait()
}

func (t *testWorkerEtcdCompact) TestWatchValidatorStageEtcdCompact() {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)

	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	defer ETCD.Close()
	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	t.Require().NoError(err)
	sourceCfg := loadSourceConfigWithoutPassword(t.T())
	sourceCfg.From = config.GetDBConfigForTest()
	sourceCfg.EnableRelay = false

	//
	// step 1: start worker
	w, err := NewSourceWorker(sourceCfg, etcdCli, "", "")
	t.Require().NoError(err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer w.Stop(true)
	go func() {
		w.Start()
	}()
	t.Require().True(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	}))

	//
	// step 2: Put a subtask config and subtask stage to this source, then delete it
	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.Decode(config.SampleSubtaskConfig, true)
	t.Require().NoError(err)
	subtaskCfg.MydumperPath = mydumperPath
	subtaskCfg.ValidatorCfg = config.ValidatorConfig{Mode: config.ValidationNone}

	// increase revision
	_, err = etcdCli.Put(context.Background(), "/dummy-key", "value")
	t.Require().NoError(err)
	rev, err := ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	t.Require().NoError(err)

	//
	// step 2.1: start a subtask manually
	t.Require().NoError(w.StartSubTask(&subtaskCfg, pb.Stage_Running, pb.Stage_Stopped, true))

	//
	// step 3: trigger etcd compaction and check whether we can receive it through watcher
	_, err = etcdCli.Compact(ctx, rev)
	t.Require().NoError(err)
	subTaskStageCh := make(chan ha.Stage, 10)
	subTaskErrCh := make(chan error, 10)
	ctxForWatch, cancelFunc := context.WithCancel(ctx)
	ha.WatchValidatorStage(ctxForWatch, etcdCli, sourceCfg.SourceID, startRev, subTaskStageCh, subTaskErrCh)
	select {
	case err = <-subTaskErrCh:
		t.Require().Equal(etcdErrCompacted, errors.Cause(err))
	case <-time.After(300 * time.Millisecond):
		t.T().Fatal("fail to get etcd error compacted")
	}
	cancelFunc()

	//
	// step 4: watch subtask stage from startRev
	subTask := w.subTaskHolder.findSubTask(subtaskCfg.Name)
	getValidator := func() *syncer.DataValidator {
		subTask.RLock()
		defer subTask.RUnlock()
		return subTask.validator
	}
	t.Require().NotNil(subTask)
	t.Require().Nil(getValidator())
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Require().NoError(w.observeValidatorStage(ctx1, startRev))
	}()
	time.Sleep(time.Second)

	subtaskCfg.ValidatorCfg = config.ValidatorConfig{Mode: config.ValidationFast}
	unitBakup := subTask.units[len(subTask.units)-1]
	subTask.units[len(subTask.units)-1] = &syncer.Syncer{} // validator need a Syncer, not a mocked unit
	validatorStage := ha.NewValidatorStage(pb.Stage_Running, subtaskCfg.SourceID, subtaskCfg.Name)
	_, err = ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg}, nil, []ha.Stage{validatorStage})
	t.Require().NoError(err)

	// validator created
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return getValidator() != nil
	}))

	subTask.units[len(subTask.units)-1] = unitBakup // restore unit
	cancel1()
	wg.Wait()

	// test operate validator
	err = w.operateValidatorStage(ha.Stage{IsDeleted: true})
	t.Require().NoError(err)
	err = w.operateValidatorStage(ha.Stage{Expect: pb.Stage_Running, Task: "not-exist"})
	t.Require().NoError(err)
	err = w.operateValidatorStage(ha.Stage{Expect: pb.Stage_Running, Task: subtaskCfg.Name})
	t.Require().Error(err)
	t.Require().Regexp(".*failed to get subtask config.*", err.Error())
	err = w.operateValidatorStage(ha.Stage{Expect: pb.Stage_Running, Source: subtaskCfg.SourceID, Task: subtaskCfg.Name})
	t.Require().NoError(err)
}

func (t *testWorkerEtcdCompact) TestWatchRelayStageEtcdCompact() {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)
	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	defer ETCD.Close()
	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	t.Require().NoError(err)
	sourceCfg := loadSourceConfigWithoutPassword(t.T())
	sourceCfg.EnableRelay = true
	sourceCfg.RelayDir = t.T().TempDir()
	sourceCfg.MetaDir = t.T().TempDir()

	// step 1: start worker
	w, err := NewSourceWorker(sourceCfg, etcdCli, "", "")
	t.Require().NoError(err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer w.Stop(true)
	go func() {
		t.Require().NoError(w.EnableRelay(false))
		w.Start()
	}()
	t.Require().True(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	}))
	// step 2: Put a relay stage to this source, then delete it
	// put mysql config into relative etcd key adapter to trigger operation event
	t.Require().NotNil(w.relayHolder)
	_, err = ha.PutSourceCfg(etcdCli, sourceCfg)
	t.Require().NoError(err)
	rev, err := ha.PutRelayStageRelayConfigSourceBound(etcdCli, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID),
		ha.NewSourceBound(sourceCfg.SourceID, cfg.Name))
	t.Require().NoError(err)
	// check relay stage, should be running
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Running
	}))
	// step 3: trigger etcd compaction and check whether we can receive it through watcher, then we delete relay stage
	_, err = etcdCli.Compact(ctx, rev)
	t.Require().NoError(err)
	_, err = ha.DeleteSourceCfgRelayStageSourceBound(etcdCli, sourceCfg.SourceID, cfg.Name)
	t.Require().NoError(err)
	relayStageCh := make(chan ha.Stage, 10)
	relayErrCh := make(chan error, 10)
	ha.WatchRelayStage(ctx, etcdCli, cfg.Name, startRev, relayStageCh, relayErrCh)
	select {
	case err := <-relayErrCh:
		t.Require().Equal(etcdErrCompacted, errors.Cause(err))
	case <-time.After(300 * time.Millisecond):
		t.T().Fatal("fail to get etcd error compacted")
	}
	// step 4: should stop the running relay because see deletion after compaction
	time.Sleep(time.Second)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Stopped
	}))
}

func (t *testServer) testSourceWorker() {
	cfg := loadSourceConfigWithoutPassword(t.T())

	dir := t.T().TempDir()
	cfg.EnableRelay = true
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	defer ETCD.Close()
	workerCfg := NewConfig()
	t.Require().NoError(workerCfg.Parse([]string{"-config=./dm-worker.toml"}))
	workerCfg.Join = masterAddr
	workerCfg.KeepAliveTTL = keepAliveTTL
	workerCfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(workerCfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	t.Require().NoError(err)

	NewRelayHolder = NewDummyRelayHolderWithInitError
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()
	w, err := NewSourceWorker(cfg, etcdCli, "", "")
	t.Require().NoError(err)
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD", `return(true)`))
	err = w.EnableRelay(false)
	t.Require().Error(err)
	t.Require().Regexp("init error", err.Error())
	t.Require().NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD"))

	NewRelayHolder = NewDummyRelayHolder
	w, err = NewSourceWorker(cfg, etcdCli, "", "")
	t.Require().NoError(err)
	t.Require().Len(w.GetUnitAndSourceStatusJSON("", nil), emptyWorkerStatusInfoJSONLength)

	// stop twice
	w.Stop(true)
	t.Require().True(w.closed.Load())
	t.Require().Len(w.subTaskHolder.getAllSubTasks(), 0)
	w.Stop(true)
	t.Require().True(w.closed.Load())
	t.Require().Len(w.subTaskHolder.getAllSubTasks(), 0)
	t.Require().True(w.closed.Load())

	t.Require().NoError(w.StartSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	}, pb.Stage_Running, pb.Stage_Stopped, true))
	task := w.subTaskHolder.findSubTask("testStartTask")
	t.Require().NotNil(task)
	t.Require().Regexp(".*worker already closed.*", task.Result().String())

	t.Require().NoError(w.StartSubTask(&config.SubTaskConfig{
		Name: "testStartTask-in-stopped",
	}, pb.Stage_Stopped, pb.Stage_Stopped, true))
	task = w.subTaskHolder.findSubTask("testStartTask-in-stopped")
	t.Require().NotNil(task)
	t.Require().Regexp(".*worker already closed.*", task.Result().String())

	err = w.UpdateSubTask(context.Background(), &config.SubTaskConfig{
		Name: "testStartTask",
	}, true)
	t.Require().Error(err)
	t.Require().Regexp(".*worker already closed.*", err.Error())

	err = w.OperateSubTask("testSubTask", pb.TaskOp_Delete)
	t.Require().Error(err)
	t.Require().Regexp(".*worker already closed.*", err.Error())
}

func (t *testServer) TestQueryValidator() {
	cfg := loadSourceConfigWithoutPassword(t.T())

	dir := t.T().TempDir()
	cfg.EnableRelay = true
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	w, err := NewSourceWorker(cfg, nil, "", "")
	w.closed.Store(false)
	t.Require().NoError(err)
	st := NewSubTaskWithStage(&config.SubTaskConfig{
		Name: "testQueryValidator",
		ValidatorCfg: config.ValidatorConfig{
			Mode: config.ValidationFull,
		},
	}, pb.Stage_Running, nil, "")
	w.subTaskHolder.recordSubTask(st)
	var ret *pb.ValidationStatus
	ret, err = w.GetValidatorStatus("invalidTaskName")
	t.Require().Nil(ret)
	t.Require().True(terror.ErrWorkerSubTaskNotFound.Equal(err))
}

func (t *testServer) setupValidator() *SourceWorker {
	cfg := loadSourceConfigWithoutPassword(t.T())

	dir := t.T().TempDir()
	cfg.EnableRelay = true
	cfg.RelayDir = dir
	cfg.MetaDir = dir
	st := NewSubTaskWithStage(&config.SubTaskConfig{
		Name: "testQueryValidator",
		ValidatorCfg: config.ValidatorConfig{
			Mode: config.ValidationFull,
		},
	}, pb.Stage_Running, nil, "")
	w, err := NewSourceWorker(cfg, nil, "", "")
	st.StartValidator(pb.Stage_Running, false)
	w.subTaskHolder.recordSubTask(st)
	w.closed.Store(false)
	t.Require().NoError(err)
	return w
}

func (t *testServer) TestGetWorkerValidatorErr() {
	w := t.setupValidator()
	// when subtask name not exists
	// return empty array
	errs, err := w.GetWorkerValidatorErr("invalidTask", pb.ValidateErrorState_InvalidErr)
	t.Require().True(terror.ErrWorkerSubTaskNotFound.Equal(err))
	t.Require().Nil(errs)
}

func (t *testServer) TestOperateWorkerValidatorErr() {
	w := t.setupValidator()
	// when subtask name not exists
	// return empty array
	taskNotFound := terror.ErrWorkerSubTaskNotFound.Generate("invalidTask")
	t.Require().Equal(taskNotFound.Error(), w.OperateWorkerValidatorErr("invalidTask", pb.ValidationErrOp_ClearErrOp, 0, true).Error())
}

func TestMasterBinlogOff(t *testing.T) {
	ctx := context.Background()
	cfg, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t, err)
	cfg.From.Password = "no need to connect"

	w, err := NewSourceWorker(cfg, nil, "", "")
	require.NoError(t, err)
	w.closed.Store(false)
	defer w.Stop(true)

	// start task
	var subtaskCfg config.SubTaskConfig
	require.NoError(t, subtaskCfg.Decode(config.SampleSubtaskConfig, true))
	require.NoError(t, w.StartSubTask(&subtaskCfg, pb.Stage_Running, pb.Stage_Stopped, true))

	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	w.sourceDBMu.Lock()
	w.sourceDB = conn.NewBaseDB(db, terror.ScopeUpstream, "5.7")
	w.sourceDBMu.Unlock()
	mockShowMasterStatusNoRows(mockDB)
	status, _, err := w.QueryStatus(ctx, subtaskCfg.Name)
	require.NoError(t, err)
	require.Len(t, status, 1)
	require.Equal(t, subtaskCfg.Name, status[0].Name)
}

func TestQueryStatusSourceStatusTimeout(t *testing.T) {
	ctx := context.Background()
	cfg, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t, err)
	cfg.From.Password = "no need to connect"

	w, err := NewSourceWorker(cfg, nil, "", "")
	require.NoError(t, err)
	w.closed.Store(false)
	defer w.Stop(true)

	// start task
	var subtaskCfg config.SubTaskConfig
	require.NoError(t, subtaskCfg.Decode(config.SampleSubtaskConfig, true))
	require.NoError(t, w.StartSubTask(&subtaskCfg, pb.Stage_Running, pb.Stage_Stopped, true))

	// make sure sourceDB is not nil so QueryStatus goes into GetSourceStatus path
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	w.sourceDBMu.Lock()
	w.sourceDB = conn.NewBaseDB(db, terror.ScopeUpstream, "5.7")
	w.sourceDBMu.Unlock()

	// make this test fast and deterministic.
	testfailpoint.Enable(t, "github.com/pingcap/tiflow/dm/worker/QueryStatusSourceStatusTimeout", "return(50)")
	testfailpoint.Enable(t, "github.com/pingcap/tiflow/dm/pkg/binlog/BlockGetSourceStatus", "return()")

	start := time.Now()
	status, _, err := w.QueryStatus(ctx, subtaskCfg.Name)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Less(t, elapsed, 2*time.Second)

	// should still return subtask status and record upstream error for query.
	require.Len(t, status, 1)
	require.Equal(t, subtaskCfg.Name, status[0].Name)
	require.NotNil(t, w.getSourceStatusErr())
}
