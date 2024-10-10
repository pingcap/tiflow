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
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
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

type testServer2 struct{}

var _ = check.Suite(&testServer2{})

func (t *testServer2) SetUpSuite(c *check.C) {
	err := log.InitLogger(&log.Config{})
	c.Assert(err, check.IsNil)

	getMinLocForSubTaskFunc = getFakeLocForSubTask
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD", `return(true)`), check.IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/SkipRefreshFromETCDInUT", `return()`), check.IsNil)
}

func (t *testServer2) TearDownSuite(c *check.C) {
	getMinLocForSubTaskFunc = getMinLocForSubTask
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD"), check.IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/worker/SkipRefreshFromETCDInUT"), check.IsNil)
}

func (t *testServer2) TestTaskAutoResume(c *check.C) {
	var (
		taskName = "sub-task-name"
		port     = 8263
	)
	hostName := "127.0.0.1:18261"
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+hostName)
	c.Assert(err, check.IsNil)
	defer ETCD.Close()

	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), check.IsNil)
	cfg.Join = hostName
	sourceConfig := loadSourceConfigWithoutPassword(c)
	sourceConfig.Checker.CheckEnable = true
	sourceConfig.Checker.CheckInterval = config.Duration{Duration: 40 * time.Millisecond}
	sourceConfig.Checker.BackoffMin = config.Duration{Duration: 20 * time.Millisecond}
	sourceConfig.Checker.BackoffMax = config.Duration{Duration: 1 * time.Second}

	cfg.WorkerAddr = fmt.Sprintf(":%d", port)

	dir := c.MkDir()
	sourceConfig.RelayDir = dir
	sourceConfig.MetaDir = dir
	sourceConfig.EnableRelay = true

	NewRelayHolder = NewDummyRelayHolder
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()

	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessForever", `return()`), check.IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessForever")
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/mockCreateUnitsDumpOnly", `return(true)`), check.IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/worker/mockCreateUnitsDumpOnly")
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/loader/ignoreLoadCheckpointErr", `return()`), check.IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/loader/ignoreLoadCheckpointErr")
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError", `return("test auto resume inject error")`), check.IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError")

	s := NewServer(cfg)
	defer s.Close()
	go func() {
		c.Assert(s.Start(), check.IsNil)
	}()
	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		if s.closed.Load() {
			return false
		}
		w, err2 := s.getOrStartWorker(sourceConfig, true)
		c.Assert(err2, check.IsNil)
		// we set sourceConfig.EnableRelay = true above
		c.Assert(w.EnableRelay(false), check.IsNil)
		c.Assert(w.EnableHandleSubtasks(), check.IsNil)
		return true
	}), check.IsTrue)
	// start task
	var subtaskCfg config.SubTaskConfig
	c.Assert(subtaskCfg.Decode(config.SampleSubtaskConfig, true), check.IsNil)
	c.Assert(err, check.IsNil)
	subtaskCfg.Mode = "full"
	subtaskCfg.Timezone = "UTC"
	c.Assert(s.getSourceWorker(true).StartSubTask(&subtaskCfg, pb.Stage_Running, pb.Stage_Stopped, true), check.IsNil)

	// check task in paused state
	c.Assert(utils.WaitSomething(100, 100*time.Millisecond, func() bool {
		subtaskStatus, _, _ := s.getSourceWorker(true).QueryStatus(context.Background(), taskName)
		for _, st := range subtaskStatus {
			if st.Name == taskName && st.Stage == pb.Stage_Paused {
				return true
			}
		}
		return false
	}), check.IsTrue)
	//nolint:errcheck
	failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError")

	rtsc, ok := s.getSourceWorker(true).taskStatusChecker.(*realTaskStatusChecker)
	c.Assert(ok, check.IsTrue)
	defer func() {
		// close multiple time
		rtsc.Close()
		rtsc.Close()
	}()

	// check task will be auto resumed
	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		sts, _, _ := s.getSourceWorker(true).QueryStatus(context.Background(), taskName)
		for _, st := range sts {
			if st.Name == taskName && st.Stage == pb.Stage_Running {
				return true
			}
		}
		c.Log(sts)
		return false
	}), check.IsTrue)
}

type testWorkerFunctionalities struct {
	createUnitCount         int32
	expectedCreateUnitCount int32
}

var _ = check.Suite(&testWorkerFunctionalities{})

func (t *testWorkerFunctionalities) SetUpSuite(c *check.C) {
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
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD", `return(true)`), check.IsNil)
}

func (t *testWorkerFunctionalities) TearDownSuite(c *check.C) {
	NewRelayHolder = NewRealRelayHolder
	NewSubTask = NewRealSubTask
	createUnits = createRealUnits
	getMinLocForSubTaskFunc = getMinLocForSubTask
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD"), check.IsNil)
}

func (t *testWorkerFunctionalities) TestWorkerFunctionalities(c *check.C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, check.IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), check.IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, check.IsNil)
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.EnableRelay = false

	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.Decode(config.SampleSubtaskConfig, true)
	c.Assert(err, check.IsNil)

	// start worker
	w, err := NewSourceWorker(sourceCfg, etcdCli, "", "")
	c.Assert(err, check.IsNil)
	defer w.Stop(true)
	go func() {
		w.Start()
	}()
	c.Assert(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	}), check.IsTrue)

	// test 1: when subTaskEnabled is false, switch on relay
	c.Assert(w.subTaskEnabled.Load(), check.IsFalse)
	t.testEnableRelay(c, w, etcdCli, sourceCfg, cfg)

	// test2: when subTaskEnabled is false, switch off relay
	c.Assert(w.subTaskEnabled.Load(), check.IsFalse)
	t.testDisableRelay(c, w)

	// test3: when relayEnabled is false, switch on subtask
	c.Assert(w.relayEnabled.Load(), check.IsFalse)

	t.testEnableHandleSubtasks(c, w, etcdCli, subtaskCfg, sourceCfg)

	// test4: when subTaskEnabled is true, switch on relay
	c.Assert(w.subTaskEnabled.Load(), check.IsTrue)

	t.testEnableRelay(c, w, etcdCli, sourceCfg, cfg)
	c.Assert(w.subTaskHolder.findSubTask(subtaskCfg.Name).cfg.UseRelay, check.IsTrue)
	t.expectedCreateUnitCount++
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return atomic.LoadInt32(&t.createUnitCount) == t.expectedCreateUnitCount
	}), check.IsTrue)

	// test5: when subTaskEnabled is true, switch off relay
	c.Assert(w.subTaskEnabled.Load(), check.IsTrue)
	t.testDisableRelay(c, w)

	c.Assert(w.subTaskHolder.findSubTask(subtaskCfg.Name).cfg.UseRelay, check.IsFalse)
	t.expectedCreateUnitCount++
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return atomic.LoadInt32(&t.createUnitCount) == t.expectedCreateUnitCount
	}), check.IsTrue)

	// test6: when relayEnabled is false, switch off subtask
	c.Assert(w.relayEnabled.Load(), check.IsFalse)

	w.DisableHandleSubtasks()
	c.Assert(w.subTaskEnabled.Load(), check.IsFalse)

	// prepare for test7 & 8
	t.testEnableRelay(c, w, etcdCli, sourceCfg, cfg)
	// test7: when relayEnabled is true, switch on subtask
	c.Assert(w.relayEnabled.Load(), check.IsTrue)

	subtaskCfg2 := subtaskCfg
	subtaskCfg2.Name = "sub-task-name-2"
	// we already added subtaskCfg, so below EnableHandleSubtasks will find an extra subtask
	t.expectedCreateUnitCount++
	t.testEnableHandleSubtasks(c, w, etcdCli, subtaskCfg2, sourceCfg)
	c.Assert(w.subTaskHolder.findSubTask(subtaskCfg.Name).cfg.UseRelay, check.IsTrue)
	c.Assert(w.subTaskHolder.findSubTask(subtaskCfg2.Name).cfg.UseRelay, check.IsTrue)

	// test8: when relayEnabled is true, switch off subtask
	c.Assert(w.relayEnabled.Load(), check.IsTrue)

	w.DisableHandleSubtasks()
	c.Assert(w.subTaskEnabled.Load(), check.IsFalse)
}

func (t *testWorkerFunctionalities) testEnableRelay(c *check.C, w *SourceWorker, etcdCli *clientv3.Client,
	sourceCfg *config.SourceConfig, cfg *Config,
) {
	c.Assert(w.EnableRelay(false), check.IsNil)

	c.Assert(w.relayEnabled.Load(), check.IsTrue)
	c.Assert(w.relayHolder.Stage(), check.Equals, pb.Stage_New)

	_, err := ha.PutSourceCfg(etcdCli, sourceCfg)
	c.Assert(err, check.IsNil)
	_, err = ha.PutRelayStageRelayConfigSourceBound(etcdCli, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID),
		ha.NewSourceBound(sourceCfg.SourceID, cfg.Name))
	c.Assert(err, check.IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Running
	}), check.IsTrue)

	_, err = ha.DeleteSourceCfgRelayStageSourceBound(etcdCli, sourceCfg.SourceID, cfg.Name)
	c.Assert(err, check.IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Stopped
	}), check.IsTrue)
}

func (t *testWorkerFunctionalities) testDisableRelay(c *check.C, w *SourceWorker) {
	w.DisableRelay()

	c.Assert(w.relayEnabled.Load(), check.IsFalse)
	c.Assert(w.relayHolder, check.IsNil)
}

func (t *testWorkerFunctionalities) testEnableHandleSubtasks(c *check.C, w *SourceWorker, etcdCli *clientv3.Client,
	subtaskCfg config.SubTaskConfig, sourceCfg *config.SourceConfig,
) {
	c.Assert(w.EnableHandleSubtasks(), check.IsNil)
	c.Assert(w.subTaskEnabled.Load(), check.IsTrue)

	_, err := ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	c.Assert(err, check.IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) != nil
	}), check.IsTrue)
	t.expectedCreateUnitCount++
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return atomic.LoadInt32(&t.createUnitCount) == t.expectedCreateUnitCount
	}), check.IsTrue)
}

type testWorkerEtcdCompact struct{}

var _ = check.Suite(&testWorkerEtcdCompact{})

func (t *testWorkerEtcdCompact) SetUpSuite(c *check.C) {
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
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD", `return(true)`), check.IsNil)
}

func (t *testWorkerEtcdCompact) TearDownSuite(c *check.C) {
	NewRelayHolder = NewRealRelayHolder
	NewSubTask = NewRealSubTask
	createUnits = createRealUnits
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD"), check.IsNil)
}

func (t *testWorkerEtcdCompact) TestWatchSubtaskStageEtcdCompact(c *check.C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)

	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, check.IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), check.IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, check.IsNil)
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.From = config.GetDBConfigForTest()
	sourceCfg.EnableRelay = false

	// step 1: start worker
	w, err := NewSourceWorker(sourceCfg, etcdCli, "", "")
	c.Assert(err, check.IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer w.Stop(true)
	go func() {
		w.Start()
	}()
	c.Assert(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	}), check.IsTrue)
	// step 2: Put a subtask config and subtask stage to this source, then delete it
	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.Decode(config.SampleSubtaskConfig, true)
	c.Assert(err, check.IsNil)
	subtaskCfg.MydumperPath = mydumperPath

	_, err = ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	c.Assert(err, check.IsNil)
	rev, err := ha.DeleteSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg},
		[]ha.Stage{ha.NewSubTaskStage(pb.Stage_Stopped, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	c.Assert(err, check.IsNil)
	// step 2.1: start a subtask manually
	c.Assert(w.StartSubTask(&subtaskCfg, pb.Stage_Running, pb.Stage_Stopped, true), check.IsNil)
	// step 3: trigger etcd compaction and check whether we can receive it through watcher
	_, err = etcdCli.Compact(ctx, rev)
	c.Assert(err, check.IsNil)
	subTaskStageCh := make(chan ha.Stage, 10)
	subTaskErrCh := make(chan error, 10)
	ha.WatchSubTaskStage(ctx, etcdCli, sourceCfg.SourceID, startRev, subTaskStageCh, subTaskErrCh)
	select {
	case err = <-subTaskErrCh:
		c.Assert(errors.Cause(err), check.Equals, etcdErrCompacted)
	case <-time.After(300 * time.Millisecond):
		c.Fatal("fail to get etcd error compacted")
	}
	// step 4: watch subtask stage from startRev
	c.Assert(w.subTaskHolder.findSubTask(subtaskCfg.Name), check.NotNil)
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(w.observeSubtaskStage(ctx1, etcdCli, startRev), check.IsNil)
	}()
	time.Sleep(time.Second)
	// step 4.1: after observe, invalid subtask should be removed
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) == nil
	}), check.IsTrue)
	// step 4.2: add a new subtask stage, worker should receive and start it
	_, err = ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	c.Assert(err, check.IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) != nil
	}), check.IsTrue)
	mockDB := conn.InitMockDB(c)
	mockShowMasterStatus(mockDB)
	status, _, err := w.QueryStatus(ctx1, subtaskCfg.Name)
	c.Assert(err, check.IsNil)
	c.Assert(status, check.HasLen, 1)
	c.Assert(status[0].Name, check.Equals, subtaskCfg.Name)
	c.Assert(status[0].Stage, check.Equals, pb.Stage_Running)
	cancel1()
	wg.Wait()
	w.subTaskHolder.closeAllSubTasks()
	// step 5: restart observe and start from startRev, this subtask should be added
	ctx2, cancel2 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(w.observeSubtaskStage(ctx2, etcdCli, startRev), check.IsNil)
	}()
	time.Sleep(time.Second)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) != nil
	}), check.IsTrue)
	mockShowMasterStatus(mockDB)
	status, _, err = w.QueryStatus(ctx2, subtaskCfg.Name)
	c.Assert(err, check.IsNil)
	c.Assert(status, check.HasLen, 1)
	c.Assert(status[0].Name, check.Equals, subtaskCfg.Name)
	c.Assert(status[0].Stage, check.Equals, pb.Stage_Running)
	w.Stop(true)
	cancel2()
	wg.Wait()
}

func (t *testWorkerEtcdCompact) TestWatchValidatorStageEtcdCompact(c *check.C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)

	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, check.IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), check.IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, check.IsNil)
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.From = config.GetDBConfigForTest()
	sourceCfg.EnableRelay = false

	//
	// step 1: start worker
	w, err := NewSourceWorker(sourceCfg, etcdCli, "", "")
	c.Assert(err, check.IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer w.Stop(true)
	go func() {
		w.Start()
	}()
	c.Assert(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	}), check.IsTrue)

	//
	// step 2: Put a subtask config and subtask stage to this source, then delete it
	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.Decode(config.SampleSubtaskConfig, true)
	c.Assert(err, check.IsNil)
	subtaskCfg.MydumperPath = mydumperPath
	subtaskCfg.ValidatorCfg = config.ValidatorConfig{Mode: config.ValidationNone}

	// increase revision
	_, err = etcdCli.Put(context.Background(), "/dummy-key", "value")
	c.Assert(err, check.IsNil)
	rev, err := ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	c.Assert(err, check.IsNil)

	//
	// step 2.1: start a subtask manually
	c.Assert(w.StartSubTask(&subtaskCfg, pb.Stage_Running, pb.Stage_Stopped, true), check.IsNil)

	//
	// step 3: trigger etcd compaction and check whether we can receive it through watcher
	_, err = etcdCli.Compact(ctx, rev)
	c.Assert(err, check.IsNil)
	subTaskStageCh := make(chan ha.Stage, 10)
	subTaskErrCh := make(chan error, 10)
	ctxForWatch, cancelFunc := context.WithCancel(ctx)
	ha.WatchValidatorStage(ctxForWatch, etcdCli, sourceCfg.SourceID, startRev, subTaskStageCh, subTaskErrCh)
	select {
	case err = <-subTaskErrCh:
		c.Assert(errors.Cause(err), check.Equals, etcdErrCompacted)
	case <-time.After(300 * time.Millisecond):
		c.Fatal("fail to get etcd error compacted")
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
	c.Assert(subTask, check.NotNil)
	c.Assert(getValidator(), check.IsNil)
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(w.observeValidatorStage(ctx1, startRev), check.IsNil)
	}()
	time.Sleep(time.Second)

	subtaskCfg.ValidatorCfg = config.ValidatorConfig{Mode: config.ValidationFast}
	unitBakup := subTask.units[len(subTask.units)-1]
	subTask.units[len(subTask.units)-1] = &syncer.Syncer{} // validator need a Syncer, not a mocked unit
	validatorStage := ha.NewValidatorStage(pb.Stage_Running, subtaskCfg.SourceID, subtaskCfg.Name)
	_, err = ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg}, nil, []ha.Stage{validatorStage})
	c.Assert(err, check.IsNil)

	// validator created
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return getValidator() != nil
	}), check.IsTrue)

	subTask.units[len(subTask.units)-1] = unitBakup // restore unit
	cancel1()
	wg.Wait()

	// test operate validator
	err = w.operateValidatorStage(ha.Stage{IsDeleted: true})
	c.Assert(err, check.IsNil)
	err = w.operateValidatorStage(ha.Stage{Expect: pb.Stage_Running, Task: "not-exist"})
	c.Assert(err, check.IsNil)
	err = w.operateValidatorStage(ha.Stage{Expect: pb.Stage_Running, Task: subtaskCfg.Name})
	c.Assert(err, check.ErrorMatches, ".*failed to get subtask config.*")
	err = w.operateValidatorStage(ha.Stage{Expect: pb.Stage_Running, Source: subtaskCfg.SourceID, Task: subtaskCfg.Name})
	c.Assert(err, check.IsNil)
}

func (t *testWorkerEtcdCompact) TestWatchRelayStageEtcdCompact(c *check.C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, check.IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), check.IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, check.IsNil)
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.EnableRelay = true
	sourceCfg.RelayDir = c.MkDir()
	sourceCfg.MetaDir = c.MkDir()

	// step 1: start worker
	w, err := NewSourceWorker(sourceCfg, etcdCli, "", "")
	c.Assert(err, check.IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer w.Stop(true)
	go func() {
		c.Assert(w.EnableRelay(false), check.IsNil)
		w.Start()
	}()
	c.Assert(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	}), check.IsTrue)
	// step 2: Put a relay stage to this source, then delete it
	// put mysql config into relative etcd key adapter to trigger operation event
	c.Assert(w.relayHolder, check.NotNil)
	_, err = ha.PutSourceCfg(etcdCli, sourceCfg)
	c.Assert(err, check.IsNil)
	rev, err := ha.PutRelayStageRelayConfigSourceBound(etcdCli, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID),
		ha.NewSourceBound(sourceCfg.SourceID, cfg.Name))
	c.Assert(err, check.IsNil)
	// check relay stage, should be running
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Running
	}), check.IsTrue)
	// step 3: trigger etcd compaction and check whether we can receive it through watcher, then we delete relay stage
	_, err = etcdCli.Compact(ctx, rev)
	c.Assert(err, check.IsNil)
	_, err = ha.DeleteSourceCfgRelayStageSourceBound(etcdCli, sourceCfg.SourceID, cfg.Name)
	c.Assert(err, check.IsNil)
	relayStageCh := make(chan ha.Stage, 10)
	relayErrCh := make(chan error, 10)
	ha.WatchRelayStage(ctx, etcdCli, cfg.Name, startRev, relayStageCh, relayErrCh)
	select {
	case err := <-relayErrCh:
		c.Assert(errors.Cause(err), check.Equals, etcdErrCompacted)
	case <-time.After(300 * time.Millisecond):
		c.Fatal("fail to get etcd error compacted")
	}
	// step 4: should stop the running relay because see deletion after compaction
	time.Sleep(time.Second)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Stopped
	}), check.IsTrue)
}

func (t *testServer) testSourceWorker(c *check.C) {
	cfg := loadSourceConfigWithoutPassword(c)

	dir := c.MkDir()
	cfg.EnableRelay = true
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, check.IsNil)
	defer ETCD.Close()
	workerCfg := NewConfig()
	c.Assert(workerCfg.Parse([]string{"-config=./dm-worker.toml"}), check.IsNil)
	workerCfg.Join = masterAddr
	workerCfg.KeepAliveTTL = keepAliveTTL
	workerCfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(workerCfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, check.IsNil)

	NewRelayHolder = NewDummyRelayHolderWithInitError
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()
	w, err := NewSourceWorker(cfg, etcdCli, "", "")
	c.Assert(err, check.IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD", `return(true)`), check.IsNil)
	c.Assert(w.EnableRelay(false), check.ErrorMatches, "init error")
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/worker/MockGetSourceCfgFromETCD"), check.IsNil)

	NewRelayHolder = NewDummyRelayHolder
	w, err = NewSourceWorker(cfg, etcdCli, "", "")
	c.Assert(err, check.IsNil)
	c.Assert(w.GetUnitAndSourceStatusJSON("", nil), check.HasLen, emptyWorkerStatusInfoJSONLength)

	// stop twice
	w.Stop(true)
	c.Assert(w.closed.Load(), check.IsTrue)
	c.Assert(w.subTaskHolder.getAllSubTasks(), check.HasLen, 0)
	w.Stop(true)
	c.Assert(w.closed.Load(), check.IsTrue)
	c.Assert(w.subTaskHolder.getAllSubTasks(), check.HasLen, 0)
	c.Assert(w.closed.Load(), check.IsTrue)

	c.Assert(w.StartSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	}, pb.Stage_Running, pb.Stage_Stopped, true), check.IsNil)
	task := w.subTaskHolder.findSubTask("testStartTask")
	c.Assert(task, check.NotNil)
	c.Assert(task.Result().String(), check.Matches, ".*worker already closed.*")

	c.Assert(w.StartSubTask(&config.SubTaskConfig{
		Name: "testStartTask-in-stopped",
	}, pb.Stage_Stopped, pb.Stage_Stopped, true), check.IsNil)
	task = w.subTaskHolder.findSubTask("testStartTask-in-stopped")
	c.Assert(task, check.NotNil)
	c.Assert(task.Result().String(), check.Matches, ".*worker already closed.*")

	err = w.UpdateSubTask(context.Background(), &config.SubTaskConfig{
		Name: "testStartTask",
	}, true)
	c.Assert(err, check.ErrorMatches, ".*worker already closed.*")

	err = w.OperateSubTask("testSubTask", pb.TaskOp_Delete)
	c.Assert(err, check.ErrorMatches, ".*worker already closed.*")
}

func (t *testServer) TestQueryValidator(c *check.C) {
	cfg := loadSourceConfigWithoutPassword(c)

	dir := c.MkDir()
	cfg.EnableRelay = true
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	w, err := NewSourceWorker(cfg, nil, "", "")
	w.closed.Store(false)
	c.Assert(err, check.IsNil)
	st := NewSubTaskWithStage(&config.SubTaskConfig{
		Name: "testQueryValidator",
		ValidatorCfg: config.ValidatorConfig{
			Mode: config.ValidationFull,
		},
	}, pb.Stage_Running, nil, "")
	w.subTaskHolder.recordSubTask(st)
	var ret *pb.ValidationStatus
	ret, err = w.GetValidatorStatus("invalidTaskName")
	c.Assert(ret, check.IsNil)
	c.Assert(terror.ErrWorkerSubTaskNotFound.Equal(err), check.IsTrue)
}

func (t *testServer) setupValidator(c *check.C) *SourceWorker {
	cfg := loadSourceConfigWithoutPassword(c)

	dir := c.MkDir()
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
	c.Assert(err, check.IsNil)
	return w
}

func (t *testServer) TestGetWorkerValidatorErr(c *check.C) {
	w := t.setupValidator(c)
	// when subtask name not exists
	// return empty array
	errs, err := w.GetWorkerValidatorErr("invalidTask", pb.ValidateErrorState_InvalidErr)
	c.Assert(terror.ErrWorkerSubTaskNotFound.Equal(err), check.IsTrue)
	c.Assert(errs, check.IsNil)
}

func (t *testServer) TestOperateWorkerValidatorErr(c *check.C) {
	w := t.setupValidator(c)
	// when subtask name not exists
	// return empty array
	taskNotFound := terror.ErrWorkerSubTaskNotFound.Generate("invalidTask")
	c.Assert(w.OperateWorkerValidatorErr("invalidTask", pb.ValidationErrOp_ClearErrOp, 0, true).Error(), check.Equals, taskNotFound.Error())
}

func TestMasterBinlogOff(t *testing.T) {
	ctx := context.Background()
	cfg, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t, err)
	cfg.From.Password = "no need to connect"

	w, err := NewSourceWorker(cfg, nil, "", "")
	require.NoError(t, err)
	w.closed.Store(false)

	// start task
	var subtaskCfg config.SubTaskConfig
	require.NoError(t, subtaskCfg.Decode(config.SampleSubtaskConfig, true))
	require.NoError(t, w.StartSubTask(&subtaskCfg, pb.Stage_Running, pb.Stage_Stopped, true))

	_, mockDB, err := conn.InitMockDBFull()
	require.NoError(t, err)
	mockShowMasterStatusNoRows(mockDB)
	status, _, err := w.QueryStatus(ctx, subtaskCfg.Name)
	require.NoError(t, err)
	require.Len(t, status, 1)
	require.Equal(t, subtaskCfg.Name, status[0].Name)
}
