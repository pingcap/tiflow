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
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/conn"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

var emptyWorkerStatusInfoJSONLength = 25

func mockShowMasterStatus(mockDB sqlmock.Sqlmock) {
	rows := mockDB.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).AddRow(
		"mysql-bin.000009", 11232, nil, nil, "074be7f4-f0f1-11ea-95bd-0242ac120002:1-699",
	)
	mockDB.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)
}

func (t *testServer) testWorker(c *C) {
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
	c.Assert(err, IsNil)
	defer ETCD.Close()
	workerCfg := NewConfig()
	c.Assert(workerCfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	workerCfg.Join = masterAddr
	workerCfg.KeepAliveTTL = keepAliveTTL
	workerCfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(workerCfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, IsNil)

	NewRelayHolder = NewDummyRelayHolderWithInitError
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()
	w, err := NewSourceWorker(cfg, etcdCli, "")
	c.Assert(err, IsNil)
	c.Assert(w.EnableRelay(), ErrorMatches, "init error")

	NewRelayHolder = NewDummyRelayHolder
	w, err = NewSourceWorker(cfg, etcdCli, "")
	c.Assert(err, IsNil)
	c.Assert(w.GetUnitAndSourceStatusJSON("", nil), HasLen, emptyWorkerStatusInfoJSONLength)

	// close twice
	w.Close()
	c.Assert(w.closed.Load(), IsTrue)
	c.Assert(w.subTaskHolder.getAllSubTasks(), HasLen, 0)
	w.Close()
	c.Assert(w.closed.Load(), IsTrue)
	c.Assert(w.subTaskHolder.getAllSubTasks(), HasLen, 0)
	c.Assert(w.closed.Load(), IsTrue)

	c.Assert(w.StartSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	}, pb.Stage_Running, true), IsNil)
	task := w.subTaskHolder.findSubTask("testStartTask")
	c.Assert(task, NotNil)
	c.Assert(task.Result().String(), Matches, ".*worker already closed.*")

	err = w.UpdateSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	})
	c.Assert(err, ErrorMatches, ".*worker already closed.*")

	err = w.OperateSubTask("testSubTask", pb.TaskOp_Stop)
	c.Assert(err, ErrorMatches, ".*worker already closed.*")
}

type testServer2 struct{}

var _ = Suite(&testServer2{})

func (t *testServer2) SetUpSuite(c *C) {
	err := log.InitLogger(&log.Config{})
	c.Assert(err, IsNil)

	getMinLocForSubTaskFunc = getFakeLocForSubTask
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/MockGetSourceCfgFromETCD", `return(true)`), IsNil)
}

func (t *testServer2) TearDownSuite(c *C) {
	getMinLocForSubTaskFunc = getMinLocForSubTask
	c.Assert(failpoint.Disable("github.com/pingcap/dm/dm/worker/MockGetSourceCfgFromETCD"), IsNil)
}

func (t *testServer2) TestTaskAutoResume(c *C) {
	var (
		taskName = "sub-task-name"
		port     = 8263
	)
	hostName := "127.0.0.1:18261"
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+hostName)
	c.Assert(err, IsNil)
	defer ETCD.Close()

	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
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

	c.Assert(failpoint.Enable("github.com/pingcap/dm/dumpling/dumpUnitProcessForever", `return()`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessForever")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/mockCreateUnitsDumpOnly", `return(true)`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/dm/worker/mockCreateUnitsDumpOnly")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/loader/ignoreLoadCheckpointErr", `return()`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/loader/ignoreLoadCheckpointErr")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError", `return("test auto resume inject error")`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError")

	s := NewServer(cfg)
	defer s.Close()
	go func() {
		c.Assert(s.Start(), IsNil)
	}()
	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		if s.closed.Load() {
			return false
		}
		w, err2 := s.getOrStartWorker(sourceConfig, true)
		c.Assert(err2, IsNil)
		// we set sourceConfig.EnableRelay = true above
		c.Assert(w.EnableRelay(), IsNil)
		c.Assert(w.EnableHandleSubtasks(), IsNil)
		return true
	}), IsTrue)
	// start task
	var subtaskCfg config.SubTaskConfig
	c.Assert(subtaskCfg.DecodeFile("./subtask.toml", true), IsNil)
	c.Assert(err, IsNil)
	subtaskCfg.Mode = "full"
	c.Assert(s.getWorker(true).StartSubTask(&subtaskCfg, pb.Stage_Running, true), IsNil)

	// check task in paused state
	c.Assert(utils.WaitSomething(100, 100*time.Millisecond, func() bool {
		subtaskStatus, _, _ := s.getWorker(true).QueryStatus(context.Background(), taskName)
		for _, st := range subtaskStatus {
			if st.Name == taskName && st.Stage == pb.Stage_Paused {
				return true
			}
		}
		return false
	}), IsTrue)
	//nolint:errcheck
	failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError")

	rtsc, ok := s.getWorker(true).taskStatusChecker.(*realTaskStatusChecker)
	c.Assert(ok, IsTrue)
	defer func() {
		// close multiple time
		rtsc.Close()
		rtsc.Close()
	}()

	// check task will be auto resumed
	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		sts, _, _ := s.getWorker(true).QueryStatus(context.Background(), taskName)
		for _, st := range sts {
			if st.Name == taskName && st.Stage == pb.Stage_Running {
				return true
			}
		}
		c.Log(sts)
		return false
	}), IsTrue)
}

type testWorkerFunctionalities struct {
	createUnitCount         int32
	expectedCreateUnitCount int32
}

var _ = Suite(&testWorkerFunctionalities{})

func (t *testWorkerFunctionalities) SetUpSuite(c *C) {
	NewRelayHolder = NewDummyRelayHolder
	NewSubTask = NewRealSubTask
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string) []unit.Unit {
		atomic.AddInt32(&t.createUnitCount, 1)
		mockDumper := NewMockUnit(pb.UnitType_Dump)
		mockLoader := NewMockUnit(pb.UnitType_Load)
		mockSync := NewMockUnit(pb.UnitType_Sync)
		return []unit.Unit{mockDumper, mockLoader, mockSync}
	}
	getMinLocForSubTaskFunc = getFakeLocForSubTask
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/MockGetSourceCfgFromETCD", `return(true)`), IsNil)
}

func (t *testWorkerFunctionalities) TearDownSuite(c *C) {
	NewRelayHolder = NewRealRelayHolder
	NewSubTask = NewRealSubTask
	createUnits = createRealUnits
	getMinLocForSubTaskFunc = getMinLocForSubTask
	c.Assert(failpoint.Disable("github.com/pingcap/dm/dm/worker/MockGetSourceCfgFromETCD"), IsNil)
}

func (t *testWorkerFunctionalities) TestWorkerFunctionalities(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, IsNil)
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.EnableRelay = false

	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.DecodeFile(subtaskSampleFile, true)
	c.Assert(err, IsNil)

	// start worker
	w, err := NewSourceWorker(sourceCfg, etcdCli, "")
	c.Assert(err, IsNil)
	defer w.Close()
	go func() {
		w.Start()
	}()
	c.Assert(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	}), IsTrue)

	// test 1: when subTaskEnabled is false, switch on relay
	c.Assert(w.subTaskEnabled.Load(), IsFalse)
	t.testEnableRelay(c, w, etcdCli, sourceCfg, cfg)

	// test2: when subTaskEnabled is false, switch off relay
	c.Assert(w.subTaskEnabled.Load(), IsFalse)
	t.testDisableRelay(c, w)

	// test3: when relayEnabled is false, switch on subtask
	c.Assert(w.relayEnabled.Load(), IsFalse)

	t.testEnableHandleSubtasks(c, w, etcdCli, subtaskCfg, sourceCfg)

	// test4: when subTaskEnabled is true, switch on relay
	c.Assert(w.subTaskEnabled.Load(), IsTrue)

	t.testEnableRelay(c, w, etcdCli, sourceCfg, cfg)
	c.Assert(w.subTaskHolder.findSubTask(subtaskCfg.Name).cfg.UseRelay, IsTrue)
	t.expectedCreateUnitCount++
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return atomic.LoadInt32(&t.createUnitCount) == t.expectedCreateUnitCount
	}), IsTrue)

	// test5: when subTaskEnabled is true, switch off relay
	c.Assert(w.subTaskEnabled.Load(), IsTrue)
	t.testDisableRelay(c, w)

	c.Assert(w.subTaskHolder.findSubTask(subtaskCfg.Name).cfg.UseRelay, IsFalse)
	t.expectedCreateUnitCount++
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return atomic.LoadInt32(&t.createUnitCount) == t.expectedCreateUnitCount
	}), IsTrue)

	// test6: when relayEnabled is false, switch off subtask
	c.Assert(w.relayEnabled.Load(), IsFalse)

	w.DisableHandleSubtasks()
	c.Assert(w.subTaskEnabled.Load(), IsFalse)

	// prepare for test7 & 8
	t.testEnableRelay(c, w, etcdCli, sourceCfg, cfg)
	// test7: when relayEnabled is true, switch on subtask
	c.Assert(w.relayEnabled.Load(), IsTrue)

	subtaskCfg2 := subtaskCfg
	subtaskCfg2.Name = "sub-task-name-2"
	// we already added subtaskCfg, so below EnableHandleSubtasks will find an extra subtask
	t.expectedCreateUnitCount++
	t.testEnableHandleSubtasks(c, w, etcdCli, subtaskCfg2, sourceCfg)
	c.Assert(w.subTaskHolder.findSubTask(subtaskCfg.Name).cfg.UseRelay, IsTrue)
	c.Assert(w.subTaskHolder.findSubTask(subtaskCfg2.Name).cfg.UseRelay, IsTrue)

	// test8: when relayEnabled is true, switch off subtask
	c.Assert(w.relayEnabled.Load(), IsTrue)

	w.DisableHandleSubtasks()
	c.Assert(w.subTaskEnabled.Load(), IsFalse)
}

func (t *testWorkerFunctionalities) testEnableRelay(c *C, w *SourceWorker, etcdCli *clientv3.Client,
	sourceCfg *config.SourceConfig, cfg *Config) {
	c.Assert(w.EnableRelay(), IsNil)

	c.Assert(w.relayEnabled.Load(), IsTrue)
	c.Assert(w.relayHolder.Stage(), Equals, pb.Stage_New)

	_, err := ha.PutSourceCfg(etcdCli, sourceCfg)
	c.Assert(err, IsNil)
	_, err = ha.PutRelayStageRelayConfigSourceBound(etcdCli, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID),
		ha.NewSourceBound(sourceCfg.SourceID, cfg.Name))
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Running
	}), IsTrue)

	_, err = ha.DeleteSourceCfgRelayStageSourceBound(etcdCli, sourceCfg.SourceID, cfg.Name)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Stopped
	}), IsTrue)
}

func (t *testWorkerFunctionalities) testDisableRelay(c *C, w *SourceWorker) {
	w.DisableRelay()

	c.Assert(w.relayEnabled.Load(), IsFalse)
	c.Assert(w.relayHolder, IsNil)
}

func (t *testWorkerFunctionalities) testEnableHandleSubtasks(c *C, w *SourceWorker, etcdCli *clientv3.Client,
	subtaskCfg config.SubTaskConfig, sourceCfg *config.SourceConfig) {
	c.Assert(w.EnableHandleSubtasks(), IsNil)
	c.Assert(w.subTaskEnabled.Load(), IsTrue)

	_, err := ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg},
		[]ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)})
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) != nil
	}), IsTrue)
	t.expectedCreateUnitCount++
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return atomic.LoadInt32(&t.createUnitCount) == t.expectedCreateUnitCount
	}), IsTrue)
}

type testWorkerEtcdCompact struct{}

var _ = Suite(&testWorkerEtcdCompact{})

func (t *testWorkerEtcdCompact) SetUpSuite(c *C) {
	NewRelayHolder = NewDummyRelayHolder
	NewSubTask = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string) *SubTask {
		cfg.UseRelay = false
		return NewRealSubTask(cfg, etcdClient, worker)
	}
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string) []unit.Unit {
		mockDumper := NewMockUnit(pb.UnitType_Dump)
		mockLoader := NewMockUnit(pb.UnitType_Load)
		mockSync := NewMockUnit(pb.UnitType_Sync)
		return []unit.Unit{mockDumper, mockLoader, mockSync}
	}
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/MockGetSourceCfgFromETCD", `return(true)`), IsNil)
}

func (t *testWorkerEtcdCompact) TearDownSuite(c *C) {
	NewRelayHolder = NewRealRelayHolder
	NewSubTask = NewRealSubTask
	createUnits = createRealUnits
	c.Assert(failpoint.Disable("github.com/pingcap/dm/dm/worker/MockGetSourceCfgFromETCD"), IsNil)
}

func (t *testWorkerEtcdCompact) TestWatchSubtaskStageEtcdCompact(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)

	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, IsNil)
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.From = config.GetDBConfigForTest()
	sourceCfg.EnableRelay = false

	// step 1: start worker
	w, err := NewSourceWorker(sourceCfg, etcdCli, "")
	c.Assert(err, IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer w.Close()
	go func() {
		w.Start()
	}()
	c.Assert(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	}), IsTrue)
	// step 2: Put a subtask config and subtask stage to this source, then delete it
	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.DecodeFile(subtaskSampleFile, true)
	c.Assert(err, IsNil)
	subtaskCfg.MydumperPath = mydumperPath

	_, err = ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg},
		[]ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)})
	c.Assert(err, IsNil)
	rev, err := ha.DeleteSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg},
		[]ha.Stage{ha.NewSubTaskStage(pb.Stage_Stopped, sourceCfg.SourceID, subtaskCfg.Name)})
	c.Assert(err, IsNil)
	// step 2.1: start a subtask manually
	c.Assert(w.StartSubTask(&subtaskCfg, pb.Stage_Running, true), IsNil)
	// step 3: trigger etcd compaction and check whether we can receive it through watcher
	_, err = etcdCli.Compact(ctx, rev)
	c.Assert(err, IsNil)
	subTaskStageCh := make(chan ha.Stage, 10)
	subTaskErrCh := make(chan error, 10)
	ha.WatchSubTaskStage(ctx, etcdCli, sourceCfg.SourceID, startRev, subTaskStageCh, subTaskErrCh)
	select {
	case err = <-subTaskErrCh:
		c.Assert(err, Equals, etcdErrCompacted)
	case <-time.After(300 * time.Millisecond):
		c.Fatal("fail to get etcd error compacted")
	}
	// step 4: watch subtask stage from startRev
	c.Assert(w.subTaskHolder.findSubTask(subtaskCfg.Name), NotNil)
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(w.observeSubtaskStage(ctx1, etcdCli, startRev), IsNil)
	}()
	time.Sleep(time.Second)
	// step 4.1: after observe, invalid subtask should be removed
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) == nil
	}), IsTrue)
	// step 4.2: add a new subtask stage, worker should receive and start it
	_, err = ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg},
		[]ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)})
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) != nil
	}), IsTrue)
	mockDB := conn.InitMockDB(c)
	mockShowMasterStatus(mockDB)
	status, _, err := w.QueryStatus(ctx1, subtaskCfg.Name)
	c.Assert(err, IsNil)
	c.Assert(status, HasLen, 1)
	c.Assert(status[0].Name, Equals, subtaskCfg.Name)
	c.Assert(status[0].Stage, Equals, pb.Stage_Running)
	cancel1()
	wg.Wait()
	w.subTaskHolder.closeAllSubTasks()
	// step 5: restart observe and start from startRev, this subtask should be added
	ctx2, cancel2 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(w.observeSubtaskStage(ctx2, etcdCli, startRev), IsNil)
	}()
	time.Sleep(time.Second)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) != nil
	}), IsTrue)
	mockShowMasterStatus(mockDB)
	status, _, err = w.QueryStatus(ctx2, subtaskCfg.Name)
	c.Assert(err, IsNil)
	c.Assert(status, HasLen, 1)
	c.Assert(status[0].Name, Equals, subtaskCfg.Name)
	c.Assert(status[0].Stage, Equals, pb.Stage_Running)
	w.Close()
	cancel2()
	wg.Wait()
}

func (t *testWorkerEtcdCompact) TestWatchRelayStageEtcdCompact(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, IsNil)
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.EnableRelay = true
	sourceCfg.RelayDir = c.MkDir()
	sourceCfg.MetaDir = c.MkDir()

	// step 1: start worker
	w, err := NewSourceWorker(sourceCfg, etcdCli, "")
	c.Assert(err, IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer w.Close()
	go func() {
		c.Assert(w.EnableRelay(), IsNil)
		w.Start()
	}()
	c.Assert(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	}), IsTrue)
	// step 2: Put a relay stage to this source, then delete it
	// put mysql config into relative etcd key adapter to trigger operation event
	c.Assert(w.relayHolder, NotNil)
	_, err = ha.PutSourceCfg(etcdCli, sourceCfg)
	c.Assert(err, IsNil)
	rev, err := ha.PutRelayStageRelayConfigSourceBound(etcdCli, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID),
		ha.NewSourceBound(sourceCfg.SourceID, cfg.Name))
	c.Assert(err, IsNil)
	// check relay stage, should be running
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Running
	}), IsTrue)
	// step 3: trigger etcd compaction and check whether we can receive it through watcher, then we delete relay stage
	_, err = etcdCli.Compact(ctx, rev)
	c.Assert(err, IsNil)
	_, err = ha.DeleteSourceCfgRelayStageSourceBound(etcdCli, sourceCfg.SourceID, cfg.Name)
	c.Assert(err, IsNil)
	relayStageCh := make(chan ha.Stage, 10)
	relayErrCh := make(chan error, 10)
	ha.WatchRelayStage(ctx, etcdCli, cfg.Name, startRev, relayStageCh, relayErrCh)
	select {
	case err := <-relayErrCh:
		c.Assert(err, Equals, etcdErrCompacted)
	case <-time.After(300 * time.Millisecond):
		c.Fatal("fail to get etcd error compacted")
	}
	// step 4: should stop the running relay because see deletion after compaction
	time.Sleep(time.Second)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Stopped
	}), IsTrue)
}
