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

package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

const (
	noRestart          = iota // do nothing in rebuildPessimist, just keep testing
	restartOnly               // restart without building new instance. mock leader role transfer
	restartNewInstance        // restart with build a new instance. mock progress restore from failure
)

var (
	etcdErrCompacted = v3rpc.ErrCompacted
	testRelayDir     = "./test_relay_dir"
)

func TestSchedulerSuite(t *testing.T) {
	suite.Run(t, new(testSchedulerSuite))
}

// clear keys in etcd test cluster.
func (t *testSchedulerSuite) clearTestInfoOperation() {
	t.T().Helper()
	require.NoError(t.T(), ha.ClearTestInfoOperation(t.etcdTestCli))
}

type testSchedulerSuite struct {
	suite.Suite
	mockCluster *integration.ClusterV3
	etcdTestCli *clientv3.Client
}

func (t *testSchedulerSuite) SetupSuite() {
	require.NoError(t.T(), log.InitLogger(&log.Config{}))

	integration.BeforeTestExternal(t.T())
	t.mockCluster = integration.NewClusterV3(t.T(), &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.mockCluster.RandClient()
}

func (t *testSchedulerSuite) TearDownSuite() {
	t.mockCluster.Terminate(t.T())
}

func (t *testSchedulerSuite) TearDownTest() {
	t.clearTestInfoOperation()
}

var stageEmpty ha.Stage

func (t *testSchedulerSuite) TestScheduler() {
	t.testSchedulerProgress(noRestart)
	t.testSchedulerProgress(restartOnly)
	t.testSchedulerProgress(restartNewInstance)
}

func (t *testSchedulerSuite) testSchedulerProgress(restart int) {
	defer t.clearTestInfoOperation()

	var (
		logger       = log.L()
		s            = NewScheduler(&logger, security.Security{})
		sourceID1    = "mysql-replica-1"
		sourceID2    = "mysql-replica-2"
		workerName1  = "dm-worker-1"
		workerName2  = "dm-worker-2"
		workerAddr1  = "127.0.0.1:8262"
		workerAddr2  = "127.0.0.1:18262"
		taskName1    = "task-1"
		taskName2    = "task-2"
		workerInfo1  = ha.NewWorkerInfo(workerName1, workerAddr1)
		workerInfo2  = ha.NewWorkerInfo(workerName2, workerAddr2)
		subtaskCfg1  config.SubTaskConfig
		keepAliveTTL = int64(5) // NOTE: this should be >= minLeaseTTL, in second.

		rebuildScheduler = func(ctx context.Context) {
			switch restart {
			case restartOnly:
				s.Close()
				require.NoError(t.T(), s.Start(ctx, t.etcdTestCli))
			case restartNewInstance:
				s.Close()
				s = NewScheduler(&logger, security.Security{})
				require.NoError(t.T(), s.Start(ctx, t.etcdTestCli))
			}
		}
	)
	sourceCfg1, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t.T(), err)
	sourceCfg1.SourceID = sourceID1
	sourceCfg2 := *sourceCfg1
	sourceCfg2.SourceID = sourceID2

	require.NoError(t.T(), subtaskCfg1.Decode(config.SampleSubtaskConfig, true))
	subtaskCfg1.SourceID = sourceID1
	subtaskCfg1.Name = taskName1
	subtaskCfg1.LoaderConfig.ImportMode = config.LoadModePhysical
	require.NoError(t.T(), subtaskCfg1.Adjust(true))
	subtaskCfg21 := subtaskCfg1
	subtaskCfg21.Name = taskName2
	require.NoError(t.T(), subtaskCfg21.Adjust(true))
	subtaskCfg22 := subtaskCfg21
	subtaskCfg22.SourceID = sourceID2
	subtaskCfg22.ValidatorCfg = config.ValidatorConfig{Mode: config.ValidationFast}
	require.NoError(t.T(), subtaskCfg22.Adjust(true))

	// not started scheduler can't do anything.
	require.True(t.T(), terror.ErrSchedulerNotStarted.Equal(s.AddSourceCfg(sourceCfg1)))
	require.True(t.T(), terror.ErrSchedulerNotStarted.Equal(s.AddSourceCfgWithWorker(sourceCfg1, workerName1)))
	require.True(t.T(), terror.ErrSchedulerNotStarted.Equal(s.UpdateSourceCfg(sourceCfg1)))
	require.True(t.T(), terror.ErrSchedulerNotStarted.Equal(s.RemoveSourceCfg(sourceID1)))
	require.True(t.T(), terror.ErrSchedulerNotStarted.Equal(s.AddSubTasks(false, pb.Stage_Running, subtaskCfg1)))
	require.True(t.T(), terror.ErrSchedulerNotStarted.Equal(s.RemoveSubTasks(taskName1, sourceID1)))
	require.True(t.T(), terror.ErrSchedulerNotStarted.Equal(s.AddWorker(workerName1, workerAddr1)))
	require.True(t.T(), terror.ErrSchedulerNotStarted.Equal(s.RemoveWorker(workerName1)))
	require.True(t.T(), terror.ErrSchedulerNotStarted.Equal(s.UpdateExpectRelayStage(pb.Stage_Running, sourceID1)))
	require.True(t.T(), terror.ErrSchedulerNotStarted.Equal(s.UpdateExpectSubTaskStage(pb.Stage_Running, taskName1, sourceID1)))
	require.True(t.T(), terror.ErrSchedulerNotStarted.Equal(s.OperateValidationTask(nil, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous info.
	require.NoError(t.T(), s.Start(ctx, t.etcdTestCli))
	require.True(t.T(), terror.ErrSchedulerStarted.Equal(s.Start(ctx, t.etcdTestCli))) // start multiple times.
	s.Close()
	s.Close() // close multiple times.

	// CASE 2: start again without any previous info.
	require.NoError(t.T(), s.Start(ctx, t.etcdTestCli))

	// CASE 2.1: add the first source config.
	// no source config exist before added.
	t.sourceCfgNotExist(s, sourceID1)
	// add source config1.
	require.NoError(t.T(), s.AddSourceCfg(sourceCfg1))
	require.True(t.T(), terror.ErrSchedulerSourceCfgExist.Equal(s.AddSourceCfg(sourceCfg1))) // can't add multiple times.
	// the source config added.
	t.sourceCfgExist(s, sourceCfg1)

	// update source cfg
	sourceCfg1.RelayDir = testRelayDir
	require.NoError(t.T(), s.UpdateSourceCfg(sourceCfg1))
	newCfg := s.GetSourceCfgByID(sourceID1)
	require.Equal(t.T(), testRelayDir, newCfg.RelayDir)

	// update with invalid SourceID
	fake := newCfg.Clone()
	fake.SourceID = "not a source id"
	require.True(t.T(), terror.ErrSchedulerSourceCfgNotExist.Equal(s.UpdateSourceCfg(fake)))

	// one unbound source exist (because no free worker).
	t.sourceBounds(s, []string{}, []string{sourceID1})
	rebuildScheduler(ctx)

	// CASE 2.2: add the first worker.
	// no worker exist before added.
	t.workerNotExist(s, workerName1)
	// add worker1.
	require.NoError(t.T(), s.AddWorker(workerName1, workerAddr1))
	require.True(t.T(), terror.ErrSchedulerWorkerExist.Equal(s.AddWorker(workerName1, workerAddr2))) // can't add with different address now.
	require.NoError(t.T(), s.AddWorker(workerName1, workerAddr1))                                    // but can add the worker multiple times (like restart the worker).
	// the worker added.
	t.workerExist(s, workerInfo1)
	t.workerOffline(s, workerName1)
	// still no bounds (because the worker is offline).
	t.sourceBounds(s, []string{}, []string{sourceID1})
	// no expect relay stage exist (because the source has never been bound).
	t.relayStageMatch(s, sourceID1, pb.Stage_InvalidStage)
	rebuildScheduler(ctx)

	// CASE 2.3: the worker become online.
	// do keep-alive for worker1.
	ctx1, cancel1 := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx1, t.etcdTestCli, workerName1, keepAliveTTL))
	}()
	// wait for source1 being bound to worker1.
	require.Eventually(t.T(), func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	}, 3*time.Second, 100*time.Millisecond)
	t.sourceBounds(s, []string{sourceID1}, []string{})
	t.workerBound(s, ha.NewSourceBound(sourceID1, workerName1))
	require.NoError(t.T(), s.StartRelay(sourceID1, []string{workerName1}))
	// expect relay stage become Running after the start relay.
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 2.4: pause the relay.
	require.NoError(t.T(), s.UpdateExpectRelayStage(pb.Stage_Paused, sourceID1))
	t.relayStageMatch(s, sourceID1, pb.Stage_Paused)
	// update relay stage without source take no effect now (and return without error).
	require.NoError(t.T(), s.UpdateExpectRelayStage(pb.Stage_Running))
	t.relayStageMatch(s, sourceID1, pb.Stage_Paused)
	// update to non-(Running, Paused) stage is invalid.
	require.True(t.T(), terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_InvalidStage, sourceID1)))
	require.True(t.T(), terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_New, sourceID1)))
	require.True(t.T(), terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_Stopped, sourceID1)))
	require.True(t.T(), terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_Finished, sourceID1)))
	// can't update stage with not existing sources now.
	require.True(t.T(), terror.ErrSchedulerRelayStageSourceNotExist.Equal(s.UpdateExpectRelayStage(pb.Stage_Running, sourceID1, sourceID2)))
	t.relayStageMatch(s, sourceID1, pb.Stage_Paused)
	rebuildScheduler(ctx)

	// CASE 2.5: resume the relay.
	require.NoError(t.T(), s.UpdateExpectRelayStage(pb.Stage_Running, sourceID1))
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 2.6: start a task with only one source.
	// wait for source bound recovered
	require.Eventually(t.T(), func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	}, 3*time.Second, 100*time.Millisecond)
	// no subtask config exists before start.
	require.NoError(t.T(), s.AddSubTasks(false, pb.Stage_Running)) // can call without configs, return without error, but take no effect.
	t.subTaskCfgNotExist(s, taskName1, sourceID1)
	t.subTaskStageMatch(s, taskName1, sourceID1, pb.Stage_InvalidStage)
	t.downstreamMetaNotExist(s, taskName1)
	// start the task.
	require.NoError(t.T(), s.AddSubTasks(false, pb.Stage_Running, subtaskCfg1))
	require.True(t.T(), terror.ErrSchedulerSubTaskExist.Equal(s.AddSubTasks(false, pb.Stage_Running, subtaskCfg1))) // add again.
	// subtask config and stage exist.
	t.subTaskCfgExist(s, subtaskCfg1)
	t.subTaskStageMatch(s, taskName1, sourceID1, pb.Stage_Running)
	t.downstreamMetaExist(s, taskName1, subtaskCfg1.To, subtaskCfg1.MetaSchema)
	t.downstreamMetaNotExist(s, taskName2)
	// check lightning status is written to etcd
	status, err := ha.GetAllLightningStatus(t.etcdTestCli, taskName1)
	require.NoError(t.T(), err)
	require.Equal(t.T(), []string{ha.LightningNotReady}, status)

	// try start a task with two sources, some sources not bound.
	require.True(t.T(), terror.ErrSchedulerSourcesUnbound.Equal(s.AddSubTasks(false, pb.Stage_Running, subtaskCfg21, subtaskCfg22)))
	t.subTaskCfgNotExist(s, taskName2, sourceID1)
	t.subTaskStageMatch(s, taskName2, sourceID1, pb.Stage_InvalidStage)
	t.subTaskCfgNotExist(s, taskName2, sourceID2)
	t.subTaskStageMatch(s, taskName2, sourceID2, pb.Stage_InvalidStage)
	rebuildScheduler(ctx)

	// CASE 2.7: pause/resume task1.
	require.NoError(t.T(), s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName1, sourceID1))
	t.subTaskStageMatch(s, taskName1, sourceID1, pb.Stage_Paused)
	require.NoError(t.T(), s.UpdateExpectSubTaskStage(pb.Stage_Running, taskName1, sourceID1))
	t.subTaskStageMatch(s, taskName1, sourceID1, pb.Stage_Running)
	// update subtask stage without source or task take no effect now (and return without error).
	require.NoError(t.T(), s.UpdateExpectSubTaskStage(pb.Stage_Paused, "", sourceID1))
	require.NoError(t.T(), s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName1))
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	// update to non-(New, Finished) stage is invalid.
	require.True(t.T(), terror.ErrSchedulerSubTaskStageInvalidUpdate.Equal(s.UpdateExpectSubTaskStage(pb.Stage_InvalidStage, taskName1, sourceID1)))
	require.True(t.T(), terror.ErrSchedulerSubTaskStageInvalidUpdate.Equal(s.UpdateExpectSubTaskStage(pb.Stage_New, taskName1, sourceID1)))
	require.True(t.T(), terror.ErrSchedulerSubTaskStageInvalidUpdate.Equal(s.UpdateExpectSubTaskStage(pb.Stage_Finished, taskName1, sourceID1)))
	// can't update stage with not existing sources now.
	require.True(t.T(), terror.ErrSchedulerSubTaskOpSourceNotExist.Equal(s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName1, sourceID1, sourceID2)))
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 2.8: worker1 become offline.
	// cancel keep-alive.
	cancel1()
	wg.Wait()
	// wait for source1 unbound from worker1.
	require.Eventually(t.T(), func() bool {
		unbounds := s.UnboundSources()
		return len(unbounds) == 1 && unbounds[0] == sourceID1
	}, 3*time.Duration(keepAliveTTL)*time.Second, time.Second)
	t.sourceBounds(s, []string{}, []string{sourceID1})
	// static information are still there.
	t.sourceCfgExist(s, sourceCfg1)
	t.subTaskCfgExist(s, subtaskCfg1)
	t.workerExist(s, workerInfo1)
	// worker1 still exists, but it's offline.
	t.workerOffline(s, workerName1)
	// expect relay stage keep Running.
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(s, taskName1, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 3: start again with previous `Offline` worker, relay stage, subtask stage.
	// CASE 3.1: previous information should recover.
	// source1 is still unbound.
	t.sourceBounds(s, []string{}, []string{sourceID1})
	// worker1 still exists, but it's offline.
	t.workerOffline(s, workerName1)
	// static information are still there.
	t.sourceCfgExist(s, sourceCfg1)
	t.subTaskCfgExist(s, subtaskCfg1)
	t.workerExist(s, workerInfo1)
	// expect relay stage keep Running.
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(s, taskName1, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 3.2: start worker1 again.
	// do keep-alive for worker1 again.
	ctx1, cancel1 = context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx1, t.etcdTestCli, workerName1, keepAliveTTL))
	}()
	// wait for source1 bound to worker1.
	require.Eventually(t.T(), func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	}, 3*time.Second, 100*time.Millisecond)
	// source1 bound to worker1.
	t.sourceBounds(s, []string{sourceID1}, []string{})
	t.workerBound(s, ha.NewSourceBound(sourceID1, workerName1))
	// expect stages keep Running.
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(s, taskName1, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.1: previous information should recover.
	// source1 is still bound.
	t.sourceBounds(s, []string{sourceID1}, []string{})
	// worker1 still exists, and it's bound.
	t.workerBound(s, ha.NewSourceBound(sourceID1, workerName1))
	// static information are still there.
	t.sourceCfgExist(s, sourceCfg1)
	t.subTaskCfgExist(s, subtaskCfg1)
	t.workerExist(s, workerInfo1)
	// expect stages keep Running.
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(s, taskName1, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.2: add another worker into the cluster.
	// worker2 not exists before added.
	t.workerNotExist(s, workerName2)
	// add worker2.
	require.NoError(t.T(), s.AddWorker(workerName2, workerAddr2))
	// the worker added, but is offline.
	t.workerExist(s, workerInfo2)
	t.workerOffline(s, workerName2)
	rebuildScheduler(ctx)

	// CASE 4.3: the worker2 become online.
	// do keep-alive for worker2.
	ctx2, cancel2 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx2, t.etcdTestCli, workerName2, keepAliveTTL))
	}()
	// wait for worker2 become Free.
	require.Eventually(t.T(), func() bool {
		w := s.GetWorkerByName(workerName2)
		return w.Stage() == WorkerFree
	}, 3*time.Second, 100*time.Millisecond)
	t.workerFree(s, workerName2)
	rebuildScheduler(ctx)

	// CASE 4.4: add source config2.
	// wait for source bound recovered
	require.Eventually(t.T(), func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	}, 3*time.Second, 100*time.Millisecond)
	// source2 not exists before.
	t.sourceCfgNotExist(s, sourceID2)
	// add source2.
	require.NoError(t.T(), s.AddSourceCfg(&sourceCfg2))
	// source2 added.
	t.sourceCfgExist(s, &sourceCfg2)
	// source2 should bound to worker2.
	t.workerBound(s, ha.NewSourceBound(sourceID2, workerName2))
	t.sourceBounds(s, []string{sourceID1, sourceID2}, []string{})
	require.NoError(t.T(), s.StartRelay(sourceID2, []string{workerName2}))
	t.relayStageMatch(s, sourceID2, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.4.1: start a task with two sources.
	// can't add more than one tasks at a time now.
	require.True(t.T(), terror.ErrSchedulerMultiTask.Equal(s.AddSubTasks(false, pb.Stage_Running, subtaskCfg1, subtaskCfg21)))
	// task2' config and stage not exists before.
	t.subTaskCfgNotExist(s, taskName2, sourceID1)
	t.subTaskCfgNotExist(s, taskName2, sourceID2)
	t.subTaskStageMatch(s, taskName2, sourceID1, pb.Stage_InvalidStage)
	t.subTaskStageMatch(s, taskName2, sourceID2, pb.Stage_InvalidStage)
	// start task2.
	require.NoError(t.T(), s.AddSubTasks(false, pb.Stage_Running, subtaskCfg21, subtaskCfg22))
	// config added, stage become Running.
	t.subTaskCfgExist(s, subtaskCfg21)
	t.subTaskCfgExist(s, subtaskCfg22)
	t.subTaskStageMatch(s, taskName2, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(s, taskName2, sourceID2, pb.Stage_Running)
	t.validatorStageMatch(s, taskName2, sourceID2, pb.Stage_Running)
	// check lightning status is written to etcd
	status, err = ha.GetAllLightningStatus(t.etcdTestCli, taskName2)
	require.NoError(t.T(), err)
	require.Equal(t.T(), []string{ha.LightningNotReady, ha.LightningNotReady}, status)
	rebuildScheduler(ctx)

	// CASE 4.4.2 fail to stop any task.
	// can call without tasks or sources, return without error, but take no effect.
	require.NoError(t.T(), s.RemoveSubTasks("", sourceID1))
	require.NoError(t.T(), s.RemoveSubTasks(taskName1))
	// stop not exist task.
	require.True(t.T(), terror.ErrSchedulerSubTaskOpTaskNotExist.Equal(s.RemoveSubTasks("not-exist", sourceID1)))
	// config and stage not changed.
	t.subTaskCfgExist(s, subtaskCfg21)
	t.subTaskCfgExist(s, subtaskCfg22)
	t.subTaskStageMatch(s, taskName2, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(s, taskName2, sourceID2, pb.Stage_Running)

	// CASE 4.5: update subtasks stage from different current stage.
	// pause <task2, source1>.
	require.NoError(t.T(), s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName2, sourceID1))
	t.subTaskStageMatch(s, taskName2, sourceID1, pb.Stage_Paused)
	t.subTaskStageMatch(s, taskName2, sourceID2, pb.Stage_Running)
	// resume <task2, source1 and source2>.
	require.NoError(t.T(), s.UpdateExpectSubTaskStage(pb.Stage_Running, taskName2, sourceID1, sourceID2))
	t.subTaskStageMatch(s, taskName2, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(s, taskName2, sourceID2, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.6: try remove source when subtasks exist.
	// wait for source bound recovered
	require.Eventually(t.T(), func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 2 && bounds[0] == sourceID1 && bounds[1] == sourceID2
	}, 3*time.Second, 100*time.Millisecond)
	require.True(t.T(), terror.ErrSchedulerSourceOpTaskExist.Equal(s.RemoveSourceCfg(sourceID2)))
	// source2 keep there.
	t.sourceCfgExist(s, &sourceCfg2)
	// source2 still bound to worker2.
	t.workerBound(s, ha.NewSourceBound(sourceID2, workerName2))
	t.sourceBounds(s, []string{sourceID1, sourceID2}, []string{})
	t.relayStageMatch(s, sourceID2, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.7: stop task2.
	require.NoError(t.T(), s.RemoveSubTasks(taskName2, sourceID1, sourceID2))
	t.subTaskCfgNotExist(s, taskName2, sourceID1)
	t.subTaskCfgNotExist(s, taskName2, sourceID2)
	t.subTaskStageMatch(s, taskName2, sourceID1, pb.Stage_InvalidStage)
	t.subTaskStageMatch(s, taskName2, sourceID2, pb.Stage_InvalidStage)
	t.validatorStageMatch(s, taskName2, sourceID2, pb.Stage_InvalidStage)
	rebuildScheduler(ctx)

	// CASE 4.7: remove source2.
	require.NoError(t.T(), s.StopRelay(sourceID2, []string{workerName2}))
	require.NoError(t.T(), s.RemoveSourceCfg(sourceID2))
	require.True(t.T(), terror.ErrSchedulerSourceCfgNotExist.Equal(s.RemoveSourceCfg(sourceID2))) // already removed.
	// source2 removed.
	t.sourceCfgNotExist(s, sourceID2)
	// worker2 become Free now.
	t.workerFree(s, workerName2)
	t.sourceBounds(s, []string{sourceID1}, []string{})
	t.relayStageMatch(s, sourceID2, pb.Stage_InvalidStage)
	rebuildScheduler(ctx)

	// CASE 4.7.1: add source2 with specify worker1
	// source2 not exist, worker1 is bound
	t.sourceCfgNotExist(s, sourceID2)
	t.workerBound(s, ha.NewSourceBound(sourceID1, workerName1))
	require.True(t.T(), terror.ErrSchedulerWorkerNotFree.Equal(s.AddSourceCfgWithWorker(&sourceCfg2, workerName1)))
	// source2 is not created because expected worker1 is already bound
	t.sourceCfgNotExist(s, sourceID2)
	rebuildScheduler(ctx)

	// CASE 4.7.2: add source2 with specify worker2
	// source2 not exist, worker2 should be free
	t.sourceCfgNotExist(s, sourceID2)
	t.workerFree(s, workerName2)
	require.NoError(t.T(), s.AddSourceCfgWithWorker(&sourceCfg2, workerName2))
	t.workerBound(s, ha.NewSourceBound(sourceID2, workerName2))
	t.sourceBounds(s, []string{sourceID1, sourceID2}, []string{})
	require.NoError(t.T(), s.StartRelay(sourceID2, []string{workerName2}))
	t.relayStageMatch(s, sourceID2, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.7.3: remove source2 again.
	require.NoError(t.T(), s.StopRelay(sourceID2, []string{workerName2}))
	require.NoError(t.T(), s.RemoveSourceCfg(sourceID2))
	require.True(t.T(), terror.ErrSchedulerSourceCfgNotExist.Equal(s.RemoveSourceCfg(sourceID2))) // already removed.
	// source2 removed.
	t.sourceCfgNotExist(s, sourceID2)
	// worker2 become Free now.
	t.workerFree(s, workerName2)
	t.sourceBounds(s, []string{sourceID1}, []string{})
	t.relayStageMatch(s, sourceID2, pb.Stage_InvalidStage)
	rebuildScheduler(ctx)

	// CASE 4.8: worker1 become offline.
	// before shutdown, worker1 bound source
	t.workerBound(s, ha.NewSourceBound(sourceID1, workerName1))
	// cancel keep-alive.
	cancel1()
	// wait for worker1 become offline.
	require.Eventually(t.T(), func() bool {
		w := s.GetWorkerByName(workerName1)
		require.NotNil(t.T(), w)
		return w.Stage() == WorkerOffline
	}, 3*time.Duration(keepAliveTTL)*time.Second, time.Second)
	t.workerOffline(s, workerName1)
	// source1 should bound to worker2.
	t.sourceBounds(s, []string{sourceID1}, []string{})
	t.workerBound(s, ha.NewSourceBound(sourceID1, workerName2))
	// expect stages keep Running.
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(s, taskName1, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.9: remove worker1.
	require.NoError(t.T(), s.RemoveWorker(workerName1))
	require.True(t.T(), terror.ErrSchedulerWorkerNotExist.Equal(s.RemoveWorker(workerName1))) // can't remove multiple times.
	// worker1 not exists now.
	t.workerNotExist(s, workerName1)
	rebuildScheduler(ctx)

	// CASE 4.10: stop task1.
	// wait for worker2 become bouned.
	require.Eventually(t.T(), func() bool {
		w := s.GetWorkerByName(workerName2)
		require.NotNil(t.T(), w)
		return w.Stage() == WorkerBound
	}, 3*time.Duration(keepAliveTTL)*time.Second, time.Second)
	require.NoError(t.T(), s.RemoveSubTasks(taskName1, sourceID1))
	t.subTaskCfgNotExist(s, taskName1, sourceID1)
	t.subTaskStageMatch(s, taskName1, sourceID1, pb.Stage_InvalidStage)
	rebuildScheduler(ctx)

	// CASE 4.11: remove worker not supported when the worker is online.
	// wait for worker2 become bouned.
	require.Eventually(t.T(), func() bool {
		w := s.GetWorkerByName(workerName2)
		require.NotNil(t.T(), w)
		return w.Stage() == WorkerBound
	}, 3*time.Duration(keepAliveTTL)*time.Second, time.Second)
	require.True(t.T(), terror.ErrSchedulerWorkerOnline.Equal(s.RemoveWorker(workerName2)))
	t.sourceBounds(s, []string{sourceID1}, []string{})
	t.workerBound(s, ha.NewSourceBound(sourceID1, workerName2))
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.12: worker2 become offline.
	cancel2()
	wg.Wait()
	// wait for worker2 become offline.
	require.Eventually(t.T(), func() bool {
		w := s.GetWorkerByName(workerName2)
		require.NotNil(t.T(), w)
		return w.Stage() == WorkerOffline
	}, 3*time.Duration(keepAliveTTL)*time.Second, time.Second)
	t.workerOffline(s, workerName2)
	// source1 should unbound
	t.sourceBounds(s, []string{}, []string{sourceID1})
	// expect stages keep Running.
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.13: remove worker2.
	require.NoError(t.T(), s.RemoveWorker(workerName2))
	t.workerNotExist(s, workerName2)
	// relay stage still there.
	t.sourceBounds(s, []string{}, []string{sourceID1})
	t.relayStageMatch(s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.14: remove source1.
	require.NoError(t.T(), s.RemoveSourceCfg(sourceID1))
	t.sourceCfgNotExist(s, sourceID1)
	t.sourceBounds(s, []string{}, []string{})
	t.relayStageMatch(s, sourceID1, pb.Stage_InvalidStage)
}

func (t *testSchedulerSuite) sourceCfgNotExist(s *Scheduler, source string) {
	t.T().Helper()
	require.Nil(t.T(), s.GetSourceCfgByID(source))
	scm, _, err := ha.GetSourceCfg(t.etcdTestCli, source, 0)
	require.NoError(t.T(), err)
	require.Len(t.T(), scm, 0)
}

func (t *testSchedulerSuite) sourceCfgExist(s *Scheduler, expectCfg *config.SourceConfig) {
	t.T().Helper()
	cfgP := s.GetSourceCfgByID(expectCfg.SourceID)
	require.Equal(t.T(), expectCfg, cfgP)
	scm, _, err := ha.GetSourceCfg(t.etcdTestCli, expectCfg.SourceID, 0)
	require.NoError(t.T(), err)
	cfgV := scm[expectCfg.SourceID]
	require.Equal(t.T(), expectCfg, cfgV)
}

func (t *testSchedulerSuite) subTaskCfgNotExist(s *Scheduler, task, source string) {
	t.T().Helper()
	require.Nil(t.T(), s.getSubTaskCfgByTaskSource(task, source))
	cfgM, _, err := ha.GetSubTaskCfg(t.etcdTestCli, source, task, 0)
	require.NoError(t.T(), err)
	require.Len(t.T(), cfgM, 0)
}

func (t *testSchedulerSuite) subTaskCfgExist(s *Scheduler, expectCfg config.SubTaskConfig) {
	t.T().Helper()
	cfgP := s.getSubTaskCfgByTaskSource(expectCfg.Name, expectCfg.SourceID)
	require.Equal(t.T(), expectCfg, *cfgP)
	cfgM, _, err := ha.GetSubTaskCfg(t.etcdTestCli, expectCfg.SourceID, expectCfg.Name, 0)
	require.NoError(t.T(), err)
	require.Len(t.T(), cfgM, 1)
	require.Equal(t.T(), expectCfg, cfgM[expectCfg.Name])
}

func (t *testSchedulerSuite) downstreamMetaNotExist(s *Scheduler, task string) {
	t.T().Helper()
	dbConfig, metaConfig := s.GetDownstreamMetaByTask(task)
	require.Nil(t.T(), dbConfig)
	require.Equal(t.T(), "", metaConfig)
}

func (t *testSchedulerSuite) downstreamMetaExist(s *Scheduler, task string, expectDBCfg dbconfig.DBConfig, expectMetaConfig string) {
	t.T().Helper()
	dbConfig, metaConfig := s.GetDownstreamMetaByTask(task)
	require.NotNil(t.T(), dbConfig)
	require.Equal(t.T(), expectDBCfg, *dbConfig)
	require.Equal(t.T(), expectMetaConfig, metaConfig)
}

func (t *testSchedulerSuite) workerNotExist(s *Scheduler, worker string) {
	t.T().Helper()
	require.Nil(t.T(), s.GetWorkerByName(worker))
	wm, _, err := ha.GetAllWorkerInfo(t.etcdTestCli)
	require.NoError(t.T(), err)
	_, ok := wm[worker]
	require.False(t.T(), ok)
}

func (t *testSchedulerSuite) workerExist(s *Scheduler, info ha.WorkerInfo) {
	t.T().Helper()
	require.NotNil(t.T(), s.GetWorkerByName(info.Name))
	require.Equal(t.T(), info, s.GetWorkerByName(info.Name).BaseInfo())
	wm, _, err := ha.GetAllWorkerInfo(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Equal(t.T(), info, wm[info.Name])
}

func (t *testSchedulerSuite) workerOffline(s *Scheduler, worker string) {
	t.T().Helper()
	w := s.GetWorkerByName(worker)
	require.NotNil(t.T(), w)
	require.Equal(t.T(), nullBound, w.Bound())
	require.Equal(t.T(), WorkerOffline, w.Stage())
	wm, _, err := ha.GetAllWorkerInfo(t.etcdTestCli)
	require.NoError(t.T(), err)
	_, ok := wm[worker]
	require.True(t.T(), ok)
	sbm, _, err := ha.GetSourceBound(t.etcdTestCli, worker)
	require.NoError(t.T(), err)
	_, ok = sbm[worker]
	require.False(t.T(), ok)
}

func (t *testSchedulerSuite) workerFree(s *Scheduler, worker string) {
	t.T().Helper()
	w := s.GetWorkerByName(worker)
	require.NotNil(t.T(), w)
	require.Equal(t.T(), nullBound, w.Bound())
	require.Equal(t.T(), WorkerFree, w.Stage())
	wm, _, err := ha.GetAllWorkerInfo(t.etcdTestCli)
	require.NoError(t.T(), err)
	_, ok := wm[worker]
	require.True(t.T(), ok)
	sbm, _, err := ha.GetSourceBound(t.etcdTestCli, worker)
	require.NoError(t.T(), err)
	_, ok = sbm[worker]
	require.False(t.T(), ok)
}

func (t *testSchedulerSuite) workerBound(s *Scheduler, bound ha.SourceBound) {
	t.T().Helper()
	w := s.GetWorkerByName(bound.Worker)
	require.NotNil(t.T(), w)
	boundDeepEqualExcludeRev(t.T(), w.Bound(), bound)
	require.Equal(t.T(), WorkerBound, w.Stage())
	wm, _, err := ha.GetAllWorkerInfo(t.etcdTestCli)
	require.NoError(t.T(), err)
	_, ok := wm[bound.Worker]
	require.True(t.T(), ok)
	sbm, _, err := ha.GetSourceBound(t.etcdTestCli, bound.Worker)
	require.NoError(t.T(), err)
	boundDeepEqualExcludeRev(t.T(), sbm[bound.Worker], bound)
}

func (t *testSchedulerSuite) sourceBounds(s *Scheduler, expectBounds, expectUnbounds []string) {
	t.T().Helper()
	require.Equal(t.T(), expectBounds, s.BoundSources())
	require.Equal(t.T(), expectUnbounds, s.UnboundSources())

	wToB, _, err := ha.GetSourceBound(t.etcdTestCli, "")
	require.NoError(t.T(), err)
	require.Len(t.T(), wToB, len(expectBounds))

	sToB := make(map[string]ha.SourceBound, len(wToB))
	for _, b := range wToB {
		sToB[b.Source] = b
	}
	for _, source := range expectBounds {
		require.NotNil(t.T(), sToB[source])
		require.NotNil(t.T(), s.GetWorkerBySource(source))
		require.Equal(t.T(), WorkerBound, s.GetWorkerBySource(source).Stage())
		boundDeepEqualExcludeRev(t.T(), sToB[source], s.GetWorkerBySource(source).Bound())
	}

	for _, source := range expectUnbounds {
		require.Nil(t.T(), s.GetWorkerBySource(source))
	}
}

func boundDeepEqualExcludeRev(t *testing.T, bound, expectBound ha.SourceBound) {
	t.Helper()
	expectBound.Revision = bound.Revision
	require.Equal(t, expectBound, bound)
}

func stageDeepEqualExcludeRev(t *testing.T, stage, expectStage ha.Stage) {
	t.Helper()
	expectStage.Revision = stage.Revision
	require.Equal(t, expectStage, stage)
}

func (t *testSchedulerSuite) relayStageMatch(s *Scheduler, source string, expectStage pb.Stage) {
	t.T().Helper()
	stage := ha.NewRelayStage(expectStage, source)
	stageDeepEqualExcludeRev(t.T(), s.GetExpectRelayStage(source), stage)

	eStage, _, err := ha.GetRelayStage(t.etcdTestCli, source)
	require.NoError(t.T(), err)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		stageDeepEqualExcludeRev(t.T(), eStage, stage)
	default:
		require.Equal(t.T(), stageEmpty, eStage)
	}
}

func (t *testSchedulerSuite) subTaskStageMatch(s *Scheduler, task, source string, expectStage pb.Stage) {
	t.T().Helper()
	stage := ha.NewSubTaskStage(expectStage, source, task)
	stageDeepEqualExcludeRev(t.T(), s.GetExpectSubTaskStage(task, source), stage)

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

func (t *testSchedulerSuite) validatorStageMatch(s *Scheduler, task, source string, expectStage pb.Stage) {
	t.T().Helper()
	stage := ha.NewValidatorStage(expectStage, source, task)
	var m map[string]ha.Stage
	if v, ok := s.expectValidatorStages.Load(task); ok {
		m = v.(map[string]ha.Stage)
	}
	if expectStage == pb.Stage_InvalidStage {
		_, ok := m[source]
		require.False(t.T(), ok)
	} else {
		stageDeepEqualExcludeRev(t.T(), m[source], stage)
	}
	stageM, _, err := ha.GetValidatorStage(t.etcdTestCli, source, task, 0)
	require.NoError(t.T(), err)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Stopped:
		require.Len(t.T(), stageM, 1)
		stageDeepEqualExcludeRev(t.T(), stageM[task], stage)
	default:
		require.Len(t.T(), stageM, 0)
	}
}

func (t *testSchedulerSuite) validatorModeMatch(s *Scheduler, task, source string, expectMode string) {
	t.T().Helper()
	cfg := s.getSubTaskCfgByTaskSource(task, source)
	require.NotNil(t.T(), cfg)
	require.Equal(t.T(), expectMode, cfg.ValidatorCfg.Mode)
}

func (t *testSchedulerSuite) TestRestartScheduler() {
	var (
		logger       = log.L()
		sourceID1    = "mysql-replica-1"
		workerName1  = "dm-worker-1"
		workerAddr1  = "127.0.0.1:8262"
		workerInfo1  = ha.NewWorkerInfo(workerName1, workerAddr1)
		sourceBound1 = ha.NewSourceBound(sourceID1, workerName1)
		wg           sync.WaitGroup
		keepAliveTTL = int64(2) // NOTE: this should be >= minLeaseTTL, in second.
	)
	sourceCfg1, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t.T(), err)
	sourceCfg1.SourceID = sourceID1

	s := NewScheduler(&logger, security.Security{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// step 1: start scheduler
	require.NoError(t.T(), s.Start(ctx, t.etcdTestCli))
	// step 1.1: add sourceCfg and worker
	require.NoError(t.T(), s.AddSourceCfg(sourceCfg1))
	t.sourceCfgExist(s, sourceCfg1)
	require.NoError(t.T(), s.AddWorker(workerName1, workerAddr1))
	t.workerExist(s, workerInfo1)
	// step 2: start a worker
	// step 2.1: worker start watching source bound
	bsm, revBound, err := ha.GetSourceBound(t.etcdTestCli, workerName1)
	require.NoError(t.T(), err)
	require.Len(t.T(), bsm, 0)
	sourceBoundCh := make(chan ha.SourceBound, 10)
	sourceBoundErrCh := make(chan error, 10)
	go func() {
		ha.WatchSourceBound(ctx, t.etcdTestCli, workerName1, revBound+1, sourceBoundCh, sourceBoundErrCh)
	}()
	// step 2.2: worker start keepAlive
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx1, t.etcdTestCli, workerName1, keepAliveTTL))
	}()
	// step 2.3: scheduler should bound source to worker
	// wait for source1 bound to worker1.
	require.Eventually(t.T(), func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	}, 3*time.Second, 100*time.Millisecond)
	checkSourceBoundCh := func() {
		require.Eventually(t.T(), func() bool {
			return len(sourceBoundCh) == 1
		}, 5*time.Second, 500*time.Millisecond)
		sourceBound := <-sourceBoundCh
		sourceBound.Revision = 0
		require.Equal(t.T(), sourceBound1, sourceBound)
		require.Len(t.T(), sourceBoundErrCh, 0)
	}
	// worker should receive a put sourceBound event
	checkSourceBoundCh()
	// case 1: scheduler restarted, and worker keepalive brock but re-setup before scheduler is started
	// step 3: restart scheduler, but don't stop worker keepalive, which can simulate two situations:
	//			a. worker keepalive breaks but re-setup again before scheduler is started
	//			b. worker is restarted but re-setup keepalive before scheduler is started
	// dm-worker will close its source when keepalive is broken, so scheduler will send an event
	// to trigger it to restart the source again
	s.Close()
	require.Len(t.T(), sourceBoundCh, 0)
	require.NoError(t.T(), s.Start(ctx, t.etcdTestCli))
	// worker should receive the trigger event again
	checkSourceBoundCh()
	// case 2: worker is restarted, and worker keepalive broke but scheduler didn't catch the delete event
	// step 4: restart worker keepalive, which can simulator one situation:
	//			a. worker keepalive breaks but re-setup again before keepaliveTTL is timeout
	require.Len(t.T(), sourceBoundCh, 0)
	ctx2, cancel2 := context.WithCancel(ctx)
	// trigger same keepalive event again, just for test
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx2, t.etcdTestCli, workerName1, keepAliveTTL))
	}()
	checkSourceBoundCh()
	// case 3: scheduler is restarted, but worker also broke after scheduler is down
	// step 5: stop scheduler -> stop worker keepalive -> restart scheduler
	//		   scheduler should unbound the source and update the bound info in etcd
	s.Close() // stop scheduler
	cancel1()
	cancel2() // stop worker keepalive
	wg.Wait()
	// check whether keepalive lease is out of date
	time.Sleep(time.Duration(keepAliveTTL) * time.Second)
	require.Eventually(t.T(), func() bool {
		kam, _, err := ha.GetKeepAliveWorkers(t.etcdTestCli)
		return err == nil && len(kam) == 0
	}, 3*time.Second, 100*time.Millisecond)
	require.Len(t.T(), sourceBoundCh, 0)
	require.Len(t.T(), sourceBoundCh, 0)
	require.NoError(t.T(), s.Start(ctx, t.etcdTestCli)) // restart scheduler
	require.Len(t.T(), s.BoundSources(), 0)
	unbounds := s.UnboundSources()
	require.Len(t.T(), unbounds, 1)
	require.Equal(t.T(), sourceID1, unbounds[0])
	sourceBound1.Source = ""
	sourceBound1.IsDeleted = true
	checkSourceBoundCh()

	// case 4: scheduler is restarted, but worker also broke after scheduler is down, then start another worker
	// step 6: add another worker -> stop scheduler -> stop worker keepalive -> restart scheduler
	//		   scheduler should unbound the source and rebound it to the newly alive worker

	// first let the source bound again
	ctx4, cancel4 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx4, t.etcdTestCli, workerName1, keepAliveTTL))
	}()
	sourceBound1.Source = sourceID1
	sourceBound1.IsDeleted = false
	checkSourceBoundCh()

	var (
		workerName2 = "dm-worker-2"
		workerAddr2 = "127.0.0.1:8263"
		workerInfo2 = ha.NewWorkerInfo(workerName2, workerAddr2)
	)

	// add another worker
	require.NoError(t.T(), s.AddWorker(workerName2, workerAddr2))
	t.workerExist(s, workerInfo2)

	// step 2.2: worker start keepAlive
	go func() {
		require.NoError(t.T(), ha.KeepAlive(ctx, t.etcdTestCli, workerName2, keepAliveTTL))
	}()

	s.Close() // stop scheduler
	cancel4() // stop worker keepalive
	wg.Wait()
	// check whether keepalive lease is out of date
	time.Sleep(time.Duration(keepAliveTTL) * time.Second)
	require.Eventually(t.T(), func() bool {
		kam, _, err := ha.GetKeepAliveWorkers(t.etcdTestCli)
		return err == nil && len(kam) == 1
	}, 3*time.Second, 100*time.Millisecond)
	require.Len(t.T(), sourceBoundCh, 0)
	require.NoError(t.T(), s.Start(ctx, t.etcdTestCli)) // restart scheduler
	require.Len(t.T(), s.BoundSources(), 1)
	w := s.workers[workerName2]
	require.Equal(t.T(), WorkerBound, w.stage)
	require.Equal(t.T(), sourceID1, w.bound.Source)
	unbounds = s.UnboundSources()
	require.Len(t.T(), unbounds, 0)
}

func (t *testSchedulerSuite) TestWatchWorkerEventEtcdCompact() {
	var (
		logger       = log.L()
		s            = NewScheduler(&logger, security.Security{})
		sourceID1    = "mysql-replica-1"
		sourceID2    = "mysql-replica-2"
		workerName1  = "dm-worker-1"
		workerName2  = "dm-worker-2"
		workerName3  = "dm-worker-3"
		workerName4  = "dm-worker-4"
		workerAddr1  = "127.0.0.1:8262"
		workerAddr2  = "127.0.0.1:18262"
		workerAddr3  = "127.0.0.1:18362"
		workerAddr4  = "127.0.0.1:18462"
		keepAliveTTL = int64(2) // NOTE: this should be >= minLeaseTTL, in second.
	)
	sourceCfg1, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t.T(), err)
	sourceCfg1.SourceID = sourceID1
	sourceCfg2 := *sourceCfg1
	sourceCfg2.SourceID = sourceID2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// step 1: start an empty scheduler without listening the worker event
	s.started.Store(true)
	s.cancel = cancel
	s.etcdCli = t.etcdTestCli

	// step 2: add two sources and register four workers
	require.NoError(t.T(), s.AddSourceCfg(sourceCfg1))
	require.NoError(t.T(), s.AddSourceCfg(&sourceCfg2))
	require.Len(t.T(), s.unbounds, 2)
	require.Contains(t.T(), s.unbounds, sourceID1)
	require.Contains(t.T(), s.unbounds, sourceID2)

	require.NoError(t.T(), s.AddWorker(workerName1, workerAddr1))
	require.NoError(t.T(), s.AddWorker(workerName2, workerAddr2))
	require.NoError(t.T(), s.AddWorker(workerName3, workerAddr3))
	require.NoError(t.T(), s.AddWorker(workerName4, workerAddr4))
	require.Len(t.T(), s.workers, 4)
	require.Contains(t.T(), s.workers, workerName1)
	require.Contains(t.T(), s.workers, workerName2)
	require.Contains(t.T(), s.workers, workerName3)
	require.Contains(t.T(), s.workers, workerName4)

	// step 3: add two workers, and then cancel them to simulate they have lost connection
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx1, t.etcdTestCli, workerName1, keepAliveTTL))
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx1, t.etcdTestCli, workerName2, keepAliveTTL))
	}()
	require.Eventually(t.T(), func() bool {
		kam, _, e := ha.GetKeepAliveWorkers(t.etcdTestCli)
		return e == nil && len(kam) == 2
	}, 3*time.Second, 100*time.Millisecond)
	cancel1()
	wg.Wait()
	// check whether keepalive lease is out of date
	time.Sleep(time.Duration(keepAliveTTL) * time.Second)
	var rev int64
	require.Eventually(t.T(), func() bool {
		kam, rev1, e := ha.GetKeepAliveWorkers(t.etcdTestCli)
		rev = rev1
		return e == nil && len(kam) == 0
	}, 3*time.Second, 100*time.Millisecond)

	// step 4: trigger etcd compaction and check whether we can receive it through watcher
	var startRev int64 = 1
	_, err = t.etcdTestCli.Compact(ctx, rev)
	require.NoError(t.T(), err)
	workerEvCh := make(chan ha.WorkerEvent, 10)
	workerErrCh := make(chan error, 10)
	ha.WatchWorkerEvent(ctx, t.etcdTestCli, startRev, workerEvCh, workerErrCh)
	select {
	case err := <-workerErrCh:
		require.Equal(t.T(), etcdErrCompacted, errors.Cause(err))
	case <-time.After(time.Second):
		t.T().Fatal("fail to get etcd error compacted")
	}

	// step 5: scheduler start to handle workerEvent from compact revision, should handle worker keepalive events correctly
	ctx2, cancel2 := context.WithCancel(ctx)
	// step 5.1: start one worker before scheduler start to handle workerEvent
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx2, t.etcdTestCli, workerName3, keepAliveTTL))
	}()
	require.Eventually(t.T(), func() bool {
		kam, _, err := ha.GetKeepAliveWorkers(t.etcdTestCli)
		if err == nil {
			if _, ok := kam[workerName3]; ok {
				return len(kam) == 1
			}
		}
		return false
	}, 3*time.Second, 100*time.Millisecond)
	// step 5.2: scheduler start to handle workerEvent
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), s.observeWorkerEvent(ctx2, startRev))
	}()
	// step 5.3: wait for scheduler to restart handleWorkerEvent, then start a new worker
	time.Sleep(time.Second)
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx2, t.etcdTestCli, workerName4, keepAliveTTL))
	}()
	require.Eventually(t.T(), func() bool {
		unbounds := s.UnboundSources()
		return len(unbounds) == 0
	}, 3*time.Second, 100*time.Millisecond)
	require.Equal(t.T(), []string{sourceID1, sourceID2}, s.BoundSources())
	cancel2()
	wg.Wait()

	// step 6: restart to observe workerEvents, should unbound all sources
	ctx3, cancel3 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), s.observeWorkerEvent(ctx3, startRev))
	}()
	require.Eventually(t.T(), func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 0
	}, 3*time.Second, 100*time.Millisecond)
	require.Equal(t.T(), []string{sourceID1, sourceID2}, s.UnboundSources())
	cancel3()
	wg.Wait()
}

func (t *testSchedulerSuite) TestLastBound() {
	var (
		logger      = log.L()
		s           = NewScheduler(&logger, security.Security{})
		sourceID1   = "mysql-replica-1"
		sourceID2   = "mysql-replica-2"
		workerName1 = "dm-worker-1"
		workerName2 = "dm-worker-2"
		workerName3 = "dm-worker-3"
		workerName4 = "dm-worker-4"
	)

	sourceCfg1, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t.T(), err)
	sourceCfg1.SourceID = sourceID1
	sourceCfg2 := sourceCfg1
	sourceCfg2.SourceID = sourceID2
	worker1 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName1}}
	worker2 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName2}}
	worker3 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName3}}
	worker4 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName4}}

	// step 1: start an empty scheduler without listening the worker event
	s.started.Store(true)
	s.etcdCli = t.etcdTestCli
	s.workers[workerName1] = worker1
	s.workers[workerName2] = worker2
	s.workers[workerName3] = worker3
	s.workers[workerName4] = worker4
	s.sourceCfgs[sourceID1] = sourceCfg1
	s.sourceCfgs[sourceID2] = sourceCfg2

	s.lastBound[workerName1] = ha.SourceBound{Source: sourceID1}
	s.lastBound[workerName2] = ha.SourceBound{Source: sourceID2}
	s.unbounds[sourceID1] = struct{}{}
	s.unbounds[sourceID2] = struct{}{}

	// worker1 goes to last bound source
	worker1.ToFree()
	bound, err := s.tryBoundForWorker(worker1)
	require.NoError(t.T(), err)
	require.True(t.T(), bound)
	require.Equal(t.T(), worker1, s.bounds[sourceID1])

	// worker3 has to bind source2
	worker3.ToFree()
	bound, err = s.tryBoundForWorker(worker3)
	require.NoError(t.T(), err)
	require.True(t.T(), bound)
	require.Equal(t.T(), worker3, s.bounds[sourceID2])

	// though worker2 has a previous source, that source is not available, so not bound
	worker2.ToFree()
	bound, err = s.tryBoundForWorker(worker2)
	require.NoError(t.T(), err)
	require.False(t.T(), bound)

	// worker4 is used to test whether source2 should be bound to worker2 rather than a new worker
	worker4.ToFree()
	bound, err = s.tryBoundForWorker(worker4)
	require.NoError(t.T(), err)
	require.False(t.T(), bound)

	// after worker3 become offline, source2 should be bound to worker2
	s.updateStatusToUnbound(sourceID2)
	_, ok := s.bounds[sourceID2]
	require.False(t.T(), ok)
	worker3.ToOffline()
	bound, err = s.tryBoundForSource(sourceID2)
	require.NoError(t.T(), err)
	require.True(t.T(), bound)
	require.Equal(t.T(), worker2, s.bounds[sourceID2])
}

func (t *testSchedulerSuite) TestInvalidLastBound() {
	var (
		logger      = log.L()
		s           = NewScheduler(&logger, security.Security{})
		sourceID1   = "mysql-replica-1"
		sourceID2   = "invalid-replica-1"
		workerName1 = "dm-worker-1"
	)

	sourceCfg1, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t.T(), err)
	sourceCfg1.SourceID = sourceID1
	sourceCfg2 := sourceCfg1
	sourceCfg2.SourceID = sourceID2
	worker1 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName1}}

	// step 1: start an empty scheduler without listening the worker event
	s.started.Store(true)
	s.etcdCli = t.etcdTestCli
	s.workers[workerName1] = worker1
	// sourceID2 doesn't have a source config and not in unbound
	s.sourceCfgs[sourceID1] = sourceCfg1
	s.lastBound[workerName1] = ha.SourceBound{Source: sourceID2}
	s.unbounds[sourceID1] = struct{}{}
	// step2: worker1 doesn't go to last bound source, because last source doesn't have a source config (might be removed)
	worker1.ToFree()
	bound, err := s.tryBoundForWorker(worker1)
	require.NoError(t.T(), err)
	require.True(t.T(), bound)
	require.Equal(t.T(), worker1, s.bounds[sourceID1])
}

func (t *testSchedulerSuite) TestTransferSource() {
	var (
		logger      = log.L()
		s           = NewScheduler(&logger, security.Security{})
		sourceID1   = "mysql-replica-1"
		sourceID2   = "mysql-replica-2"
		sourceID3   = "mysql-replica-3"
		sourceID4   = "mysql-replica-4"
		workerName1 = "dm-worker-1"
		workerName2 = "dm-worker-2"
		workerName3 = "dm-worker-3"
		workerName4 = "dm-worker-4"
	)

	worker1 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName1}}
	worker2 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName2}}
	worker3 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName3}}
	worker4 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName4}}

	// step 1: start an empty scheduler
	s.started.Store(true)
	s.etcdCli = t.etcdTestCli
	s.workers[workerName1] = worker1
	s.workers[workerName2] = worker2
	s.workers[workerName3] = worker3
	s.workers[workerName4] = worker4
	s.sourceCfgs[sourceID1] = &config.SourceConfig{}
	s.sourceCfgs[sourceID2] = &config.SourceConfig{}

	worker1.ToFree()
	require.NoError(t.T(), s.boundSourceToWorker(sourceID1, worker1))
	worker2.ToFree()
	require.NoError(t.T(), s.boundSourceToWorker(sourceID2, worker2))

	require.Equal(t.T(), worker1, s.bounds[sourceID1])
	require.Equal(t.T(), worker2, s.bounds[sourceID2])

	worker3.ToFree()
	worker4.ToFree()

	ctx := context.Background()
	// test invalid transfer: source not exists
	require.Error(t.T(), s.TransferSource(ctx, "not-exist", workerName3))

	// test valid transfer: source -> worker = bound -> free
	require.NoError(t.T(), s.TransferSource(ctx, sourceID1, workerName4))
	require.Equal(t.T(), worker4, s.bounds[sourceID1])
	require.Equal(t.T(), WorkerFree, worker1.Stage())

	// test valid transfer: source -> worker = unbound -> free
	s.sourceCfgs[sourceID3] = &config.SourceConfig{}
	s.unbounds[sourceID3] = struct{}{}
	require.NoError(t.T(), s.TransferSource(ctx, sourceID3, workerName3))
	require.Equal(t.T(), worker3, s.bounds[sourceID3])

	// test valid transfer: self
	require.NoError(t.T(), s.TransferSource(ctx, sourceID3, workerName3))
	require.Equal(t.T(), worker3, s.bounds[sourceID3])

	// test invalid transfer: source -> worker = bound -> bound
	require.Error(t.T(), s.TransferSource(ctx, sourceID1, workerName3))
	require.Equal(t.T(), worker4, s.bounds[sourceID1])
	require.Equal(t.T(), worker3, s.bounds[sourceID3])

	// test invalid transfer: source -> worker = bound -> offline
	worker1.ToOffline()
	require.Error(t.T(), s.TransferSource(ctx, sourceID1, workerName1))
	require.Equal(t.T(), worker4, s.bounds[sourceID1])

	// test invalid transfer: source -> worker = unbound -> bound
	s.sourceCfgs[sourceID4] = &config.SourceConfig{}
	s.unbounds[sourceID4] = struct{}{}
	require.Error(t.T(), s.TransferSource(ctx, sourceID4, workerName3))
	require.Equal(t.T(), worker3, s.bounds[sourceID3])
	delete(s.unbounds, sourceID4)
	delete(s.sourceCfgs, sourceID4)

	worker1.ToFree()
	// now we have (worker1, nil) (worker2, source2) (worker3, source3) (worker4, source1)

	// test fail halfway won't left old worker unbound
	require.NoError(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/failToReplaceSourceBound", `return()`))
	require.Error(t.T(), s.TransferSource(ctx, sourceID1, workerName1))
	require.Equal(t.T(), worker4, s.bounds[sourceID1])
	require.Equal(t.T(), WorkerFree, worker1.Stage())
	require.NoError(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/failToReplaceSourceBound"))

	// set running tasks
	s.expectSubTaskStages.Store("test", map[string]ha.Stage{sourceID1: {Expect: pb.Stage_Running}})

	// test can't transfer when running tasks not in sync unit
	require.NoError(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/operateWorkerQueryStatus", `return("notInSyncUnit")`))
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/operateWorkerQueryStatus") //nolint:errcheck
	require.True(t.T(), terror.ErrSchedulerRequireRunningTaskInSyncUnit.Equal(s.TransferSource(ctx, sourceID1, workerName1)))
	require.NoError(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/operateWorkerQueryStatus"))

	// test can't transfer when query status met error
	require.NoError(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/operateWorkerQueryStatus", `return("error")`))
	require.Contains(t.T(), s.TransferSource(ctx, sourceID1, workerName1).Error(), "failed to query worker")
	require.NoError(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/operateWorkerQueryStatus"))

	// test can transfer when all running task is in sync unit
	require.NoError(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/skipBatchOperateTaskOnWorkerSleep", `return()`))
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/skipBatchOperateTaskOnWorkerSleep") //nolint:errcheck
	require.NoError(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/operateWorkerQueryStatus", `return("allTaskIsPaused")`))

	// we only retry 10 times, open a failpoint to make need retry more than 10 times, so this transfer will fail
	require.NoError(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/batchOperateTaskOnWorkerMustRetry", `return(11)`))
	require.True(t.T(), terror.ErrSchedulerPauseTaskForTransferSource.Equal(s.TransferSource(ctx, sourceID1, workerName1)))
	require.NoError(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/batchOperateTaskOnWorkerMustRetry"))

	// now we can transfer successfully after 2 times retry
	s.expectSubTaskStages.Store("test", map[string]ha.Stage{sourceID1: {Expect: pb.Stage_Running}})
	require.NoError(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/batchOperateTaskOnWorkerMustRetry", `return(2)`))
	require.NoError(t.T(), s.TransferSource(ctx, sourceID1, workerName1))
	require.NoError(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/batchOperateTaskOnWorkerMustRetry"))
	require.Equal(t.T(), worker1, s.bounds[sourceID1])
	require.Equal(t.T(), WorkerBound, worker1.Stage())
	require.NoError(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/operateWorkerQueryStatus"))
	require.NoError(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/skipBatchOperateTaskOnWorkerSleep"))
}

func (t *testSchedulerSuite) TestStartStopRelay() {
	var (
		logger      = log.L()
		s           = NewScheduler(&logger, security.Security{})
		sourceID1   = "mysql-replica-1"
		sourceID2   = "mysql-replica-2"
		sourceID3   = "mysql-replica-3"
		sourceID4   = "mysql-replica-4"
		workerName1 = "dm-worker-1"
		workerName2 = "dm-worker-2"
		workerName3 = "dm-worker-3"
		workerName4 = "dm-worker-4"
	)

	worker1 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName1}}
	worker2 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName2}}
	worker3 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName3}}
	worker4 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName4}}

	// step 1: start an empty scheduler
	s.started.Store(true)
	s.etcdCli = t.etcdTestCli
	s.workers[workerName1] = worker1
	s.workers[workerName2] = worker2
	s.workers[workerName3] = worker3
	s.workers[workerName4] = worker4
	s.sourceCfgs[sourceID1] = &config.SourceConfig{}
	s.sourceCfgs[sourceID2] = &config.SourceConfig{}

	worker1.ToFree()
	require.NoError(t.T(), s.boundSourceToWorker(sourceID1, worker1))
	worker2.ToFree()
	require.NoError(t.T(), s.boundSourceToWorker(sourceID2, worker2))

	require.Equal(t.T(), worker1, s.bounds[sourceID1])
	require.Equal(t.T(), worker2, s.bounds[sourceID2])

	worker3.ToFree()
	worker4.ToFree()

	// test not exist source
	require.True(t.T(), terror.ErrSchedulerSourceCfgNotExist.Equal(s.StartRelay(sourceID3, []string{workerName1})))
	require.True(t.T(), terror.ErrSchedulerSourceCfgNotExist.Equal(s.StopRelay(sourceID4, []string{workerName1})))
	noWorkerSources := []string{sourceID1, sourceID2, sourceID3, sourceID4}
	for _, source := range noWorkerSources {
		workers, err := s.GetRelayWorkers(source)
		require.NoError(t.T(), err)
		require.Len(t.T(), workers, 0)
	}

	// start-relay success on bound-same-source and free worker
	require.NoError(t.T(), s.StartRelay(sourceID1, []string{workerName1}))
	require.NoError(t.T(), s.StartRelay(sourceID1, []string{workerName1}))
	require.Len(t.T(), s.expectRelayStages, 1)
	require.Contains(t.T(), s.expectRelayStages, sourceID1)
	require.NoError(t.T(), s.StartRelay(sourceID1, []string{workerName3}))
	require.Len(t.T(), s.relayWorkers, 1)
	require.Len(t.T(), s.relayWorkers[sourceID1], 2)
	require.Contains(t.T(), s.relayWorkers[sourceID1], workerName1)
	require.Contains(t.T(), s.relayWorkers[sourceID1], workerName3)
	workers, err := s.GetRelayWorkers(sourceID1)
	require.NoError(t.T(), err)
	require.Equal(t.T(), []*Worker{worker1, worker3}, workers)

	// failed on bound-not-same-source worker and not exist worker
	require.True(t.T(), terror.ErrSchedulerRelayWorkersWrongBound.Equal(s.StartRelay(sourceID1, []string{workerName2})))
	require.True(t.T(), terror.ErrSchedulerWorkerNotExist.Equal(s.StartRelay(sourceID1, []string{"not-exist"})))

	// failed on one worker multiple relay source
	require.True(t.T(), terror.ErrSchedulerRelayWorkersBusy.Equal(s.StartRelay(sourceID2, []string{workerName3})))

	// start another relay worker
	require.NoError(t.T(), s.StartRelay(sourceID2, []string{workerName2}))
	require.Len(t.T(), s.expectRelayStages, 2)
	require.Contains(t.T(), s.expectRelayStages, sourceID2)
	require.Len(t.T(), s.relayWorkers[sourceID2], 1)
	require.Contains(t.T(), s.relayWorkers[sourceID2], workerName2)
	workers, err = s.GetRelayWorkers(sourceID2)
	require.NoError(t.T(), err)
	require.Equal(t.T(), []*Worker{worker2}, workers)

	// failed on not-same-source worker and not exist worker
	require.True(t.T(), terror.ErrSchedulerRelayWorkersWrongRelay.Equal(s.StopRelay(sourceID1, []string{workerName2})))
	require.True(t.T(), terror.ErrSchedulerWorkerNotExist.Equal(s.StopRelay(sourceID1, []string{"not-exist"})))

	// nothing changed
	workers, err = s.GetRelayWorkers(sourceID1)
	require.NoError(t.T(), err)
	require.Equal(t.T(), []*Worker{worker1, worker3}, workers)
	workers, err = s.GetRelayWorkers(sourceID2)
	require.NoError(t.T(), err)
	require.Equal(t.T(), []*Worker{worker2}, workers)

	// stop-relay success
	require.NoError(t.T(), s.StopRelay(sourceID1, []string{workerName1}))
	require.NoError(t.T(), s.StopRelay(sourceID1, []string{workerName1}))
	require.NoError(t.T(), s.StopRelay(sourceID1, []string{workerName3}))
	require.Len(t.T(), s.expectRelayStages, 1)
	require.Contains(t.T(), s.expectRelayStages, sourceID2)
	require.Len(t.T(), s.relayWorkers, 1)
	require.Contains(t.T(), s.relayWorkers, sourceID2)
	workers, err = s.GetRelayWorkers(sourceID1)
	require.NoError(t.T(), err)
	require.Len(t.T(), workers, 0)

	// can't bind source to a worker which has different relay
	// currently source1 -> worker1, source2 -> worker2
	require.Equal(t.T(), workerName1, s.bounds[sourceID1].baseInfo.Name)
	require.Equal(t.T(), workerName2, s.bounds[sourceID2].baseInfo.Name)

	s.updateStatusToUnbound(sourceID2)
	require.Equal(t.T(), WorkerRelay, worker2.Stage())
	require.NoError(t.T(), s.StopRelay(sourceID2, []string{workerName2}))
	require.Equal(t.T(), WorkerFree, worker2.Stage())

	require.NoError(t.T(), s.StartRelay(sourceID1, []string{workerName2}))
	require.Equal(t.T(), WorkerRelay, worker2.Stage())
	require.Equal(t.T(), sourceID1, worker2.RelaySourceID())

	worker3.ToOffline()
	worker4.ToOffline()

	bound, err := s.tryBoundForSource(sourceID2)
	require.NoError(t.T(), err)
	require.False(t.T(), bound)
}

func (t *testSchedulerSuite) TestRelayWithWithoutWorker() {
	var (
		logger      = log.L()
		s           = NewScheduler(&logger, security.Security{})
		sourceID1   = "mysql-replica-1"
		workerName1 = "dm-worker-1"
		workerName2 = "dm-worker-2"
	)

	worker1 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName1}}
	worker2 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName2}}

	// step 1: start an empty scheduler
	s.started.Store(true)
	s.etcdCli = t.etcdTestCli
	s.workers[workerName1] = worker1
	s.workers[workerName2] = worker2
	s.sourceCfgs[sourceID1] = &config.SourceConfig{}

	worker1.ToFree()
	require.NoError(t.T(), s.boundSourceToWorker(sourceID1, worker1))
	worker2.ToFree()

	// step 2: check when enable-relay = false, can start/stop relay without worker name
	require.NoError(t.T(), s.StartRelay(sourceID1, []string{}))
	require.True(t.T(), s.sourceCfgs[sourceID1].EnableRelay)

	require.NoError(t.T(), s.StartRelay(sourceID1, []string{}))
	require.True(t.T(), s.sourceCfgs[sourceID1].EnableRelay)

	require.NoError(t.T(), s.StopRelay(sourceID1, []string{}))
	require.False(t.T(), s.sourceCfgs[sourceID1].EnableRelay)

	require.NoError(t.T(), s.StopRelay(sourceID1, []string{}))
	require.False(t.T(), s.sourceCfgs[sourceID1].EnableRelay)

	// step 3: check when enable-relay = false, can start/stop relay with worker name
	require.NoError(t.T(), s.StartRelay(sourceID1, []string{workerName1, workerName2}))
	require.False(t.T(), s.sourceCfgs[sourceID1].EnableRelay)
	require.Equal(t.T(), WorkerBound, worker1.Stage())
	require.Equal(t.T(), WorkerRelay, worker2.Stage())

	require.NoError(t.T(), s.StopRelay(sourceID1, []string{workerName1}))
	require.Equal(t.T(), WorkerBound, worker1.Stage())
	require.Equal(t.T(), WorkerRelay, worker2.Stage())

	require.NoError(t.T(), s.StopRelay(sourceID1, []string{workerName2}))
	require.Equal(t.T(), WorkerBound, worker1.Stage())
	require.Equal(t.T(), WorkerFree, worker2.Stage())

	// step 4: check when enable-relay = true, can't start/stop relay with worker name
	require.NoError(t.T(), s.StartRelay(sourceID1, []string{}))

	err := s.StartRelay(sourceID1, []string{workerName1})
	require.True(t.T(), terror.ErrSchedulerStartRelayOnBound.Equal(err))
	err = s.StartRelay(sourceID1, []string{workerName2})
	require.True(t.T(), terror.ErrSchedulerStartRelayOnBound.Equal(err))

	err = s.StopRelay(sourceID1, []string{workerName1})
	require.True(t.T(), terror.ErrSchedulerStopRelayOnBound.Equal(err))
	err = s.StopRelay(sourceID1, []string{workerName2})
	require.True(t.T(), terror.ErrSchedulerStopRelayOnBound.Equal(err))

	require.NoError(t.T(), s.StopRelay(sourceID1, []string{}))

	// step5. check when started relay with workerName, can't turn on enable-relay
	require.NoError(t.T(), s.StartRelay(sourceID1, []string{workerName1}))

	err = s.StartRelay(sourceID1, []string{})
	require.True(t.T(), terror.ErrSchedulerStartRelayOnSpecified.Equal(err))
	err = s.StopRelay(sourceID1, []string{})
	require.True(t.T(), terror.ErrSchedulerStopRelayOnSpecified.Equal(err))
}

func checkAllWorkersClosed(t *testing.T, s *Scheduler, closed bool) {
	t.Helper()
	for _, worker := range s.workers {
		cli, ok := worker.cli.(*workerrpc.GRPCClient)
		require.True(t, ok)
		require.Equal(t, closed, cli.Closed())
	}
}

func (t *testSchedulerSuite) TestCloseAllWorkers() {
	var (
		logger = log.L()
		s      = NewScheduler(&logger, security.Security{})
		names  []string
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 1; i < 4; i++ {
		names = append(names, fmt.Sprintf("worker%d", i))
	}

	for i, name := range names {
		info := ha.NewWorkerInfo(name, fmt.Sprintf("127.0.0.1:%d", 50801+i))
		_, err := ha.PutWorkerInfo(t.etcdTestCli, info)
		require.NoError(t.T(), err)
	}

	require.NoError(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/failToRecoverWorkersBounds", "return"))
	// Test closed when fail to start
	require.Errorf(t.T(), s.Start(ctx, t.etcdTestCli), "failToRecoverWorkersBounds")
	require.Len(t.T(), s.workers, 3)
	checkAllWorkersClosed(t.T(), s, true)
	require.NoError(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/failToRecoverWorkersBounds"))

	s.workers = map[string]*Worker{}
	require.NoError(t.T(), s.Start(ctx, t.etcdTestCli))
	checkAllWorkersClosed(t.T(), s, false)
	s.Close()
	require.Len(t.T(), s.workers, 3)
	checkAllWorkersClosed(t.T(), s, true)
}

func (t *testSchedulerSuite) TestStartSourcesWithoutSourceConfigsInEtcd() {
	var (
		logger       = log.L()
		s            = NewScheduler(&logger, security.Security{})
		sourceID1    = "mysql-replica-1"
		sourceID2    = "mysql-replica-2"
		workerName1  = "dm-worker-1"
		workerName2  = "dm-worker-2"
		workerAddr1  = "127.0.0.1:28362"
		workerAddr2  = "127.0.0.1:28363"
		wg           sync.WaitGroup
		keepaliveTTL = int64(60)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.started.Store(true)
	s.etcdCli = t.etcdTestCli
	// found source configs before bound
	s.sourceCfgs[sourceID1] = &config.SourceConfig{}
	s.sourceCfgs[sourceID2] = &config.SourceConfig{}
	s.unbounds[sourceID1] = struct{}{}
	s.unbounds[sourceID2] = struct{}{}
	require.NoError(t.T(), s.AddWorker(workerName1, workerAddr1))
	require.NoError(t.T(), s.AddWorker(workerName2, workerAddr2))

	wg.Add(2)
	go func() {
		require.NoError(t.T(), ha.KeepAlive(ctx, t.etcdTestCli, workerName1, keepaliveTTL))
		wg.Done()
	}()
	go func() {
		require.NoError(t.T(), ha.KeepAlive(ctx, t.etcdTestCli, workerName2, keepaliveTTL))
		wg.Done()
	}()

	s.workers[workerName1].stage = WorkerFree
	s.workers[workerName2].stage = WorkerFree
	bound, err := s.tryBoundForSource(sourceID1)
	require.NoError(t.T(), err)
	require.True(t.T(), bound)
	bound, err = s.tryBoundForSource(sourceID2)
	require.NoError(t.T(), err)
	require.True(t.T(), bound)

	s.started.Store(false)
	sbm, _, err := ha.GetSourceBound(t.etcdTestCli, "")
	require.NoError(t.T(), err)
	require.Len(t.T(), sbm, 2)
	require.Eventually(t.T(), func() bool {
		kam, _, err2 := ha.GetKeepAliveWorkers(t.etcdTestCli)
		if err2 != nil {
			return false
		}
		return len(kam) == 2
	}, 3*time.Second, 100*time.Millisecond)
	// there isn't any source config in etcd
	require.NoError(t.T(), s.Start(ctx, t.etcdTestCli))
	require.Len(t.T(), s.bounds, 0)
	sbm, _, err = ha.GetSourceBound(t.etcdTestCli, "")
	require.NoError(t.T(), err)
	require.Len(t.T(), sbm, 0)
	cancel()
	wg.Wait()
}

func (t *testSchedulerSuite) TestTransferWorkerAndSource() {
	var (
		logger      = log.L()
		s           = NewScheduler(&logger, security.Security{})
		sourceID1   = "mysql-replica-1"
		sourceID2   = "mysql-replica-2"
		sourceID3   = "mysql-replica-3"
		sourceID4   = "mysql-replica-4"
		workerName1 = "dm-worker-1"
		workerName2 = "dm-worker-2"
		workerName3 = "dm-worker-3"
		workerName4 = "dm-worker-4"
	)

	worker1 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName1}}
	worker2 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName2}}
	worker3 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName3}}
	worker4 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName4}}

	// step 1: start an empty scheduler
	s.started.Store(true)
	s.etcdCli = t.etcdTestCli
	s.workers[workerName1] = worker1
	s.workers[workerName2] = worker2
	s.workers[workerName3] = worker3
	s.workers[workerName4] = worker4
	s.sourceCfgs[sourceID1] = &config.SourceConfig{}
	s.sourceCfgs[sourceID2] = &config.SourceConfig{}

	worker1.ToFree()
	worker2.ToFree()
	worker3.ToFree()
	worker4.ToFree()
	s.unbounds[sourceID1] = struct{}{}
	s.unbounds[sourceID2] = struct{}{}

	// test free worker and unbound source
	require.NoError(t.T(), s.transferWorkerAndSource(workerName1, "", "", sourceID1))
	require.NoError(t.T(), s.transferWorkerAndSource("", sourceID2, workerName2, ""))
	require.Equal(t.T(), worker1, s.bounds[sourceID1])
	require.Equal(t.T(), worker2, s.bounds[sourceID2])
	require.Equal(t.T(), 0, len(s.unbounds))

	// test transfer bound source to free worker
	require.NoError(t.T(), s.transferWorkerAndSource(workerName1, sourceID1, workerName4, ""))
	require.Equal(t.T(), worker4, s.bounds[sourceID1])
	require.Equal(t.T(), WorkerFree, worker1.Stage())
	require.Equal(t.T(), WorkerBound, worker4.Stage())

	require.NoError(t.T(), s.transferWorkerAndSource(workerName3, "", workerName2, sourceID2))
	require.Equal(t.T(), worker3, s.bounds[sourceID2])
	require.Equal(t.T(), WorkerFree, worker2.Stage())
	require.Equal(t.T(), WorkerBound, worker3.Stage())

	// test transfer bound worker to unbound source
	s.unbounds[sourceID3] = struct{}{}
	s.unbounds[sourceID4] = struct{}{}
	require.NoError(t.T(), s.transferWorkerAndSource("", sourceID3, workerName3, sourceID2))
	require.Equal(t.T(), worker3, s.bounds[sourceID3])
	// sourceID2 bound to last bound worker
	require.Equal(t.T(), worker2, s.bounds[sourceID2])

	require.NoError(t.T(), s.transferWorkerAndSource(workerName4, sourceID1, "", sourceID4))
	require.Equal(t.T(), worker4, s.bounds[sourceID4])
	// sourceID1 bound to last bound worker
	require.Equal(t.T(), worker1, s.bounds[sourceID1])

	require.Equal(t.T(), 0, len(s.unbounds))

	// test transfer two bound sources
	require.NoError(t.T(), s.transferWorkerAndSource(workerName1, sourceID1, workerName2, sourceID2))
	require.NoError(t.T(), s.transferWorkerAndSource(workerName4, sourceID4, workerName3, sourceID3))
	require.Equal(t.T(), worker2, s.bounds[sourceID1])
	require.Equal(t.T(), worker1, s.bounds[sourceID2])
	require.Equal(t.T(), worker4, s.bounds[sourceID3])
	require.Equal(t.T(), worker3, s.bounds[sourceID4])

	require.NoError(t.T(), worker1.StartRelay(sourceID2))
	err := s.transferWorkerAndSource(workerName1, sourceID2, workerName2, sourceID1)
	require.True(t.T(), terror.ErrSchedulerBoundDiffWithStartedRelay.Equal(err))
}

func (t *testSchedulerSuite) TestWatchLoadTask() {
	var (
		logger      = log.L()
		s           = NewScheduler(&logger, security.Security{})
		task1       = "task1"
		task2       = "task2"
		sourceID1   = "mysql-replica-1"
		sourceID2   = "mysql-replica-2"
		workerName1 = "dm-worker-1"
		workerName2 = "dm-worker-2"
		workerName3 = "dm-worker-3"
		workerName4 = "dm-worker-4"
	)

	// step 1: start an empty scheduler
	s.started.Store(true)
	s.etcdCli = t.etcdTestCli

	worker1 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName1}}
	worker2 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName2}}
	worker3 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName3}}
	worker4 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName4}}
	s.workers[workerName1] = worker1
	s.workers[workerName2] = worker2
	s.workers[workerName3] = worker3
	s.workers[workerName4] = worker4
	s.sourceCfgs[sourceID1] = &config.SourceConfig{}
	s.sourceCfgs[sourceID2] = &config.SourceConfig{}
	s.subTaskCfgs.Store(task1, map[string]config.SubTaskConfig{
		sourceID1: {},
	})
	s.subTaskCfgs.Store(task2, map[string]config.SubTaskConfig{
		sourceID1: {},
		sourceID2: {},
	})

	worker1.ToFree()
	require.NoError(t.T(), s.boundSourceToWorker(sourceID1, worker1))
	worker2.ToFree()
	require.NoError(t.T(), s.boundSourceToWorker(sourceID2, worker2))
	require.Equal(t.T(), worker1, s.bounds[sourceID1])
	require.Equal(t.T(), worker2, s.bounds[sourceID2])

	worker3.ToFree()
	worker4.ToOffline()

	// put task1, source1, worker3
	_, err := ha.PutLoadTask(t.etcdTestCli, task1, sourceID1, workerName3)
	require.NoError(t.T(), err)
	// put task2, source2, worker4
	_, err = ha.PutLoadTask(t.etcdTestCli, task2, sourceID2, workerName4)
	require.NoError(t.T(), err)

	// get all load tasks
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel1()
	loadTasks, startRev, err := ha.GetAllLoadTask(t.etcdTestCli)
	require.NoError(t.T(), err)
	s.loadTasks = loadTasks

	require.True(t.T(), s.hasLoadTaskByWorkerAndSource(workerName3, sourceID1))
	require.True(t.T(), s.hasLoadTaskByWorkerAndSource(workerName4, sourceID2))

	// observer load tasks
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), s.observeLoadTask(ctx1, startRev))
	}()

	// put task2, source1, worker1
	_, err = ha.PutLoadTask(t.etcdTestCli, task2, sourceID1, workerName1)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.hasLoadTaskByWorkerAndSource(workerName1, sourceID1)
	}, 3*time.Second, 100*time.Millisecond)

	// del task2, source1, worker1
	_, _, err = ha.DelLoadTask(t.etcdTestCli, task2, sourceID1)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return !s.hasLoadTaskByWorkerAndSource(workerName1, sourceID1)
	}, 3*time.Second, 100*time.Millisecond)

	// source1 transfer to worker3
	require.Eventually(t.T(), func() bool {
		w, ok := s.bounds[sourceID1]
		return ok && w.baseInfo.Name == workerName3
	}, 3*time.Second, 100*time.Millisecond)

	require.Equal(t.T(), worker3, s.bounds[sourceID1])
	require.Equal(t.T(), WorkerFree, worker1.stage)

	// worker4 online
	// source2 transfer to worker4
	require.NoError(t.T(), s.handleWorkerOnline(ha.WorkerEvent{WorkerName: workerName4}, true))
	require.Eventually(t.T(), func() bool {
		w, ok := s.bounds[sourceID2]
		return ok && w.baseInfo.Name == workerName4
	}, 3*time.Second, 100*time.Millisecond)
	require.Equal(t.T(), worker4, s.bounds[sourceID2])
	require.Equal(t.T(), WorkerFree, worker2.stage)

	// after stop-task, hasLoadTaskByWorkerAndSource is no longer valid
	require.True(t.T(), s.hasLoadTaskByWorkerAndSource(workerName4, sourceID2))
	s.subTaskCfgs.Delete(task2)
	require.False(t.T(), s.hasLoadTaskByWorkerAndSource(workerName4, sourceID2))

	cancel1()
	wg.Wait()
}

func (t *testSchedulerSuite) TestWorkerHasDiffRelayAndBound() {
	var (
		logger      = log.L()
		s           = NewScheduler(&logger, security.Security{})
		sourceID1   = "mysql-replica-1"
		sourceID2   = "mysql-replica-2"
		workerName1 = "dm-worker-1"
		keepAlive   = int64(3)
	)

	workerInfo := ha.WorkerInfo{
		Name: workerName1,
		Addr: "workerinfo.addr",
	}
	bound := ha.SourceBound{
		Source: sourceID1,
		Worker: workerName1,
	}

	sourceCfg, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t.T(), err)
	sourceCfg.Checker.BackoffMax = config.Duration{Duration: 5 * time.Second}

	// prepare etcd data
	s.etcdCli = t.etcdTestCli
	sourceCfg.SourceID = sourceID1
	_, err = ha.PutSourceCfg(t.etcdTestCli, sourceCfg)
	require.NoError(t.T(), err)
	sourceCfg.SourceID = sourceID2
	_, err = ha.PutSourceCfg(t.etcdTestCli, sourceCfg)
	require.NoError(t.T(), err)
	_, err = ha.PutRelayConfig(t.etcdTestCli, sourceID2, workerName1)
	require.NoError(t.T(), err)
	_, err = ha.PutWorkerInfo(t.etcdTestCli, workerInfo)
	require.NoError(t.T(), err)
	_, err = ha.PutSourceBound(t.etcdTestCli, bound)
	require.NoError(t.T(), err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	//nolint:errcheck
	go ha.KeepAlive(ctx, t.etcdTestCli, workerName1, keepAlive)

	// bootstrap
	require.NoError(t.T(), s.recoverSources())
	require.NoError(t.T(), s.recoverRelayConfigs())
	_, err = s.recoverWorkersBounds()
	require.NoError(t.T(), err)

	// check
	require.Len(t.T(), s.relayWorkers[sourceID2], 1)
	_, ok := s.relayWorkers[sourceID2][workerName1]
	require.True(t.T(), ok)
	worker := s.workers[workerName1]
	require.Equal(t.T(), WorkerRelay, worker.Stage())
	require.Equal(t.T(), sourceID2, worker.RelaySourceID())
	_, ok = s.unbounds[sourceID1]
	require.True(t.T(), ok)
}

func (t *testSchedulerSuite) TestUpgradeCauseConflictRelayType() {
	var (
		logger      = log.L()
		s           = NewScheduler(&logger, security.Security{})
		sourceID1   = "mysql-replica-1"
		workerName1 = "dm-worker-1"
		workerName2 = "dm-worker-2"
		keepAlive   = int64(3)
	)

	workerInfo1 := ha.WorkerInfo{
		Name: workerName1,
		Addr: "workerinfo.addr",
	}
	workerInfo2 := ha.WorkerInfo{
		Name: workerName2,
		Addr: "workerinfo.addr",
	}
	bound := ha.SourceBound{
		Source: sourceID1,
		Worker: workerName1,
	}

	sourceCfg, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t.T(), err)
	sourceCfg.Checker.BackoffMax = config.Duration{Duration: 5 * time.Second}

	// prepare etcd data
	s.etcdCli = t.etcdTestCli
	sourceCfg.EnableRelay = true
	sourceCfg.SourceID = sourceID1
	_, err = ha.PutSourceCfg(t.etcdTestCli, sourceCfg)
	require.NoError(t.T(), err)
	_, err = ha.PutRelayConfig(t.etcdTestCli, sourceID1, workerName1)
	require.NoError(t.T(), err)
	_, err = ha.PutRelayConfig(t.etcdTestCli, sourceID1, workerName2)
	require.NoError(t.T(), err)
	_, err = ha.PutWorkerInfo(t.etcdTestCli, workerInfo1)
	require.NoError(t.T(), err)
	_, err = ha.PutWorkerInfo(t.etcdTestCli, workerInfo2)
	require.NoError(t.T(), err)
	_, err = ha.PutSourceBound(t.etcdTestCli, bound)
	require.NoError(t.T(), err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	//nolint:errcheck
	go ha.KeepAlive(ctx, t.etcdTestCli, workerName1, keepAlive)
	//nolint:errcheck
	go ha.KeepAlive(ctx, t.etcdTestCli, workerName2, keepAlive)

	// bootstrap
	require.NoError(t.T(), s.recoverSources())
	require.NoError(t.T(), s.recoverRelayConfigs())
	_, err = s.recoverWorkersBounds()
	require.NoError(t.T(), err)

	// check when the relay config is conflicting with source config, relay config should be deleted
	require.Len(t.T(), s.relayWorkers[sourceID1], 0)
	result, _, err := ha.GetAllRelayConfig(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Len(t.T(), result, 0)

	worker := s.workers[workerName1]
	require.Equal(t.T(), WorkerBound, worker.Stage())
	require.Len(t.T(), worker.RelaySourceID(), 0)
	require.Equal(t.T(), WorkerFree, s.workers[workerName2].Stage())
}

func (t *testSchedulerSuite) TestOperateValidatorTask() {
	var (
		logger      = log.L()
		s           = NewScheduler(&logger, security.Security{})
		sourceID1   = "mysql-replica-1"
		workerName1 = "dm-worker-1"
		taskName    = "task-1"
		keepAlive   = int64(2)
		subtaskCfg  config.SubTaskConfig
	)
	require.NoError(t.T(), subtaskCfg.Decode(config.SampleSubtaskConfig, true))
	subtaskCfg.SourceID = sourceID1
	subtaskCfg.Name = taskName
	subtaskCfg.ValidatorCfg = config.ValidatorConfig{Mode: config.ValidationNone}
	require.NoError(t.T(), subtaskCfg.Adjust(true))

	workerInfo1 := ha.WorkerInfo{
		Name: workerName1,
		Addr: "workerinfo.addr",
	}
	bound := ha.SourceBound{
		Source: sourceID1,
		Worker: workerName1,
	}
	sourceCfg, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t.T(), err)
	s.etcdCli = t.etcdTestCli
	sourceCfg.SourceID = sourceID1
	_, err = ha.PutSourceCfg(t.etcdTestCli, sourceCfg)
	require.NoError(t.T(), err)
	_, err = ha.PutWorkerInfo(t.etcdTestCli, workerInfo1)
	require.NoError(t.T(), err)
	_, err = ha.PutSourceBound(t.etcdTestCli, bound)
	require.NoError(t.T(), err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	//nolint:errcheck
	go ha.KeepAlive(ctx, t.etcdTestCli, workerName1, keepAlive)
	require.NoError(t.T(), s.recoverSources())
	_, err = s.recoverWorkersBounds()
	require.NoError(t.T(), err)
	require.NoError(t.T(), s.Start(ctx, t.etcdTestCli))
	// CASE 1: start subtask without starting validation
	require.NoError(t.T(), s.AddSubTasks(false, pb.Stage_Running, subtaskCfg)) // create new subtask without validation
	t.subTaskCfgExist(s, subtaskCfg)
	subtaskCfg.ValidatorCfg.Mode = config.ValidationFull // set mode
	validatorStages := []ha.Stage{ha.NewValidatorStage(pb.Stage_Running, subtaskCfg.SourceID, subtaskCfg.Name)}
	changedCfgs := []config.SubTaskConfig{subtaskCfg}
	require.NoError(t.T(), s.OperateValidationTask(validatorStages, changedCfgs))        // create validator task
	t.validatorStageMatch(s, subtaskCfg.Name, subtaskCfg.SourceID, pb.Stage_Running)     // task running
	t.validatorModeMatch(s, subtaskCfg.Name, subtaskCfg.SourceID, config.ValidationFull) // succeed to change mode

	// CASE 2: stop running subtask
	validatorStages = []ha.Stage{ha.NewValidatorStage(pb.Stage_Stopped, subtaskCfg.SourceID, subtaskCfg.Name)}
	changedCfgs = []config.SubTaskConfig{}
	require.NoError(t.T(), s.OperateValidationTask(validatorStages, changedCfgs))
	t.validatorStageMatch(s, subtaskCfg.Name, subtaskCfg.SourceID, pb.Stage_Stopped) // task stopped
	require.NoError(t.T(), s.OperateValidationTask(validatorStages, changedCfgs))    // stop stopped validator task with no error
	t.validatorStageMatch(s, subtaskCfg.Name, subtaskCfg.SourceID, pb.Stage_Stopped) // stage not changed
}

func (t *testSchedulerSuite) TestUpdateSubTasksAndSourceCfg() {
	defer t.clearTestInfoOperation()

	var (
		logger       = log.L()
		s            = NewScheduler(&logger, security.Security{})
		sourceID1    = "mysql-replica-1"
		taskName1    = "task-1"
		workerName1  = "dm-worker-1"
		workerAddr1  = "127.0.0.1:8262"
		subtaskCfg1  config.SubTaskConfig
		keepAliveTTL = int64(5)
		ctx          = context.Background()
	)
	sourceCfg1, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t.T(), err)
	sourceCfg1.SourceID = sourceID1

	require.NoError(t.T(), subtaskCfg1.Decode(config.SampleSubtaskConfig, true))
	subtaskCfg1.SourceID = sourceID1
	subtaskCfg1.Name = taskName1
	require.NoError(t.T(), subtaskCfg1.Adjust(true))

	// not started scheduler can't update
	t.True(terror.ErrSchedulerNotStarted.Equal(s.UpdateSubTasks(ctx, subtaskCfg1)))
	t.True(terror.ErrSchedulerNotStarted.Equal(s.UpdateSourceCfg(sourceCfg1)))

	// start the scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.NoError(s.Start(ctx, t.etcdTestCli))

	// can't update source when source not added
	t.True(terror.ErrSchedulerSourceCfgNotExist.Equal(s.UpdateSourceCfg(sourceCfg1)))

	subtaskCfg2 := subtaskCfg1
	subtaskCfg2.Name = "fake name"
	// can't update subtask with different task name
	t.True(terror.ErrSchedulerMultiTask.Equal(s.UpdateSubTasks(ctx, subtaskCfg1, subtaskCfg2)))

	// can't update not added subtask
	t.True(terror.ErrSchedulerTaskNotExist.Equal(s.UpdateSubTasks(ctx, subtaskCfg1)))

	// start worker,add source and subtask
	t.NoError(s.AddSourceCfg(sourceCfg1))
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	t.NoError(s.AddWorker(workerName1, workerAddr1))
	go func() {
		t.NoError(ha.KeepAlive(ctx1, t.etcdTestCli, workerName1, keepAliveTTL))
	}()
	// wait for source1 bound to worker1.
	t.Eventually(func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	}, 100*30*time.Millisecond, 30*time.Millisecond)
	t.NoError(s.AddSubTasks(false, pb.Stage_Running, subtaskCfg1))

	// can't update subtask not in scheduler
	subtaskCfg2.Name = subtaskCfg1.Name
	subtaskCfg2.SourceID = "fake source name"
	t.True(terror.ErrSchedulerSubTaskNotExist.Equal(s.UpdateSubTasks(ctx, subtaskCfg2)))

	// can't update subtask in running stage
	t.True(terror.ErrSchedulerSubTaskCfgUpdate.Equal(s.UpdateSubTasks(ctx, subtaskCfg1)))
	// can't update source when there is running tasks
	t.True(terror.ErrSchedulerSourceCfgUpdate.Equal(s.UpdateSourceCfg(sourceCfg1)))

	// pause task
	t.NoError(s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName1, sourceID1))

	// can't update source when there is a relay worker for this source
	t.NoError(s.StartRelay(sourceID1, []string{workerName1}))
	t.True(terror.ErrSchedulerSourceCfgUpdate.Equal(s.UpdateSourceCfg(sourceCfg1)))
	t.NoError(s.StopRelay(sourceID1, []string{workerName1}))

	// can't updated when worker rpc error
	t.NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/operateCheckSubtasksCanUpdate", `return("error")`))
	t.Regexp("query error", s.UpdateSubTasks(ctx, subtaskCfg1))
	t.NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/operateCheckSubtasksCanUpdate"))

	// can't updated when worker rpc check not pass
	t.NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/operateCheckSubtasksCanUpdate", `return("failed")`))
	t.True(terror.ErrSchedulerSubTaskCfgUpdate.Equal(s.UpdateSubTasks(ctx, subtaskCfg1)))
	t.NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/operateCheckSubtasksCanUpdate"))

	// update success
	subtaskCfg1.Batch = 1000
	t.NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/master/scheduler/operateCheckSubtasksCanUpdate", `return("success")`))
	t.NoError(s.UpdateSubTasks(ctx, subtaskCfg1))
	t.NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/master/scheduler/operateCheckSubtasksCanUpdate"))
	t.Equal(s.getSubTaskCfgByTaskSource(taskName1, sourceID1).Batch, subtaskCfg1.Batch)

	sourceCfg1.MetaDir = "new meta"
	t.NoError(s.UpdateSourceCfg(sourceCfg1))
	t.Equal(s.GetSourceCfgByID(sourceID1).MetaDir, sourceCfg1.MetaDir)
}

func (t *testSchedulerSuite) TestValidatorEnabledAndGetValidatorStage() {
	logger := log.L()
	s := NewScheduler(&logger, security.Security{})
	task := "test"
	source := "source"
	m, _ := s.expectValidatorStages.LoadOrStore(task, map[string]ha.Stage{})
	m.(map[string]ha.Stage)[source] = ha.Stage{Expect: pb.Stage_Running}

	t.True(s.ValidatorEnabled(task, source))
	t.False(s.ValidatorEnabled(task, "not-exist"))
	t.False(s.ValidatorEnabled("not-exist", source))
	t.False(s.ValidatorEnabled("not-exist", "not-exist"))

	stage := s.GetValidatorStage(task, source)
	t.NotNil(stage)
	t.Equal(pb.Stage_Running, stage.Expect)
	t.Nil(s.GetValidatorStage(task, "not-exist"))
	t.Nil(s.GetValidatorStage("not-exist", source))
	t.Nil(s.GetValidatorStage("not-exist", "not-exist"))
}
