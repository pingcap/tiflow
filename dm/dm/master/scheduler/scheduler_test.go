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

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"go.etcd.io/etcd/clientv3"
	v3rpc "go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

const (
	// do not forget to update this path if the file removed/renamed.
	sourceSampleFile = "../../worker/source.yaml"
	// do not forget to update this path if the file removed/renamed.
	subTaskSampleFile = "../../worker/subtask.toml"
)

const (
	noRestart          = iota // do nothing in rebuildPessimist, just keep testing
	restartOnly               // restart without building new instance. mock leader role transfer
	restartNewInstance        // restart with build a new instance. mock progress restore from failure
)

var (
	etcdTestCli      *clientv3.Client
	etcdErrCompacted = v3rpc.ErrCompacted
	testRelayDir     = "./test_relay_dir"
)

func TestScheduler(t *testing.T) {
	err := log.InitLogger(&log.Config{})
	if err != nil {
		t.Fatal(err)
	}

	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	TestingT(t)
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(c *C) {
	c.Assert(ha.ClearTestInfoOperation(etcdTestCli), IsNil)
}

type testScheduler struct{}

var _ = Suite(&testScheduler{})

var stageEmpty ha.Stage

func (t *testScheduler) TestScheduler(c *C) {
	t.testSchedulerProgress(c, noRestart)
	t.testSchedulerProgress(c, restartOnly)
	t.testSchedulerProgress(c, restartNewInstance)
}

func (t *testScheduler) testSchedulerProgress(c *C, restart int) {
	defer clearTestInfoOperation(c)

	var (
		logger       = log.L()
		s            = NewScheduler(&logger, config.Security{})
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
				c.Assert(s.Start(ctx, etcdTestCli), IsNil)
			case restartNewInstance:
				s.Close()
				s = NewScheduler(&logger, config.Security{})
				c.Assert(s.Start(ctx, etcdTestCli), IsNil)
			}
		}
	)
	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, IsNil)
	sourceCfg1.SourceID = sourceID1
	sourceCfg2 := *sourceCfg1
	sourceCfg2.SourceID = sourceID2

	c.Assert(subtaskCfg1.DecodeFile(subTaskSampleFile, true), IsNil)
	subtaskCfg1.SourceID = sourceID1
	subtaskCfg1.Name = taskName1
	c.Assert(subtaskCfg1.Adjust(true), IsNil)
	subtaskCfg21 := subtaskCfg1
	subtaskCfg21.Name = taskName2
	c.Assert(subtaskCfg21.Adjust(true), IsNil)
	subtaskCfg22 := subtaskCfg21
	subtaskCfg22.SourceID = sourceID2
	c.Assert(subtaskCfg22.Adjust(true), IsNil)

	// not started scheduler can't do anything.
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.AddSourceCfg(sourceCfg1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.UpdateSourceCfg(sourceCfg1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.RemoveSourceCfg(sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.AddSubTasks(false, subtaskCfg1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.RemoveSubTasks(taskName1, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.AddWorker(workerName1, workerAddr1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.RemoveWorker(workerName1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.UpdateExpectRelayStage(pb.Stage_Running, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.UpdateExpectSubTaskStage(pb.Stage_Running, taskName1, sourceID1)), IsTrue)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous info.
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)
	c.Assert(terror.ErrSchedulerStarted.Equal(s.Start(ctx, etcdTestCli)), IsTrue) // start multiple times.
	s.Close()
	s.Close() // close multiple times.

	// CASE 2: start again without any previous info.
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)

	// CASE 2.1: add the first source config.
	// no source config exist before added.
	t.sourceCfgNotExist(c, s, sourceID1)
	// add source config1.
	c.Assert(s.AddSourceCfg(sourceCfg1), IsNil)
	c.Assert(terror.ErrSchedulerSourceCfgExist.Equal(s.AddSourceCfg(sourceCfg1)), IsTrue) // can't add multiple times.
	// the source config added.
	t.sourceCfgExist(c, s, sourceCfg1)

	// update source cfg
	sourceCfg1.RelayDir = testRelayDir
	c.Assert(s.UpdateSourceCfg(sourceCfg1), IsNil)
	newCfg := s.GetSourceCfgByID(sourceID1)
	c.Assert(newCfg.RelayDir, Equals, testRelayDir)

	// update with invalid SourceID
	fake := newCfg.Clone()
	fake.SourceID = "not a source id"
	c.Assert(terror.ErrSchedulerSourceCfgNotExist.Equal(s.UpdateSourceCfg(fake)), IsTrue)

	// update field not related to relay-log will failed
	fake2 := newCfg.Clone()
	fake2.AutoFixGTID = !fake2.AutoFixGTID
	c.Assert(terror.ErrSchedulerSourceCfgUpdate.Equal(s.UpdateSourceCfg(fake2)), IsTrue)

	// one unbound source exist (because no free worker).
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	rebuildScheduler(ctx)

	// CASE 2.2: add the first worker.
	// no worker exist before added.
	t.workerNotExist(c, s, workerName1)
	// add worker1.
	c.Assert(s.AddWorker(workerName1, workerAddr1), IsNil)
	c.Assert(terror.ErrSchedulerWorkerExist.Equal(s.AddWorker(workerName1, workerAddr2)), IsTrue) // can't add with different address now.
	c.Assert(s.AddWorker(workerName1, workerAddr1), IsNil)                                        // but can add the worker multiple times (like restart the worker).
	// the worker added.
	t.workerExist(c, s, workerInfo1)
	t.workerOffline(c, s, workerName1)
	// still no bounds (because the worker is offline).
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	// no expect relay stage exist (because the source has never been bounded).
	t.relayStageMatch(c, s, sourceID1, pb.Stage_InvalidStage)
	rebuildScheduler(ctx)

	// CASE 2.3: the worker become online.
	// do keep-alive for worker1.
	ctx1, cancel1 := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx1, etcdTestCli, workerName1, keepAliveTTL), IsNil)
	}()
	// wait for source1 bound to worker1.
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	}), IsTrue)
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName1))
	c.Assert(s.StartRelay(sourceID1, []string{workerName1}), IsNil)
	// expect relay stage become Running after the start relay.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 2.4: pause the relay.
	c.Assert(s.UpdateExpectRelayStage(pb.Stage_Paused, sourceID1), IsNil)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Paused)
	// update relay stage without source take no effect now (and return without error).
	c.Assert(s.UpdateExpectRelayStage(pb.Stage_Running), IsNil)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Paused)
	// update to non-(Running, Paused) stage is invalid.
	c.Assert(terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_InvalidStage, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_New, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_Stopped, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_Finished, sourceID1)), IsTrue)
	// can't update stage with not existing sources now.
	c.Assert(terror.ErrSchedulerRelayStageSourceNotExist.Equal(s.UpdateExpectRelayStage(pb.Stage_Running, sourceID1, sourceID2)), IsTrue)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Paused)
	rebuildScheduler(ctx)

	// CASE 2.5: resume the relay.
	c.Assert(s.UpdateExpectRelayStage(pb.Stage_Running, sourceID1), IsNil)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 2.6: start a task with only one source.
	// wait for source bound recovered
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	}), IsTrue)
	// no subtask config exists before start.
	c.Assert(s.AddSubTasks(false), IsNil) // can call without configs, return without error, but take no effect.
	t.subTaskCfgNotExist(c, s, taskName1, sourceID1)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_InvalidStage)
	t.downstreamMetaNotExist(c, s, taskName1)
	// start the task.
	c.Assert(s.AddSubTasks(false, subtaskCfg1), IsNil)
	c.Assert(terror.ErrSchedulerSubTaskExist.Equal(s.AddSubTasks(false, subtaskCfg1)), IsTrue) // add again.
	// subtask config and stage exist.
	t.subTaskCfgExist(c, s, subtaskCfg1)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)
	t.downstreamMetaExist(c, s, taskName1, subtaskCfg1.To, subtaskCfg1.MetaSchema)
	t.downstreamMetaNotExist(c, s, taskName2)

	// update source config when task already started will failed
	c.Assert(terror.ErrSchedulerSourceOpTaskExist.Equal(s.UpdateSourceCfg(sourceCfg1)), IsTrue)

	// try start a task with two sources, some sources not bound.
	c.Assert(terror.ErrSchedulerSourcesUnbound.Equal(s.AddSubTasks(false, subtaskCfg21, subtaskCfg22)), IsTrue)
	t.subTaskCfgNotExist(c, s, taskName2, sourceID1)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_InvalidStage)
	t.subTaskCfgNotExist(c, s, taskName2, sourceID2)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_InvalidStage)
	rebuildScheduler(ctx)

	// CASE 2.7: pause/resume task1.
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName1, sourceID1), IsNil)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Paused)
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Running, taskName1, sourceID1), IsNil)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)
	// update subtask stage without source or task take no effect now (and return without error).
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Paused, "", sourceID1), IsNil)
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName1), IsNil)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	// update to non-(Running, Paused) stage is invalid.
	c.Assert(terror.ErrSchedulerSubTaskStageInvalidUpdate.Equal(s.UpdateExpectSubTaskStage(pb.Stage_InvalidStage, taskName1, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerSubTaskStageInvalidUpdate.Equal(s.UpdateExpectSubTaskStage(pb.Stage_New, taskName1, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerSubTaskStageInvalidUpdate.Equal(s.UpdateExpectSubTaskStage(pb.Stage_Stopped, taskName1, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerSubTaskStageInvalidUpdate.Equal(s.UpdateExpectSubTaskStage(pb.Stage_Finished, taskName1, sourceID1)), IsTrue)
	// can't update stage with not existing sources now.
	c.Assert(terror.ErrSchedulerSubTaskOpSourceNotExist.Equal(s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName1, sourceID1, sourceID2)), IsTrue)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 2.8: worker1 become offline.
	// cancel keep-alive.
	cancel1()
	wg.Wait()
	// wait for source1 unbound from worker1.
	c.Assert(utils.WaitSomething(int(3*keepAliveTTL), time.Second, func() bool {
		unbounds := s.UnboundSources()
		return len(unbounds) == 1 && unbounds[0] == sourceID1
	}), IsTrue)
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	// static information are still there.
	t.sourceCfgExist(c, s, sourceCfg1)
	t.subTaskCfgExist(c, s, subtaskCfg1)
	t.workerExist(c, s, workerInfo1)
	// worker1 still exists, but it's offline.
	t.workerOffline(c, s, workerName1)
	// expect relay stage keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 3: start again with previous `Offline` worker, relay stage, subtask stage.
	// CASE 3.1: previous information should recover.
	// source1 is still unbound.
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	// worker1 still exists, but it's offline.
	t.workerOffline(c, s, workerName1)
	// static information are still there.
	t.sourceCfgExist(c, s, sourceCfg1)
	t.subTaskCfgExist(c, s, subtaskCfg1)
	t.workerExist(c, s, workerInfo1)
	// expect relay stage keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 3.2: start worker1 again.
	// do keep-alive for worker1 again.
	ctx1, cancel1 = context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx1, etcdTestCli, workerName1, keepAliveTTL), IsNil)
	}()
	// wait for source1 bound to worker1.
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	}), IsTrue)
	// source1 bound to worker1.
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName1))
	// expect stages keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.1: previous information should recover.
	// source1 is still bound.
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	// worker1 still exists, and it's bound.
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName1))
	// static information are still there.
	t.sourceCfgExist(c, s, sourceCfg1)
	t.subTaskCfgExist(c, s, subtaskCfg1)
	t.workerExist(c, s, workerInfo1)
	// expect stages keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.2: add another worker into the cluster.
	// worker2 not exists before added.
	t.workerNotExist(c, s, workerName2)
	// add worker2.
	c.Assert(s.AddWorker(workerName2, workerAddr2), IsNil)
	// the worker added, but is offline.
	t.workerExist(c, s, workerInfo2)
	t.workerOffline(c, s, workerName2)
	rebuildScheduler(ctx)

	// CASE 4.3: the worker2 become online.
	// do keep-alive for worker2.
	ctx2, cancel2 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx2, etcdTestCli, workerName2, keepAliveTTL), IsNil)
	}()
	// wait for worker2 become Free.
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		w := s.GetWorkerByName(workerName2)
		return w.Stage() == WorkerFree
	}), IsTrue)
	t.workerFree(c, s, workerName2)
	rebuildScheduler(ctx)

	// CASE 4.4: add source config2.
	// wait for source bound recovered
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	}), IsTrue)
	// source2 not exists before.
	t.sourceCfgNotExist(c, s, sourceID2)
	// add source2.
	c.Assert(s.AddSourceCfg(&sourceCfg2), IsNil)
	// source2 added.
	t.sourceCfgExist(c, s, &sourceCfg2)
	// source2 should bound to worker2.
	t.workerBound(c, s, ha.NewSourceBound(sourceID2, workerName2))
	t.sourceBounds(c, s, []string{sourceID1, sourceID2}, []string{})
	c.Assert(s.StartRelay(sourceID2, []string{workerName2}), IsNil)
	t.relayStageMatch(c, s, sourceID2, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.4: start a task with two sources.
	// can't add more than one tasks at a time now.
	c.Assert(terror.ErrSchedulerMultiTask.Equal(s.AddSubTasks(false, subtaskCfg1, subtaskCfg21)), IsTrue)
	// task2' config and stage not exists before.
	t.subTaskCfgNotExist(c, s, taskName2, sourceID1)
	t.subTaskCfgNotExist(c, s, taskName2, sourceID2)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_InvalidStage)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_InvalidStage)
	// start task2.
	c.Assert(s.AddSubTasks(false, subtaskCfg21, subtaskCfg22), IsNil)
	// config added, stage become Running.
	t.subTaskCfgExist(c, s, subtaskCfg21)
	t.subTaskCfgExist(c, s, subtaskCfg22)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.4.1 fail to stop any task.
	// can call without tasks or sources, return without error, but take no effect.
	c.Assert(s.RemoveSubTasks("", sourceID1), IsNil)
	c.Assert(s.RemoveSubTasks(taskName1), IsNil)
	// stop not exist task.
	c.Assert(terror.ErrSchedulerSubTaskOpTaskNotExist.Equal(s.RemoveSubTasks("not-exist", sourceID1)), IsTrue)
	// config and stage not changed.
	t.subTaskCfgExist(c, s, subtaskCfg21)
	t.subTaskCfgExist(c, s, subtaskCfg22)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_Running)

	// CASE 4.5: update subtasks stage from different current stage.
	// pause <task2, source1>.
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName2, sourceID1), IsNil)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_Paused)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_Running)
	// resume <task2, source1 and source2>.
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Running, taskName2, sourceID1, sourceID2), IsNil)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.6: try remove source when subtasks exist.
	// wait for source bound recovered
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 2 && bounds[0] == sourceID1 && bounds[1] == sourceID2
	}), IsTrue)
	c.Assert(terror.ErrSchedulerSourceOpTaskExist.Equal(s.RemoveSourceCfg(sourceID2)), IsTrue)
	// source2 keep there.
	t.sourceCfgExist(c, s, &sourceCfg2)
	// source2 still bound to worker2.
	t.workerBound(c, s, ha.NewSourceBound(sourceID2, workerName2))
	t.sourceBounds(c, s, []string{sourceID1, sourceID2}, []string{})
	t.relayStageMatch(c, s, sourceID2, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.7: stop task2.
	c.Assert(s.RemoveSubTasks(taskName2, sourceID1, sourceID2), IsNil)
	t.subTaskCfgNotExist(c, s, taskName2, sourceID1)
	t.subTaskCfgNotExist(c, s, taskName2, sourceID2)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_InvalidStage)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_InvalidStage)
	rebuildScheduler(ctx)

	// CASE 4.7: remove source2.
	c.Assert(s.StopRelay(sourceID2, []string{workerName2}), IsNil)
	c.Assert(s.RemoveSourceCfg(sourceID2), IsNil)
	c.Assert(terror.ErrSchedulerSourceCfgNotExist.Equal(s.RemoveSourceCfg(sourceID2)), IsTrue) // already removed.
	// source2 removed.
	t.sourceCfgNotExist(c, s, sourceID2)
	// worker2 become Free now.
	t.workerFree(c, s, workerName2)
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	t.relayStageMatch(c, s, sourceID2, pb.Stage_InvalidStage)
	rebuildScheduler(ctx)

	// CASE 4.8: worker1 become offline.
	// before shutdown, worker1 bound source
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName1))
	// cancel keep-alive.
	cancel1()
	// wait for worker1 become offline.
	c.Assert(utils.WaitSomething(int(3*keepAliveTTL), time.Second, func() bool {
		w := s.GetWorkerByName(workerName1)
		c.Assert(w, NotNil)
		return w.Stage() == WorkerOffline
	}), IsTrue)
	t.workerOffline(c, s, workerName1)
	// source1 should bound to worker2.
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName2))
	// expect stages keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.9: remove worker1.
	c.Assert(s.RemoveWorker(workerName1), IsNil)
	c.Assert(terror.ErrSchedulerWorkerNotExist.Equal(s.RemoveWorker(workerName1)), IsTrue) // can't remove multiple times.
	// worker1 not exists now.
	t.workerNotExist(c, s, workerName1)
	rebuildScheduler(ctx)

	// CASE 4.10: stop task1.
	// wait for worker2 become bouned.
	c.Assert(utils.WaitSomething(int(3*keepAliveTTL), time.Second, func() bool {
		w := s.GetWorkerByName(workerName2)
		c.Assert(w, NotNil)
		return w.Stage() == WorkerBound
	}), IsTrue)
	c.Assert(s.RemoveSubTasks(taskName1, sourceID1), IsNil)
	t.subTaskCfgNotExist(c, s, taskName1, sourceID1)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_InvalidStage)
	rebuildScheduler(ctx)

	// CASE 4.11: remove worker not supported when the worker is online.
	// wait for worker2 become bouned.
	c.Assert(utils.WaitSomething(int(3*keepAliveTTL), time.Second, func() bool {
		w := s.GetWorkerByName(workerName2)
		c.Assert(w, NotNil)
		return w.Stage() == WorkerBound
	}), IsTrue)
	c.Assert(terror.ErrSchedulerWorkerOnline.Equal(s.RemoveWorker(workerName2)), IsTrue)
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName2))
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.12: worker2 become offline.
	cancel2()
	wg.Wait()
	// wait for worker2 become offline.
	c.Assert(utils.WaitSomething(int(3*keepAliveTTL), time.Second, func() bool {
		w := s.GetWorkerByName(workerName2)
		c.Assert(w, NotNil)
		return w.Stage() == WorkerOffline
	}), IsTrue)
	t.workerOffline(c, s, workerName2)
	// source1 should unbound
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	// expect stages keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.13: remove worker2.
	c.Assert(s.RemoveWorker(workerName2), IsNil)
	t.workerNotExist(c, s, workerName2)
	// relay stage still there.
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	rebuildScheduler(ctx)

	// CASE 4.14: remove source1.
	c.Assert(s.RemoveSourceCfg(sourceID1), IsNil)
	t.sourceCfgNotExist(c, s, sourceID1)
	t.sourceBounds(c, s, []string{}, []string{})
	t.relayStageMatch(c, s, sourceID1, pb.Stage_InvalidStage)
}

func (t *testScheduler) sourceCfgNotExist(c *C, s *Scheduler, source string) {
	c.Assert(s.GetSourceCfgByID(source), IsNil)
	scm, _, err := ha.GetSourceCfg(etcdTestCli, source, 0)
	c.Assert(err, IsNil)
	c.Assert(scm, HasLen, 0)
}

func (t *testScheduler) sourceCfgExist(c *C, s *Scheduler, expectCfg *config.SourceConfig) {
	cfgP := s.GetSourceCfgByID(expectCfg.SourceID)
	c.Assert(cfgP, DeepEquals, expectCfg)
	scm, _, err := ha.GetSourceCfg(etcdTestCli, expectCfg.SourceID, 0)
	c.Assert(err, IsNil)
	cfgV := scm[expectCfg.SourceID]
	c.Assert(cfgV, DeepEquals, expectCfg)
}

func (t *testScheduler) subTaskCfgNotExist(c *C, s *Scheduler, task, source string) {
	c.Assert(s.getSubTaskCfgByTaskSource(task, source), IsNil)
	cfgM, _, err := ha.GetSubTaskCfg(etcdTestCli, source, task, 0)
	c.Assert(err, IsNil)
	c.Assert(cfgM, HasLen, 0)
}

func (t *testScheduler) subTaskCfgExist(c *C, s *Scheduler, expectCfg config.SubTaskConfig) {
	cfgP := s.getSubTaskCfgByTaskSource(expectCfg.Name, expectCfg.SourceID)
	c.Assert(cfgP, DeepEquals, &expectCfg)
	cfgM, _, err := ha.GetSubTaskCfg(etcdTestCli, expectCfg.SourceID, expectCfg.Name, 0)
	c.Assert(err, IsNil)
	c.Assert(cfgM, HasLen, 1)
	c.Assert(cfgM[expectCfg.Name], DeepEquals, expectCfg)
}

func (t *testScheduler) downstreamMetaNotExist(c *C, s *Scheduler, task string) {
	dbConfig, metaConfig := s.GetDownstreamMetaByTask(task)
	c.Assert(dbConfig, IsNil)
	c.Assert(metaConfig, Equals, "")
}

func (t *testScheduler) downstreamMetaExist(c *C, s *Scheduler, task string, expectDBCfg config.DBConfig, expectMetaConfig string) {
	dbConfig, metaConfig := s.GetDownstreamMetaByTask(task)
	c.Assert(dbConfig, NotNil)
	c.Assert(dbConfig, DeepEquals, &expectDBCfg)
	c.Assert(metaConfig, Equals, expectMetaConfig)
}

func (t *testScheduler) workerNotExist(c *C, s *Scheduler, worker string) {
	c.Assert(s.GetWorkerByName(worker), IsNil)
	wm, _, err := ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	_, ok := wm[worker]
	c.Assert(ok, IsFalse)
}

func (t *testScheduler) workerExist(c *C, s *Scheduler, info ha.WorkerInfo) {
	c.Assert(s.GetWorkerByName(info.Name), NotNil)
	c.Assert(s.GetWorkerByName(info.Name).BaseInfo(), DeepEquals, info)
	wm, _, err := ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(wm[info.Name], DeepEquals, info)
}

func (t *testScheduler) workerOffline(c *C, s *Scheduler, worker string) {
	w := s.GetWorkerByName(worker)
	c.Assert(w, NotNil)
	c.Assert(w.Bound(), DeepEquals, nullBound)
	c.Assert(w.Stage(), Equals, WorkerOffline)
	wm, _, err := ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	_, ok := wm[worker]
	c.Assert(ok, IsTrue)
	sbm, _, err := ha.GetSourceBound(etcdTestCli, worker)
	c.Assert(err, IsNil)
	_, ok = sbm[worker]
	c.Assert(ok, IsFalse)
}

func (t *testScheduler) workerFree(c *C, s *Scheduler, worker string) {
	w := s.GetWorkerByName(worker)
	c.Assert(w, NotNil)
	c.Assert(w.Bound(), DeepEquals, nullBound)
	c.Assert(w.Stage(), Equals, WorkerFree)
	wm, _, err := ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	_, ok := wm[worker]
	c.Assert(ok, IsTrue)
	sbm, _, err := ha.GetSourceBound(etcdTestCli, worker)
	c.Assert(err, IsNil)
	_, ok = sbm[worker]
	c.Assert(ok, IsFalse)
}

func (t *testScheduler) workerBound(c *C, s *Scheduler, bound ha.SourceBound) {
	w := s.GetWorkerByName(bound.Worker)
	c.Assert(w, NotNil)
	boundDeepEqualExcludeRev(c, w.Bound(), bound)
	c.Assert(w.Stage(), Equals, WorkerBound)
	wm, _, err := ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	_, ok := wm[bound.Worker]
	c.Assert(ok, IsTrue)
	sbm, _, err := ha.GetSourceBound(etcdTestCli, bound.Worker)
	c.Assert(err, IsNil)
	boundDeepEqualExcludeRev(c, sbm[bound.Worker], bound)
}

func (t *testScheduler) sourceBounds(c *C, s *Scheduler, expectBounds, expectUnbounds []string) {
	c.Assert(s.BoundSources(), DeepEquals, expectBounds)
	c.Assert(s.UnboundSources(), DeepEquals, expectUnbounds)

	wToB, _, err := ha.GetSourceBound(etcdTestCli, "")
	c.Assert(err, IsNil)
	c.Assert(wToB, HasLen, len(expectBounds))

	sToB := make(map[string]ha.SourceBound, len(wToB))
	for _, b := range wToB {
		sToB[b.Source] = b
	}
	for _, source := range expectBounds {
		c.Assert(sToB[source], NotNil)
		c.Assert(s.GetWorkerBySource(source), NotNil)
		c.Assert(s.GetWorkerBySource(source).Stage(), Equals, WorkerBound)
		boundDeepEqualExcludeRev(c, sToB[source], s.GetWorkerBySource(source).Bound())
	}

	for _, source := range expectUnbounds {
		c.Assert(s.GetWorkerBySource(source), IsNil)
	}
}

func boundDeepEqualExcludeRev(c *C, bound, expectBound ha.SourceBound) {
	expectBound.Revision = bound.Revision
	c.Assert(bound, DeepEquals, expectBound)
}

func stageDeepEqualExcludeRev(c *C, stage, expectStage ha.Stage) {
	expectStage.Revision = stage.Revision
	c.Assert(stage, DeepEquals, expectStage)
}

func (t *testScheduler) relayStageMatch(c *C, s *Scheduler, source string, expectStage pb.Stage) {
	stage := ha.NewRelayStage(expectStage, source)
	stageDeepEqualExcludeRev(c, s.GetExpectRelayStage(source), stage)

	eStage, _, err := ha.GetRelayStage(etcdTestCli, source)
	c.Assert(err, IsNil)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		stageDeepEqualExcludeRev(c, eStage, stage)
	default:
		c.Assert(eStage, DeepEquals, stageEmpty)
	}
}

func (t *testScheduler) subTaskStageMatch(c *C, s *Scheduler, task, source string, expectStage pb.Stage) {
	stage := ha.NewSubTaskStage(expectStage, source, task)
	stageDeepEqualExcludeRev(c, s.GetExpectSubTaskStage(task, source), stage)

	eStageM, _, err := ha.GetSubTaskStage(etcdTestCli, source, task)
	c.Assert(err, IsNil)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		c.Assert(eStageM, HasLen, 1)
		stageDeepEqualExcludeRev(c, eStageM[task], stage)
	default:
		c.Assert(eStageM, HasLen, 0)
	}
}

func (t *testScheduler) TestRestartScheduler(c *C) {
	defer clearTestInfoOperation(c)

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
	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, IsNil)
	sourceCfg1.SourceID = sourceID1

	s := NewScheduler(&logger, config.Security{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// step 1: start scheduler
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)
	// step 1.1: add sourceCfg and worker
	c.Assert(s.AddSourceCfg(sourceCfg1), IsNil)
	t.sourceCfgExist(c, s, sourceCfg1)
	c.Assert(s.AddWorker(workerName1, workerAddr1), IsNil)
	t.workerExist(c, s, workerInfo1)
	// step 2: start a worker
	// step 2.1: worker start watching source bound
	bsm, revBound, err := ha.GetSourceBound(etcdTestCli, workerName1)
	c.Assert(err, IsNil)
	c.Assert(bsm, HasLen, 0)
	sourceBoundCh := make(chan ha.SourceBound, 10)
	sourceBoundErrCh := make(chan error, 10)
	go func() {
		ha.WatchSourceBound(ctx, etcdTestCli, workerName1, revBound+1, sourceBoundCh, sourceBoundErrCh)
	}()
	// step 2.2: worker start keepAlive
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx1, etcdTestCli, workerName1, keepAliveTTL), IsNil)
	}()
	// step 2.3: scheduler should bound source to worker
	// wait for source1 bound to worker1.
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	}), IsTrue)
	checkSourceBoundCh := func() {
		c.Assert(utils.WaitSomething(10, 500*time.Millisecond, func() bool {
			return len(sourceBoundCh) == 1
		}), IsTrue)
		sourceBound := <-sourceBoundCh
		sourceBound.Revision = 0
		c.Assert(sourceBound, DeepEquals, sourceBound1)
		c.Assert(sourceBoundErrCh, HasLen, 0)
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
	c.Assert(sourceBoundCh, HasLen, 0)
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)
	// worker should receive the trigger event again
	checkSourceBoundCh()
	// case 2: worker is restarted, and worker keepalive broke but scheduler didn't catch the delete event
	// step 4: restart worker keepalive, which can simulator one situation:
	//			a. worker keepalive breaks but re-setup again before keepaliveTTL is timeout
	c.Assert(sourceBoundCh, HasLen, 0)
	ctx2, cancel2 := context.WithCancel(ctx)
	// trigger same keepalive event again, just for test
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx2, etcdTestCli, workerName1, keepAliveTTL), IsNil)
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
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		kam, _, err := ha.GetKeepAliveWorkers(etcdTestCli)
		return err == nil && len(kam) == 0
	}), IsTrue)
	c.Assert(sourceBoundCh, HasLen, 0)
	c.Assert(s.Start(ctx, etcdTestCli), IsNil) // restart scheduler
	c.Assert(s.BoundSources(), HasLen, 0)
	unbounds := s.UnboundSources()
	c.Assert(unbounds, HasLen, 1)
	c.Assert(unbounds[0], Equals, sourceID1)
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
		c.Assert(ha.KeepAlive(ctx4, etcdTestCli, workerName1, keepAliveTTL), IsNil)
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
	c.Assert(s.AddWorker(workerName2, workerAddr2), IsNil)
	t.workerExist(c, s, workerInfo2)

	// step 2.2: worker start keepAlive
	go func() {
		c.Assert(ha.KeepAlive(ctx, etcdTestCli, workerName2, keepAliveTTL), IsNil)
	}()

	s.Close() // stop scheduler
	cancel4() // stop worker keepalive
	wg.Wait()
	// check whether keepalive lease is out of date
	time.Sleep(time.Duration(keepAliveTTL) * time.Second)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		kam, _, err := ha.GetKeepAliveWorkers(etcdTestCli)
		return err == nil && len(kam) == 1
	}), IsTrue)
	c.Assert(sourceBoundCh, HasLen, 0)
	c.Assert(s.Start(ctx, etcdTestCli), IsNil) // restart scheduler
	c.Assert(s.BoundSources(), HasLen, 1)
	w := s.workers[workerName2]
	c.Assert(w.stage, Equals, WorkerBound)
	c.Assert(w.bound.Source, Equals, sourceID1)
	unbounds = s.UnboundSources()
	c.Assert(unbounds, HasLen, 0)
}

func (t *testScheduler) TestWatchWorkerEventEtcdCompact(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger       = log.L()
		s            = NewScheduler(&logger, config.Security{})
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
	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, IsNil)
	sourceCfg1.SourceID = sourceID1
	sourceCfg2 := *sourceCfg1
	sourceCfg2.SourceID = sourceID2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// step 1: start an empty scheduler without listening the worker event
	s.started.Store(true)
	s.cancel = cancel
	s.etcdCli = etcdTestCli

	// step 2: add two sources and register four workers
	c.Assert(s.AddSourceCfg(sourceCfg1), IsNil)
	c.Assert(s.AddSourceCfg(&sourceCfg2), IsNil)
	c.Assert(s.unbounds, HasLen, 2)
	c.Assert(s.unbounds, HasKey, sourceID1)
	c.Assert(s.unbounds, HasKey, sourceID2)

	c.Assert(s.AddWorker(workerName1, workerAddr1), IsNil)
	c.Assert(s.AddWorker(workerName2, workerAddr2), IsNil)
	c.Assert(s.AddWorker(workerName3, workerAddr3), IsNil)
	c.Assert(s.AddWorker(workerName4, workerAddr4), IsNil)
	c.Assert(s.workers, HasLen, 4)
	c.Assert(s.workers, HasKey, workerName1)
	c.Assert(s.workers, HasKey, workerName2)
	c.Assert(s.workers, HasKey, workerName3)
	c.Assert(s.workers, HasKey, workerName4)

	// step 3: add two workers, and then cancel them to simulate they have lost connection
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx1, etcdTestCli, workerName1, keepAliveTTL), IsNil)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx1, etcdTestCli, workerName2, keepAliveTTL), IsNil)
	}()
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		kam, _, e := ha.GetKeepAliveWorkers(etcdTestCli)
		return e == nil && len(kam) == 2
	}), IsTrue)
	cancel1()
	wg.Wait()
	// check whether keepalive lease is out of date
	time.Sleep(time.Duration(keepAliveTTL) * time.Second)
	var rev int64
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		kam, rev1, e := ha.GetKeepAliveWorkers(etcdTestCli)
		rev = rev1
		return e == nil && len(kam) == 0
	}), IsTrue)

	// step 4: trigger etcd compaction and check whether we can receive it through watcher
	var startRev int64 = 1
	_, err = etcdTestCli.Compact(ctx, rev)
	c.Assert(err, IsNil)
	workerEvCh := make(chan ha.WorkerEvent, 10)
	workerErrCh := make(chan error, 10)
	ha.WatchWorkerEvent(ctx, etcdTestCli, startRev, workerEvCh, workerErrCh)
	select {
	case err := <-workerErrCh:
		c.Assert(err, Equals, etcdErrCompacted)
	case <-time.After(time.Second):
		c.Fatal("fail to get etcd error compacted")
	}

	// step 5: scheduler start to handle workerEvent from compact revision, should handle worker keepalive events correctly
	ctx2, cancel2 := context.WithCancel(ctx)
	// step 5.1: start one worker before scheduler start to handle workerEvent
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx2, etcdTestCli, workerName3, keepAliveTTL), IsNil)
	}()
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		kam, _, err := ha.GetKeepAliveWorkers(etcdTestCli)
		if err == nil {
			if _, ok := kam[workerName3]; ok {
				return len(kam) == 1
			}
		}
		return false
	}), IsTrue)
	// step 5.2: scheduler start to handle workerEvent
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(s.observeWorkerEvent(ctx2, startRev), IsNil)
	}()
	// step 5.3: wait for scheduler to restart handleWorkerEvent, then start a new worker
	time.Sleep(time.Second)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx2, etcdTestCli, workerName4, keepAliveTTL), IsNil)
	}()
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		unbounds := s.UnboundSources()
		return len(unbounds) == 0
	}), IsTrue)
	c.Assert(s.BoundSources(), DeepEquals, []string{sourceID1, sourceID2})
	cancel2()
	wg.Wait()

	// step 6: restart to observe workerEvents, should unbound all sources
	ctx3, cancel3 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(s.observeWorkerEvent(ctx3, startRev), IsNil)
	}()
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 0
	}), IsTrue)
	c.Assert(s.UnboundSources(), DeepEquals, []string{sourceID1, sourceID2})
	cancel3()
	wg.Wait()
}

func (t *testScheduler) TestLastBound(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger      = log.L()
		s           = NewScheduler(&logger, config.Security{})
		sourceID1   = "mysql-replica-1"
		sourceID2   = "mysql-replica-2"
		workerName1 = "dm-worker-1"
		workerName2 = "dm-worker-2"
		workerName3 = "dm-worker-3"
		workerName4 = "dm-worker-4"
	)

	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, IsNil)
	sourceCfg1.SourceID = sourceID1
	sourceCfg2 := sourceCfg1
	sourceCfg2.SourceID = sourceID2
	worker1 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName1}}
	worker2 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName2}}
	worker3 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName3}}
	worker4 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName4}}

	// step 1: start an empty scheduler without listening the worker event
	s.started.Store(true)
	s.etcdCli = etcdTestCli
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

	// worker1 goes to last bounded source
	worker1.ToFree()
	bounded, err := s.tryBoundForWorker(worker1)
	c.Assert(err, IsNil)
	c.Assert(bounded, IsTrue)
	c.Assert(s.bounds[sourceID1], DeepEquals, worker1)

	// worker3 has to bounded to source2
	worker3.ToFree()
	bounded, err = s.tryBoundForWorker(worker3)
	c.Assert(err, IsNil)
	c.Assert(bounded, IsTrue)
	c.Assert(s.bounds[sourceID2], DeepEquals, worker3)

	// though worker2 has a previous source, that source is not available, so not bound
	worker2.ToFree()
	bounded, err = s.tryBoundForWorker(worker2)
	c.Assert(err, IsNil)
	c.Assert(bounded, IsFalse)

	// worker4 is used to test whether source2 should be bounded to worker2 rather than a new worker
	worker4.ToFree()
	bounded, err = s.tryBoundForWorker(worker4)
	c.Assert(err, IsNil)
	c.Assert(bounded, IsFalse)

	// after worker3 become offline, source2 should be bounded to worker2
	s.updateStatusToUnbound(sourceID2)
	_, ok := s.bounds[sourceID2]
	c.Assert(ok, IsFalse)
	worker3.ToOffline()
	bounded, err = s.tryBoundForSource(sourceID2)
	c.Assert(err, IsNil)
	c.Assert(bounded, IsTrue)
	c.Assert(s.bounds[sourceID2], DeepEquals, worker2)
}

func (t *testScheduler) TestInvalidLastBound(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger      = log.L()
		s           = NewScheduler(&logger, config.Security{})
		sourceID1   = "mysql-replica-1"
		sourceID2   = "invalid-replica-1"
		workerName1 = "dm-worker-1"
	)

	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, IsNil)
	sourceCfg1.SourceID = sourceID1
	sourceCfg2 := sourceCfg1
	sourceCfg2.SourceID = sourceID2
	worker1 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName1}}

	// step 1: start an empty scheduler without listening the worker event
	s.started.Store(true)
	s.etcdCli = etcdTestCli
	s.workers[workerName1] = worker1
	// sourceID2 doesn't have a source config and not in unbound
	s.sourceCfgs[sourceID1] = sourceCfg1
	s.lastBound[workerName1] = ha.SourceBound{Source: sourceID2}
	s.unbounds[sourceID1] = struct{}{}
	// step2: worker1 doesn't go to last bounded source, because last source doesn't have a source config (might be removed)
	worker1.ToFree()
	bounded, err := s.tryBoundForWorker(worker1)
	c.Assert(err, IsNil)
	c.Assert(bounded, IsTrue)
	c.Assert(s.bounds[sourceID1], DeepEquals, worker1)
}

func (t *testScheduler) TestTransferSource(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger      = log.L()
		s           = NewScheduler(&logger, config.Security{})
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
	s.etcdCli = etcdTestCli
	s.workers[workerName1] = worker1
	s.workers[workerName2] = worker2
	s.workers[workerName3] = worker3
	s.workers[workerName4] = worker4
	s.sourceCfgs[sourceID1] = &config.SourceConfig{}
	s.sourceCfgs[sourceID2] = &config.SourceConfig{}

	worker1.ToFree()
	c.Assert(s.boundSourceToWorker(sourceID1, worker1), IsNil)
	worker2.ToFree()
	c.Assert(s.boundSourceToWorker(sourceID2, worker2), IsNil)

	c.Assert(s.bounds[sourceID1], DeepEquals, worker1)
	c.Assert(s.bounds[sourceID2], DeepEquals, worker2)

	worker3.ToFree()
	worker4.ToFree()

	ctx := context.Background()
	// test invalid transfer: source not exists
	c.Assert(s.TransferSource(ctx, "not-exist", workerName3), NotNil)

	// test valid transfer: source -> worker = bound -> free
	c.Assert(s.TransferSource(ctx, sourceID1, workerName4), IsNil)
	c.Assert(s.bounds[sourceID1], DeepEquals, worker4)
	c.Assert(worker1.Stage(), Equals, WorkerFree)

	// test valid transfer: source -> worker = unbound -> free
	s.sourceCfgs[sourceID3] = &config.SourceConfig{}
	s.unbounds[sourceID3] = struct{}{}
	c.Assert(s.TransferSource(ctx, sourceID3, workerName3), IsNil)
	c.Assert(s.bounds[sourceID3], DeepEquals, worker3)

	// test valid transfer: self
	c.Assert(s.TransferSource(ctx, sourceID3, workerName3), IsNil)
	c.Assert(s.bounds[sourceID3], DeepEquals, worker3)

	// test invalid transfer: source -> worker = bound -> bound
	c.Assert(s.TransferSource(ctx, sourceID1, workerName3), NotNil)
	c.Assert(s.bounds[sourceID1], DeepEquals, worker4)
	c.Assert(s.bounds[sourceID3], DeepEquals, worker3)

	// test invalid transfer: source -> worker = bound -> offline
	worker1.ToOffline()
	c.Assert(s.TransferSource(ctx, sourceID1, workerName1), NotNil)
	c.Assert(s.bounds[sourceID1], DeepEquals, worker4)

	// test invalid transfer: source -> worker = unbound -> bound
	s.sourceCfgs[sourceID4] = &config.SourceConfig{}
	s.unbounds[sourceID4] = struct{}{}
	c.Assert(s.TransferSource(ctx, sourceID4, workerName3), NotNil)
	c.Assert(s.bounds[sourceID3], DeepEquals, worker3)
	delete(s.unbounds, sourceID4)
	delete(s.sourceCfgs, sourceID4)

	worker1.ToFree()
	// now we have (worker1, nil) (worker2, source2) (worker3, source3) (worker4, source1)

	// test fail halfway won't left old worker unbound
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/scheduler/failToReplaceSourceBound", `return()`), IsNil)
	c.Assert(s.TransferSource(ctx, sourceID1, workerName1), NotNil)
	c.Assert(s.bounds[sourceID1], DeepEquals, worker4)
	c.Assert(worker1.Stage(), Equals, WorkerFree)
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/failToReplaceSourceBound"), IsNil)

	// set running tasks
	s.expectSubTaskStages.Store("test", map[string]ha.Stage{sourceID1: {Expect: pb.Stage_Running}})

	// test can't transfer when running tasks not in sync unit
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/scheduler/operateWorkerQueryStatus", `return("notInSyncUnit")`), IsNil)
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/operateWorkerQueryStatus") //nolint:errcheck
	c.Assert(terror.ErrSchedulerRequireRunningTaskInSyncUnit.Equal(s.TransferSource(ctx, sourceID1, workerName1)), IsTrue)
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/operateWorkerQueryStatus"), IsNil)

	// test can't transfer when query status met error
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/scheduler/operateWorkerQueryStatus", `return("error")`), IsNil)
	c.Assert(s.TransferSource(ctx, sourceID1, workerName1), ErrorMatches, "failed to query worker.*")
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/operateWorkerQueryStatus"), IsNil)

	// test can transfer when all running task is in sync unit
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/scheduler/skipBatchOperateTaskOnWorkerSleep", `return()`), IsNil)
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/skipBatchOperateTaskOnWorkerSleep") //nolint:errcheck
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/scheduler/operateWorkerQueryStatus", `return("allTaskIsPaused")`), IsNil)

	// we only retry 10 times, open a failpoint to make need retry more than 10 times, so this transfer will fail
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/scheduler/batchOperateTaskOnWorkerMustRetry", `return(11)`), IsNil)
	c.Assert(terror.ErrSchedulerPauseTaskForTransferSource.Equal(s.TransferSource(ctx, sourceID1, workerName1)), IsTrue)
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/batchOperateTaskOnWorkerMustRetry"), IsNil)

	// now we can transfer successfully after 2 times retry
	s.expectSubTaskStages.Store("test", map[string]ha.Stage{sourceID1: {Expect: pb.Stage_Running}})
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/scheduler/batchOperateTaskOnWorkerMustRetry", `return(2)`), IsNil)
	c.Assert(s.TransferSource(ctx, sourceID1, workerName1), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/batchOperateTaskOnWorkerMustRetry"), IsNil)
	c.Assert(s.bounds[sourceID1], DeepEquals, worker1)
	c.Assert(worker1.Stage(), Equals, WorkerBound)
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/operateWorkerQueryStatus"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/skipBatchOperateTaskOnWorkerSleep"), IsNil)
}

func (t *testScheduler) TestStartStopRelay(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger      = log.L()
		s           = NewScheduler(&logger, config.Security{})
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
	s.etcdCli = etcdTestCli
	s.workers[workerName1] = worker1
	s.workers[workerName2] = worker2
	s.workers[workerName3] = worker3
	s.workers[workerName4] = worker4
	s.sourceCfgs[sourceID1] = &config.SourceConfig{}
	s.sourceCfgs[sourceID2] = &config.SourceConfig{}

	worker1.ToFree()
	c.Assert(s.boundSourceToWorker(sourceID1, worker1), IsNil)
	worker2.ToFree()
	c.Assert(s.boundSourceToWorker(sourceID2, worker2), IsNil)

	c.Assert(s.bounds[sourceID1], DeepEquals, worker1)
	c.Assert(s.bounds[sourceID2], DeepEquals, worker2)

	worker3.ToFree()
	worker4.ToFree()

	// test not exist source
	c.Assert(terror.ErrSchedulerSourceCfgNotExist.Equal(s.StartRelay(sourceID3, []string{workerName1})), IsTrue)
	c.Assert(terror.ErrSchedulerSourceCfgNotExist.Equal(s.StopRelay(sourceID4, []string{workerName1})), IsTrue)
	noWorkerSources := []string{sourceID1, sourceID2, sourceID3, sourceID4}
	for _, source := range noWorkerSources {
		workers, err := s.GetRelayWorkers(source)
		c.Assert(err, IsNil)
		c.Assert(workers, HasLen, 0)
	}

	// start-relay success on bound-same-source and free worker
	c.Assert(s.StartRelay(sourceID1, []string{workerName1}), IsNil)
	c.Assert(s.StartRelay(sourceID1, []string{workerName1}), IsNil)
	c.Assert(s.expectRelayStages, HasLen, 1)
	c.Assert(s.expectRelayStages, HasKey, sourceID1)
	c.Assert(s.StartRelay(sourceID1, []string{workerName3}), IsNil)
	c.Assert(s.relayWorkers, HasLen, 1)
	c.Assert(s.relayWorkers[sourceID1], HasLen, 2)
	c.Assert(s.relayWorkers[sourceID1], HasKey, workerName1)
	c.Assert(s.relayWorkers[sourceID1], HasKey, workerName3)
	workers, err := s.GetRelayWorkers(sourceID1)
	c.Assert(err, IsNil)
	c.Assert(workers, DeepEquals, []*Worker{worker1, worker3})

	// failed on bound-not-same-source worker and not exist worker
	c.Assert(terror.ErrSchedulerRelayWorkersWrongBound.Equal(s.StartRelay(sourceID1, []string{workerName2})), IsTrue)
	c.Assert(terror.ErrSchedulerWorkerNotExist.Equal(s.StartRelay(sourceID1, []string{"not-exist"})), IsTrue)

	// failed on one worker multiple relay source
	c.Assert(terror.ErrSchedulerRelayWorkersBusy.Equal(s.StartRelay(sourceID2, []string{workerName3})), IsTrue)

	// start another relay worker
	c.Assert(s.StartRelay(sourceID2, []string{workerName2}), IsNil)
	c.Assert(s.expectRelayStages, HasLen, 2)
	c.Assert(s.expectRelayStages, HasKey, sourceID2)
	c.Assert(s.relayWorkers[sourceID2], HasLen, 1)
	c.Assert(s.relayWorkers[sourceID2], HasKey, workerName2)
	workers, err = s.GetRelayWorkers(sourceID2)
	c.Assert(err, IsNil)
	c.Assert(workers, DeepEquals, []*Worker{worker2})

	// failed on not-same-source worker and not exist worker
	c.Assert(terror.ErrSchedulerRelayWorkersWrongRelay.Equal(s.StopRelay(sourceID1, []string{workerName2})), IsTrue)
	c.Assert(terror.ErrSchedulerWorkerNotExist.Equal(s.StopRelay(sourceID1, []string{"not-exist"})), IsTrue)

	// nothing changed
	workers, err = s.GetRelayWorkers(sourceID1)
	c.Assert(err, IsNil)
	c.Assert(workers, DeepEquals, []*Worker{worker1, worker3})
	workers, err = s.GetRelayWorkers(sourceID2)
	c.Assert(err, IsNil)
	c.Assert(workers, DeepEquals, []*Worker{worker2})

	// stop-relay success
	c.Assert(s.StopRelay(sourceID1, []string{workerName1}), IsNil)
	c.Assert(s.StopRelay(sourceID1, []string{workerName1}), IsNil)
	c.Assert(s.StopRelay(sourceID1, []string{workerName3}), IsNil)
	c.Assert(s.expectRelayStages, HasLen, 1)
	c.Assert(s.expectRelayStages, HasKey, sourceID2)
	c.Assert(s.relayWorkers, HasLen, 1)
	c.Assert(s.relayWorkers, HasKey, sourceID2)
	workers, err = s.GetRelayWorkers(sourceID1)
	c.Assert(err, IsNil)
	c.Assert(workers, HasLen, 0)

	// can't bind source to a worker which has different relay
	// currently source1 -> worker1, source2 -> worker2
	c.Assert(s.bounds[sourceID1].baseInfo.Name, Equals, workerName1)
	c.Assert(s.bounds[sourceID2].baseInfo.Name, Equals, workerName2)

	s.updateStatusToUnbound(sourceID2)
	c.Assert(worker2.Stage(), Equals, WorkerRelay)
	c.Assert(s.StopRelay(sourceID2, []string{workerName2}), IsNil)
	c.Assert(worker2.Stage(), Equals, WorkerFree)

	c.Assert(s.StartRelay(sourceID1, []string{workerName2}), IsNil)
	c.Assert(worker2.Stage(), Equals, WorkerRelay)
	c.Assert(worker2.RelaySourceID(), Equals, sourceID1)

	worker3.ToOffline()
	worker4.ToOffline()

	bound, err := s.tryBoundForSource(sourceID2)
	c.Assert(err, IsNil)
	c.Assert(bound, IsFalse)
}

func (t *testScheduler) TestRelayWithWithoutWorker(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger      = log.L()
		s           = NewScheduler(&logger, config.Security{})
		sourceID1   = "mysql-replica-1"
		workerName1 = "dm-worker-1"
		workerName2 = "dm-worker-2"
	)

	worker1 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName1}}
	worker2 := &Worker{baseInfo: ha.WorkerInfo{Name: workerName2}}

	// step 1: start an empty scheduler
	s.started.Store(true)
	s.etcdCli = etcdTestCli
	s.workers[workerName1] = worker1
	s.workers[workerName2] = worker2
	s.sourceCfgs[sourceID1] = &config.SourceConfig{}

	worker1.ToFree()
	c.Assert(s.boundSourceToWorker(sourceID1, worker1), IsNil)
	worker2.ToFree()

	// step 2: check when enable-relay = false, can start/stop relay without worker name
	c.Assert(s.StartRelay(sourceID1, []string{}), IsNil)
	c.Assert(s.sourceCfgs[sourceID1].EnableRelay, IsTrue)

	c.Assert(s.StartRelay(sourceID1, []string{}), IsNil)
	c.Assert(s.sourceCfgs[sourceID1].EnableRelay, IsTrue)

	c.Assert(s.StopRelay(sourceID1, []string{}), IsNil)
	c.Assert(s.sourceCfgs[sourceID1].EnableRelay, IsFalse)

	c.Assert(s.StopRelay(sourceID1, []string{}), IsNil)
	c.Assert(s.sourceCfgs[sourceID1].EnableRelay, IsFalse)

	// step 3: check when enable-relay = false, can start/stop relay with worker name
	c.Assert(s.StartRelay(sourceID1, []string{workerName1, workerName2}), IsNil)
	c.Assert(s.sourceCfgs[sourceID1].EnableRelay, IsFalse)
	c.Assert(worker1.Stage(), Equals, WorkerBound)
	c.Assert(worker2.Stage(), Equals, WorkerRelay)

	c.Assert(s.StopRelay(sourceID1, []string{workerName1}), IsNil)
	c.Assert(worker1.Stage(), Equals, WorkerBound)
	c.Assert(worker2.Stage(), Equals, WorkerRelay)

	c.Assert(s.StopRelay(sourceID1, []string{workerName2}), IsNil)
	c.Assert(worker1.Stage(), Equals, WorkerBound)
	c.Assert(worker2.Stage(), Equals, WorkerFree)

	// step 4: check when enable-relay = true, can't start/stop relay with worker name
	c.Assert(s.StartRelay(sourceID1, []string{}), IsNil)

	err := s.StartRelay(sourceID1, []string{workerName1})
	c.Assert(terror.ErrSchedulerStartRelayOnBound.Equal(err), IsTrue)
	err = s.StartRelay(sourceID1, []string{workerName2})
	c.Assert(terror.ErrSchedulerStartRelayOnBound.Equal(err), IsTrue)

	err = s.StopRelay(sourceID1, []string{workerName1})
	c.Assert(terror.ErrSchedulerStopRelayOnBound.Equal(err), IsTrue)
	err = s.StopRelay(sourceID1, []string{workerName2})
	c.Assert(terror.ErrSchedulerStopRelayOnBound.Equal(err), IsTrue)

	c.Assert(s.StopRelay(sourceID1, []string{}), IsNil)

	// step5. check when started relay with workerName, can't turn on enable-relay
	c.Assert(s.StartRelay(sourceID1, []string{workerName1}), IsNil)

	err = s.StartRelay(sourceID1, []string{})
	c.Assert(terror.ErrSchedulerStartRelayOnSpecified.Equal(err), IsTrue)
	err = s.StopRelay(sourceID1, []string{})
	c.Assert(terror.ErrSchedulerStopRelayOnSpecified.Equal(err), IsTrue)
}

func checkAllWorkersClosed(c *C, s *Scheduler, closed bool) {
	for _, worker := range s.workers {
		cli, ok := worker.cli.(*workerrpc.GRPCClient)
		c.Assert(ok, IsTrue)
		c.Assert(cli.Closed(), Equals, closed)
	}
}

func (t *testScheduler) TestCloseAllWorkers(c *C) {
	var (
		logger = log.L()
		s      = NewScheduler(&logger, config.Security{})
		names  []string
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 1; i < 4; i++ {
		names = append(names, fmt.Sprintf("worker%d", i))
	}

	for i, name := range names {
		info := ha.NewWorkerInfo(name, fmt.Sprintf("127.0.0.1:%d", 50801+i))
		_, err := ha.PutWorkerInfo(etcdTestCli, info)
		c.Assert(err, IsNil)
	}

	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dm/master/scheduler/failToRecoverWorkersBounds", "return"), IsNil)
	// Test closed when fail to start
	c.Assert(s.Start(ctx, etcdTestCli), ErrorMatches, "failToRecoverWorkersBounds")
	c.Assert(s.workers, HasLen, 3)
	checkAllWorkersClosed(c, s, true)
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/dm/master/scheduler/failToRecoverWorkersBounds"), IsNil)

	s.workers = map[string]*Worker{}
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)
	checkAllWorkersClosed(c, s, false)
	s.Close()
	c.Assert(s.workers, HasLen, 3)
	checkAllWorkersClosed(c, s, true)
}

func (t *testScheduler) TestStartSourcesWithoutSourceConfigsInEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger       = log.L()
		s            = NewScheduler(&logger, config.Security{})
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
	s.etcdCli = etcdTestCli
	// found source configs before bound
	s.sourceCfgs[sourceID1] = &config.SourceConfig{}
	s.sourceCfgs[sourceID2] = &config.SourceConfig{}
	s.unbounds[sourceID1] = struct{}{}
	s.unbounds[sourceID2] = struct{}{}
	c.Assert(s.AddWorker(workerName1, workerAddr1), IsNil)
	c.Assert(s.AddWorker(workerName2, workerAddr2), IsNil)

	wg.Add(2)
	go func() {
		c.Assert(ha.KeepAlive(ctx, etcdTestCli, workerName1, keepaliveTTL), IsNil)
		wg.Done()
	}()
	go func() {
		c.Assert(ha.KeepAlive(ctx, etcdTestCli, workerName2, keepaliveTTL), IsNil)
		wg.Done()
	}()

	s.workers[workerName1].stage = WorkerFree
	s.workers[workerName2].stage = WorkerFree
	bounded, err := s.tryBoundForSource(sourceID1)
	c.Assert(err, IsNil)
	c.Assert(bounded, IsTrue)
	bounded, err = s.tryBoundForSource(sourceID2)
	c.Assert(err, IsNil)
	c.Assert(bounded, IsTrue)

	s.started.Store(false)
	sbm, _, err := ha.GetSourceBound(etcdTestCli, "")
	c.Assert(err, IsNil)
	c.Assert(sbm, HasLen, 2)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		kam, _, err2 := ha.GetKeepAliveWorkers(etcdTestCli)
		if err2 != nil {
			return false
		}
		return len(kam) == 2
	}), IsTrue)
	// there isn't any source config in etcd
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)
	c.Assert(s.bounds, HasLen, 0)
	sbm, _, err = ha.GetSourceBound(etcdTestCli, "")
	c.Assert(err, IsNil)
	c.Assert(sbm, HasLen, 0)
	cancel()
	wg.Wait()
}

func (t *testScheduler) TestTransferWorkerAndSource(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger      = log.L()
		s           = NewScheduler(&logger, config.Security{})
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
	s.etcdCli = etcdTestCli
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

	// test free worker and unbounded source
	c.Assert(s.transferWorkerAndSource(workerName1, "", "", sourceID1), IsNil)
	c.Assert(s.transferWorkerAndSource("", sourceID2, workerName2, ""), IsNil)
	c.Assert(s.bounds[sourceID1], DeepEquals, worker1)
	c.Assert(s.bounds[sourceID2], DeepEquals, worker2)
	c.Assert(len(s.unbounds), Equals, 0)

	// test transfer bounded source to free worker
	c.Assert(s.transferWorkerAndSource(workerName1, sourceID1, workerName4, ""), IsNil)
	c.Assert(s.bounds[sourceID1], DeepEquals, worker4)
	c.Assert(worker1.Stage(), Equals, WorkerFree)
	c.Assert(worker4.Stage(), Equals, WorkerBound)

	c.Assert(s.transferWorkerAndSource(workerName3, "", workerName2, sourceID2), IsNil)
	c.Assert(s.bounds[sourceID2], DeepEquals, worker3)
	c.Assert(worker2.Stage(), Equals, WorkerFree)
	c.Assert(worker3.Stage(), Equals, WorkerBound)

	// test transfer bounded worker to unbounded source
	s.unbounds[sourceID3] = struct{}{}
	s.unbounds[sourceID4] = struct{}{}
	c.Assert(s.transferWorkerAndSource("", sourceID3, workerName3, sourceID2), IsNil)
	c.Assert(s.bounds[sourceID3], DeepEquals, worker3)
	// sourceID2 bound to last bound worker
	c.Assert(s.bounds[sourceID2], DeepEquals, worker2)

	c.Assert(s.transferWorkerAndSource(workerName4, sourceID1, "", sourceID4), IsNil)
	c.Assert(s.bounds[sourceID4], DeepEquals, worker4)
	// sourceID1 bound to last bound worker
	c.Assert(s.bounds[sourceID1], DeepEquals, worker1)

	c.Assert(len(s.unbounds), Equals, 0)

	// test transfer two bounded sources
	c.Assert(s.transferWorkerAndSource(workerName1, sourceID1, workerName2, sourceID2), IsNil)
	c.Assert(s.transferWorkerAndSource(workerName4, sourceID4, workerName3, sourceID3), IsNil)
	c.Assert(s.bounds[sourceID1], DeepEquals, worker2)
	c.Assert(s.bounds[sourceID2], DeepEquals, worker1)
	c.Assert(s.bounds[sourceID3], DeepEquals, worker4)
	c.Assert(s.bounds[sourceID4], DeepEquals, worker3)

	c.Assert(worker1.StartRelay(sourceID2), IsNil)
	err := s.transferWorkerAndSource(workerName1, sourceID2, workerName2, sourceID1)
	c.Assert(terror.ErrSchedulerBoundDiffWithStartedRelay.Equal(err), IsTrue)
}

func (t *testScheduler) TestWatchLoadTask(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger      = log.L()
		s           = NewScheduler(&logger, config.Security{})
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
	s.etcdCli = etcdTestCli

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
	c.Assert(s.boundSourceToWorker(sourceID1, worker1), IsNil)
	worker2.ToFree()
	c.Assert(s.boundSourceToWorker(sourceID2, worker2), IsNil)
	c.Assert(s.bounds[sourceID1], DeepEquals, worker1)
	c.Assert(s.bounds[sourceID2], DeepEquals, worker2)

	worker3.ToFree()
	worker4.ToOffline()

	// put task1, source1, worker3
	_, err := ha.PutLoadTask(etcdTestCli, task1, sourceID1, workerName3)
	c.Assert(err, IsNil)
	// put task2, source2, worker4
	_, err = ha.PutLoadTask(etcdTestCli, task2, sourceID2, workerName4)
	c.Assert(err, IsNil)

	// get all load tasks
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel1()
	loadTasks, startRev, err := ha.GetAllLoadTask(etcdTestCli)
	c.Assert(err, IsNil)
	s.loadTasks = loadTasks

	c.Assert(s.hasLoadTaskByWorkerAndSource(workerName3, sourceID1), IsTrue)
	c.Assert(s.hasLoadTaskByWorkerAndSource(workerName4, sourceID2), IsTrue)

	// observer load tasks
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(s.observeLoadTask(ctx1, startRev), IsNil)
	}()

	// put task2, source1, worker1
	_, err = ha.PutLoadTask(etcdTestCli, task2, sourceID1, workerName1)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.hasLoadTaskByWorkerAndSource(workerName1, sourceID1)
	}), IsTrue)

	// del task2, source1, worker1
	_, _, err = ha.DelLoadTask(etcdTestCli, task2, sourceID1)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return !s.hasLoadTaskByWorkerAndSource(workerName1, sourceID1)
	}), IsTrue)

	// source1 transfer to worker3
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		w, ok := s.bounds[sourceID1]
		return ok && w.baseInfo.Name == workerName3
	}), IsTrue)

	c.Assert(s.bounds[sourceID1], DeepEquals, worker3)
	c.Assert(worker1.stage, Equals, WorkerFree)

	// worker4 online
	// source2 transfer to worker4
	c.Assert(s.handleWorkerOnline(ha.WorkerEvent{WorkerName: workerName4}, true), IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		w, ok := s.bounds[sourceID2]
		return ok && w.baseInfo.Name == workerName4
	}), IsTrue)
	c.Assert(s.bounds[sourceID2], DeepEquals, worker4)
	c.Assert(worker2.stage, Equals, WorkerFree)

	// after stop-task, hasLoadTaskByWorkerAndSource is no longer valid
	c.Assert(s.hasLoadTaskByWorkerAndSource(workerName4, sourceID2), IsTrue)
	s.subTaskCfgs.Delete(task2)
	c.Assert(s.hasLoadTaskByWorkerAndSource(workerName4, sourceID2), IsFalse)

	cancel1()
	wg.Wait()
}

func (t *testScheduler) TestWorkerHasDiffRelayAndBound(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger      = log.L()
		s           = NewScheduler(&logger, config.Security{})
		sourceID1   = "mysql-replica-1"
		sourceID2   = "mysql-replica-2"
		workerName1 = "dm-worker-1"
		keepAlive   = int64(3)
	)

	workerInfo := ha.WorkerInfo{Name: workerName1}
	bound := ha.SourceBound{
		Source: sourceID1,
		Worker: workerName1,
	}

	sourceCfg, err := config.LoadFromFile("../source.yaml")
	c.Assert(err, IsNil)
	sourceCfg.Checker.BackoffMax = config.Duration{Duration: 5 * time.Second}

	// prepare etcd data
	s.etcdCli = etcdTestCli
	sourceCfg.SourceID = sourceID1
	_, err = ha.PutSourceCfg(etcdTestCli, sourceCfg)
	c.Assert(err, IsNil)
	sourceCfg.SourceID = sourceID2
	_, err = ha.PutSourceCfg(etcdTestCli, sourceCfg)
	c.Assert(err, IsNil)
	_, err = ha.PutRelayConfig(etcdTestCli, sourceID2, workerName1)
	c.Assert(err, IsNil)
	_, err = ha.PutWorkerInfo(etcdTestCli, workerInfo)
	c.Assert(err, IsNil)
	_, err = ha.PutSourceBound(etcdTestCli, bound)
	c.Assert(err, IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	//nolint:errcheck
	go ha.KeepAlive(ctx, etcdTestCli, workerName1, keepAlive)

	// bootstrap
	c.Assert(s.recoverSources(), IsNil)
	c.Assert(s.recoverRelayConfigs(), IsNil)
	_, err = s.recoverWorkersBounds()
	c.Assert(err, IsNil)

	// check
	c.Assert(s.relayWorkers[sourceID2], HasLen, 1)
	_, ok := s.relayWorkers[sourceID2][workerName1]
	c.Assert(ok, IsTrue)
	worker := s.workers[workerName1]
	c.Assert(worker.Stage(), Equals, WorkerRelay)
	c.Assert(worker.RelaySourceID(), Equals, sourceID2)
	_, ok = s.unbounds[sourceID1]
	c.Assert(ok, IsTrue)
}

func (t *testScheduler) TestUpgradeCauseConflictRelayType(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger      = log.L()
		s           = NewScheduler(&logger, config.Security{})
		sourceID1   = "mysql-replica-1"
		workerName1 = "dm-worker-1"
		workerName2 = "dm-worker-2"
		keepAlive   = int64(3)
	)

	workerInfo1 := ha.WorkerInfo{Name: workerName1}
	workerInfo2 := ha.WorkerInfo{Name: workerName2}
	bound := ha.SourceBound{
		Source: sourceID1,
		Worker: workerName1,
	}

	sourceCfg, err := config.LoadFromFile("../source.yaml")
	c.Assert(err, IsNil)
	sourceCfg.Checker.BackoffMax = config.Duration{Duration: 5 * time.Second}

	// prepare etcd data
	s.etcdCli = etcdTestCli
	sourceCfg.EnableRelay = true
	sourceCfg.SourceID = sourceID1
	_, err = ha.PutSourceCfg(etcdTestCli, sourceCfg)
	c.Assert(err, IsNil)
	_, err = ha.PutRelayConfig(etcdTestCli, sourceID1, workerName1)
	c.Assert(err, IsNil)
	_, err = ha.PutRelayConfig(etcdTestCli, sourceID1, workerName2)
	c.Assert(err, IsNil)
	_, err = ha.PutWorkerInfo(etcdTestCli, workerInfo1)
	c.Assert(err, IsNil)
	_, err = ha.PutWorkerInfo(etcdTestCli, workerInfo2)
	c.Assert(err, IsNil)
	_, err = ha.PutSourceBound(etcdTestCli, bound)
	c.Assert(err, IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	//nolint:errcheck
	go ha.KeepAlive(ctx, etcdTestCli, workerName1, keepAlive)
	//nolint:errcheck
	go ha.KeepAlive(ctx, etcdTestCli, workerName2, keepAlive)

	// bootstrap
	c.Assert(s.recoverSources(), IsNil)
	c.Assert(s.recoverRelayConfigs(), IsNil)
	_, err = s.recoverWorkersBounds()
	c.Assert(err, IsNil)

	// check when the relay config is conflicting with source config, relay config should be deleted
	c.Assert(s.relayWorkers[sourceID1], HasLen, 0)
	result, _, err := ha.GetAllRelayConfig(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(result, HasLen, 0)

	worker := s.workers[workerName1]
	c.Assert(worker.Stage(), Equals, WorkerBound)
	c.Assert(worker.RelaySourceID(), HasLen, 0)
	c.Assert(s.workers[workerName2].Stage(), Equals, WorkerFree)
}
