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
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/master/metrics"
	"github.com/pingcap/tiflow/dm/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

const (
	maxQueryWorkerRetryNum = 10
)

// Scheduler schedules tasks for DM-worker instances, including:
// - register/unregister DM-worker instances.
// - observe the online/offline status of DM-worker instances.
// - observe add/remove operations for upstream sources' config.
// - schedule upstream sources to DM-worker instances.
// - schedule data migration subtask operations.
// - holds agents of DM-worker instances.
// NOTE: the DM-master server MUST wait for this scheduler become started before handling client requests.
// Cases trigger a source-to-worker bound try:
// - a worker from Offline to Free:
//   - receive keep-alive.
// - a worker from Bound to Free:
//   - trigger by unbound: `a source removed`.
// - a new source added:
//   - add source request from user.
// - a source unbound from another worker:
//   - trigger by unbound: `a worker from Bound to Offline`.
//   - TODO(csuzhangxc): design a strategy to ensure the old worker already shutdown its work.
// Cases trigger a source-to-worker unbound try.
// - a worker from Bound to Offline:
//   - lost keep-alive.
// - a source removed:
//   - remove source request from user.
// TODO: try to handle the return `err` of etcd operations,
//   because may put into etcd, but the response to the etcd client interrupted.
// Relay scheduling:
// - scheduled by source
//   DM-worker will enable relay according to its bound source, in current implementation, it will read `enable-relay`
//   of source config and decide whether to enable relay.
//   turn on `enable-relay`:
//   - use `enable-relay: true` when create source
//   - `start-relay -s source` to dynamically change `enable-relay`
//   turn off `enable-relay`:
//   - use `enable-relay: false` when create source
//   - `stop-relay -s source` to dynamically change `enable-relay`
//   - found conflict schedule type with (source, worker) when scheduler bootstrap
// - scheduled by (source, worker)
//   DM-worker will check if relay is assigned to it no matter it's bound or not. In current implementation, it will
//   read UpstreamRelayWorkerKeyAdapter in etcd.
//   add UpstreamRelayWorkerKeyAdapter:
//   - use `start-relay -s source -w worker`
//   remove UpstreamRelayWorkerKeyAdapter:
//   - use `stop-relay -s source -w worker`
//   - remove worker by `offline-member`
type Scheduler struct {
	mu sync.RWMutex

	logger log.Logger

	started atomic.Bool // whether the scheduler already started for work.
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	etcdCli *clientv3.Client

	// must acquire latch from subtaskLatch before accessing subTaskCfgs and expectSubTaskStages,
	// the latch key is task name.
	// TODO: also sourceLatch, relayLatch
	subtaskLatch *latches

	// all source configs, source ID -> source config.
	// add:
	// - add source by user request (calling `AddSourceCfg`).
	// - recover from etcd (calling `recoverSources`).
	// delete:
	// - remove source by user request (calling `RemoveSourceCfg`).
	sourceCfgs map[string]*config.SourceConfig

	// all subtask configs, task name -> source ID -> subtask config.
	// add:
	// - add/start subtask by user request (calling `AddSubTasks`).
	// - recover from etcd (calling `recoverSubTasks`).
	// delete:
	// - remove/stop subtask by user request (calling `RemoveSubTasks`).
	subTaskCfgs sync.Map

	// all DM-workers, worker name -> worker.
	// add:
	// - add worker by user request (calling `AddWorker`).
	// - recover from etcd (calling `recoverWorkersBounds`).
	// delete:
	// - remove worker by user request (calling `RemoveWorker`).
	workers map[string]*Worker

	// all bound relationship, source ID -> worker.
	// add:
	// - when bind a source to a worker, in updateStatusToBound
	// delete:
	// - when unbind a source from a worker, in updateStatusToUnbound
	// see `Cases trigger a source-to-worker bound try` above.
	bounds map[string]*Worker

	// unbound (pending to bound) sources.
	// NOTE: refactor to support scheduling by priority.
	// add:
	// - add source by user request (calling `AddSourceCfg`).
	// - recover from etcd (calling `recoverWorkersBounds`).
	// - when the bounding worker become offline, in updateStatusToUnbound.
	// delete:
	// - remove source by user request (calling `RemoveSourceCfg`).
	// - when bounded the source to a worker, in updateStatusToBound.
	unbounds map[string]struct{}

	// a mirror of bounds whose element is not deleted when worker unbound. worker -> SourceBound
	lastBound map[string]ha.SourceBound

	// expectant relay stages for sources, source ID -> stage.
	// add:
	// - bound the source to a worker (at first time).
	// - recover from etcd (calling `recoverSources`).
	// update:
	// - update stage by user request (calling `UpdateExpectRelayStage`).
	// delete:
	// - remove source by user request (calling `RemoveSourceCfg`).
	expectRelayStages map[string]ha.Stage

	// expectant subtask stages for tasks & sources, task name -> source ID -> stage.
	// add:
	// - add/start subtask by user request (calling `AddSubTasks`).
	// - recover from etcd (calling `recoverSubTasks`).
	// update:
	// - update stage by user request (calling `UpdateExpectSubTaskStage`).
	// delete:
	// - remove/stop subtask by user request (calling `RemoveSubTasks`).
	expectSubTaskStages sync.Map

	// a source has its relay workers. source-id -> set(worker-name)
	// add:
	// - start-relay
	// - recover from etcd (calling `recoverRelayConfigs`)
	// delete:
	// - stop-relay
	relayWorkers map[string]map[string]struct{}

	// expectant validator stages, task name -> source ID -> stage.
	// add:
	// - on subtask start with validator mode not none
	// - start validator manually
	// - recover from etcd
	// update
	// - update stage by user request
	// delete:
	// - when subtask is removed by user request
	expectValidatorStages sync.Map

	// workers in load stage
	// task -> source -> worker
	loadTasks map[string]map[string]string

	securityCfg config.Security
}

// NewScheduler creates a new scheduler instance.
func NewScheduler(pLogger *log.Logger, securityCfg config.Security) *Scheduler {
	return &Scheduler{
		logger:            pLogger.WithFields(zap.String("component", "scheduler")),
		subtaskLatch:      newLatches(),
		sourceCfgs:        make(map[string]*config.SourceConfig),
		workers:           make(map[string]*Worker),
		bounds:            make(map[string]*Worker),
		unbounds:          make(map[string]struct{}),
		lastBound:         make(map[string]ha.SourceBound),
		expectRelayStages: make(map[string]ha.Stage),
		relayWorkers:      make(map[string]map[string]struct{}),
		loadTasks:         make(map[string]map[string]string),
		securityCfg:       securityCfg,
	}
}

// Start starts the scheduler for work.
// NOTE: for logic errors, it should start without returning errors (but report via metrics or log) so that the user can fix them.
func (s *Scheduler) Start(pCtx context.Context, etcdCli *clientv3.Client) (err error) {
	s.logger.Info("the scheduler is starting")

	s.mu.Lock()
	defer func() {
		if err != nil {
			s.CloseAllWorkers()
		}
		s.mu.Unlock()
	}()

	if s.started.Load() {
		return terror.ErrSchedulerStarted.Generate()
	}

	s.etcdCli = etcdCli // set s.etcdCli first for safety, observeWorkerEvent will use s.etcdCli in retry
	s.reset()           // reset previous status.

	// recover previous status from etcd.
	err = s.recoverSources()
	if err != nil {
		return err
	}
	err = s.recoverSubTasks()
	if err != nil {
		return err
	}
	err = s.recoverRelayConfigs()
	if err != nil {
		return err
	}

	var loadTaskRev int64
	loadTaskRev, err = s.recoverLoadTasks(false)
	if err != nil {
		return err
	}

	var rev int64
	rev, err = s.recoverWorkersBounds()
	if err != nil {
		return err
	}

	// check if we can bind free or relay source and workers
	for _, w := range s.workers {
		if w.stage == WorkerFree || w.stage == WorkerRelay {
			bound, err := s.tryBoundForWorker(w)
			if err != nil {
				return err
			}
			if !bound {
				break
			}
		}
	}

	ctx, cancel := context.WithCancel(pCtx)

	s.wg.Add(1)
	go func(rev1 int64) {
		defer s.wg.Done()
		// starting to observe status of DM-worker instances.
		// TODO: handle fatal error from observeWorkerEvent
		//nolint:errcheck
		s.observeWorkerEvent(ctx, rev1)
	}(rev)

	s.wg.Add(1)
	go func(rev1 int64) {
		defer s.wg.Done()
		// starting to observe load task.
		// TODO: handle fatal error from observeLoadTask
		//nolint:errcheck
		s.observeLoadTask(ctx, rev1)
	}(loadTaskRev)

	s.started.Store(true) // started now
	s.cancel = cancel
	s.logger.Info("the scheduler has started")
	return nil
}

// Close closes the scheduler.
func (s *Scheduler) Close() {
	s.mu.Lock()

	if !s.started.Load() {
		s.mu.Unlock()
		return
	}

	s.logger.Info("the scheduler is closing")
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	s.CloseAllWorkers()
	s.mu.Unlock()

	// need to wait for goroutines to return which may hold the mutex.
	s.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.started.Store(false) // closed now.
	s.logger.Info("the scheduler has closed")
}

// CloseAllWorkers closes all the scheduler's workers.
func (s *Scheduler) CloseAllWorkers() {
	for _, worker := range s.workers {
		worker.Close()
	}
}

// AddSourceCfg adds the upstream source config to the cluster, and try to bound source to worker
// NOTE: please verify the config before call this.
func (s *Scheduler) AddSourceCfg(cfg *config.SourceConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	err := s.addSource(cfg)
	if err != nil {
		return err
	}

	// try to bound it to a Free worker.
	_, err = s.tryBoundForSource(cfg.SourceID)
	return err
}

// AddSourceCfgWithWorker adds the upstream source config to the cluster, and try to bound source to specify worker
// NOTE: please verify the config before call this.
func (s *Scheduler) AddSourceCfgWithWorker(cfg *config.SourceConfig, workerName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	// check whether worker exists.
	w, ok := s.workers[workerName]
	if !ok {
		return terror.ErrSchedulerWorkerNotExist.Generate(workerName)
	}

	if w.stage != WorkerFree {
		return terror.ErrSchedulerWorkerNotFree.Generate(workerName)
	}

	if err := s.addSource(cfg); err != nil {
		return err
	}

	return s.boundSourceToWorker(cfg.SourceID, w)
}

// addSource adds the upstream source config to the cluster.
func (s *Scheduler) addSource(cfg *config.SourceConfig) error {
	// 1. check whether exists.
	if _, ok := s.sourceCfgs[cfg.SourceID]; ok {
		return terror.ErrSchedulerSourceCfgExist.Generate(cfg.SourceID)
	}
	// 2. put the config into etcd.
	_, err := ha.PutSourceCfg(s.etcdCli, cfg)
	if err != nil {
		return err
	}

	// 3. record the config in the scheduler.
	s.sourceCfgs[cfg.SourceID] = cfg
	s.unbounds[cfg.SourceID] = struct{}{}
	return nil
}

// UpdateSourceCfg update the upstream source config to the cluster.
func (s *Scheduler) UpdateSourceCfg(cfg *config.SourceConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	// 1. check whether the config exists.
	_, ok := s.sourceCfgs[cfg.SourceID]
	if !ok {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(cfg.SourceID)
	}
	// 2. check if tasks using this configuration are running
	runningStage := pb.Stage_Running
	if tasks := s.GetTaskNameListBySourceName(cfg.SourceID, &runningStage); len(tasks) > 0 {
		return terror.ErrSchedulerSourceCfgUpdate.Generate(cfg.SourceID)
	}
	// 3. check if this source is enable relay
	if _, ok := s.expectRelayStages[cfg.SourceID]; ok {
		return terror.ErrSchedulerSourceCfgUpdate.Generate(cfg.SourceID)
	}
	// 4. put the config into etcd.
	_, err := ha.PutSourceCfg(s.etcdCli, cfg)
	if err != nil {
		return err
	}
	// 5. record the config in the scheduler.
	s.sourceCfgs[cfg.SourceID] = cfg
	return nil
}

// RemoveSourceCfg removes the upstream source config in the cluster.
// when removing the upstream source config, it should also remove:
// - any existing relay stage.
// - any source-worker bound relationship.
func (s *Scheduler) RemoveSourceCfg(source string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	// 1. check whether the config exists.
	if _, ok := s.sourceCfgs[source]; !ok {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(source)
	}

	// 2. check whether any subtask or relay config exists for the source.
	existingSubtasksM := make(map[string]struct{})
	s.subTaskCfgs.Range(func(k, v interface{}) bool {
		task := k.(string)
		cfg := v.(map[string]config.SubTaskConfig)
		for source2 := range cfg {
			if source2 == source {
				existingSubtasksM[task] = struct{}{}
			}
		}
		return true
	})

	existingSubtasks := strMapToSlice(existingSubtasksM)
	if len(existingSubtasks) > 0 {
		return terror.ErrSchedulerSourceOpTaskExist.Generate(source, existingSubtasks)
	}
	relayWorkers := s.relayWorkers[source]
	if len(relayWorkers) != 0 {
		return terror.ErrSchedulerSourceOpRelayExist.Generate(source, strMapToSlice(relayWorkers))
	}

	// 3. find worker name by source ID.
	var (
		workerName string // empty should be fine below.
		worker     *Worker
	)
	if w, ok2 := s.bounds[source]; ok2 {
		worker = w
		workerName = w.BaseInfo().Name
	}

	// 4. delete the info in etcd.
	_, err := ha.DeleteSourceCfgRelayStageSourceBound(s.etcdCli, source, workerName)
	if err != nil {
		return err
	}

	// 5. delete the config and expectant stage in the scheduler
	delete(s.sourceCfgs, source)
	delete(s.expectRelayStages, source)

	// 6. unbound for the source.
	s.updateStatusToUnbound(source)

	// 7. remove it from unbounds.
	delete(s.unbounds, source)

	// 8. try to bound the worker for another source.
	if worker != nil {
		_, err = s.tryBoundForWorker(worker)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetSourceCfgs gets all source cfgs, return nil when error happens.
func (s *Scheduler) GetSourceCfgs() map[string]*config.SourceConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	clone := make(map[string]*config.SourceConfig, len(s.sourceCfgs))
	for sourceID, sourceCfg := range s.sourceCfgs {
		cloneCfg := sourceCfg.Clone()
		clone[sourceID] = cloneCfg
	}
	return clone
}

// GetSourceCfgIDs gets all added source ID.
func (s *Scheduler) GetSourceCfgIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	id := make([]string, 0, len(s.sourceCfgs))
	for i := range s.sourceCfgs {
		id = append(id, i)
	}
	return id
}

// GetSourceCfgByID gets source config by source ID.
func (s *Scheduler) GetSourceCfgByID(source string) *config.SourceConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg, ok := s.sourceCfgs[source]
	if !ok {
		return nil
	}
	clone := *cfg
	return &clone
}

// transferWorkerAndSource swaps two sources between two workers (maybe empty). The input means before invocation of
// this function, left worker and left source are bound, right worker and right source are bound. After this function,
// left worker should be bound to right source and vice versa.
// lworker, "", "", rsource				This means an unbounded source bounded to a free worker
// lworker, lsource, rworker, "" 		This means transfer a source from a worker to another free worker
// lworker, lsource, "", rsource		This means transfer a worker from a bounded source to another unbounded source
// lworker, lsource, rworker, rsource	This means transfer two bounded relations.
func (s *Scheduler) transferWorkerAndSource(lworker, lsource, rworker, rsource string) error {
	// in first four arrays, index 0 is for left worker, index 1 is for right worker
	var (
		inputWorkers [2]string
		inputSources [2]string
		workers      [2]*Worker
		bounds       [2]ha.SourceBound
		boundWorkers []string
		boundsToPut  []ha.SourceBound
		ok           bool
	)

	s.logger.Info("transfer source and worker", zap.String("left worker", lworker), zap.String("left source", lsource), zap.String("right worker", rworker), zap.String("right source", rsource))

	inputWorkers[0], inputWorkers[1] = lworker, rworker
	inputSources[0], inputSources[1] = lsource, rsource

	for i, workerName := range inputWorkers {
		if workerName != "" {
			workers[i], ok = s.workers[workerName]
			// should not happen, avoid panic
			if !ok {
				s.logger.Error("could not found worker in scheduler", zap.String("worker", workerName))
				return terror.ErrSchedulerWorkerNotExist.Generate(workerName)
			}
		}
	}

	// check if the swap is valid, to avoid we messing up metadata in etcd.
	for i := range inputWorkers {
		if inputWorkers[i] != "" {
			got := workers[i].bound.Source
			expect := inputSources[i]
			if got != expect {
				return terror.ErrSchedulerWrongWorkerInput.Generate(inputWorkers[i], expect, got)
			}

			// if the worker has started-relay for a source, it can't be bound to another source.
			relaySource := workers[i].RelaySourceID()
			another := i ^ 1 // make use of XOR to flip 0 and 1
			toBindSource := inputSources[another]
			if relaySource != "" && toBindSource != "" && relaySource != toBindSource {
				return terror.ErrSchedulerBoundDiffWithStartedRelay.Generate(inputWorkers[i], toBindSource, relaySource)
			}
		}
	}

	// get current bound workers.
	for i := range inputWorkers {
		if inputWorkers[i] != "" && inputSources[i] != "" {
			boundWorkers = append(boundWorkers, inputWorkers[i])
		}
	}

	// del current bound relations.
	if _, err := ha.DeleteSourceBound(s.etcdCli, boundWorkers...); err != nil {
		return err
	}

	// update unbound sources
	for _, sourceID := range inputSources {
		if sourceID != "" {
			s.updateStatusToUnbound(sourceID)
		}
	}

	// put new bound relations.
	// TODO: move this and above DeleteSourceBound in one txn.
	for i := range inputWorkers {
		another := i ^ 1 // make use of XOR to flip 0 and 1
		if inputWorkers[i] != "" && inputSources[another] != "" {
			b := ha.NewSourceBound(inputSources[another], inputWorkers[i])
			bounds[i] = b
			boundsToPut = append(boundsToPut, b)
		}
	}
	if _, err := ha.PutSourceBound(s.etcdCli, boundsToPut...); err != nil {
		return err
	}

	// update bound sources and workers
	for i := range inputWorkers {
		another := i ^ 1 // make use of XOR to flip 0 and 1
		if inputWorkers[i] != "" && inputSources[another] != "" {
			err := s.updateStatusToBound(workers[i], bounds[i])
			// TODO: if we failed here, etcd has been modified!! we should try this memory check then modify persistent data
			// and revert if failed
			if err != nil {
				s.logger.DPanic("failed to update status to bound, but has written etcd", zap.Error(err))
			}
		}
	}

	// if one of the workers/sources become free/unbounded
	// try bound it.
	for i := range inputWorkers {
		another := i ^ 1 // make use of XOR to flip 0 and 1
		if inputWorkers[i] != "" && inputSources[another] == "" {
			if _, err := s.tryBoundForWorker(workers[i]); err != nil {
				return err
			}
		}
	}
	for i := range inputSources {
		another := i ^ 1 // make use of XOR to flip 0 and 1
		if inputSources[i] != "" && inputWorkers[another] == "" {
			if _, err := s.tryBoundForSource(inputSources[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

// TransferSource unbinds the `source` and binds it to a free or same-source-relay `worker`.
// If fails halfway, the old worker should try recover.
func (s *Scheduler) TransferSource(ctx context.Context, source, worker string) error {
	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}
	s.mu.RLock()
	// 1. check existence or no need
	if _, ok := s.sourceCfgs[source]; !ok {
		s.mu.RUnlock()
		return terror.ErrSchedulerSourceCfgNotExist.Generate(source)
	}
	w, ok := s.workers[worker]
	if !ok {
		s.mu.RUnlock()
		return terror.ErrSchedulerWorkerNotExist.Generate(worker)
	}
	oldWorker, hasOldWorker := s.bounds[source]
	if hasOldWorker && oldWorker.BaseInfo().Name == worker {
		s.mu.RUnlock()
		return nil
	}
	s.mu.RUnlock()

	// 2. check new worker is free and not started relay for another source
	switch w.Stage() {
	case WorkerOffline, WorkerBound:
		return terror.ErrSchedulerWorkerInvalidTrans.Generate(worker, w.Stage(), WorkerBound)
	case WorkerFree:
	case WorkerRelay:
		if relaySource := w.RelaySourceID(); relaySource != source {
			return terror.ErrSchedulerBoundDiffWithStartedRelay.Generate(worker, source, relaySource)
		}
	}

	// 3. if no old worker, bound it directly
	if !hasOldWorker {
		s.logger.Warn("in transfer source, found a free worker and not bound source, which should not happened",
			zap.String("source", source),
			zap.String("worker", worker))
		return s.boundSourceToWorker(source, w)
	}

	// 4. check if old worker has running tasks
	runningStage := pb.Stage_Running
	if runningTasks := s.GetTaskNameListBySourceName(source, &runningStage); len(runningTasks) > 0 {
		// we only allow automatically transfer-source if all subtasks are in the sync phase.
		resp, err := oldWorker.queryStatus(ctx)
		if err != nil {
			return terror.Annotatef(err, "failed to query worker: %s status err", oldWorker.baseInfo.Name)
		}
		for _, status := range resp.QueryStatus.GetSubTaskStatus() {
			if status.GetUnit() != pb.UnitType_Sync {
				return terror.ErrSchedulerRequireRunningTaskInSyncUnit.Generate(runningTasks, source)
			}
		}
		// pause running tasks
		if batchPauseErr := s.BatchOperateTaskOnWorker(ctx, oldWorker, runningTasks, source, pb.Stage_Paused, true); batchPauseErr != nil {
			return batchPauseErr
		}
		// we need resume tasks that we just paused, we use another goroutine to do this because if error happens
		// just logging this message and let user handle it manually
		defer func() {
			go func() {
				if err := s.BatchOperateTaskOnWorker(context.Background(), w, runningTasks, source, pb.Stage_Running, false); err != nil {
					s.logger.Warn(
						"auto resume task failed", zap.Any("tasks", runningTasks),
						zap.String("source", source), zap.String("worker", worker), zap.Error(err))
				}
			}()
		}()
	}

	// 5. replace the source bound
	failpoint.Inject("failToReplaceSourceBound", func(_ failpoint.Value) {
		failpoint.Return(errors.New("failToPutSourceBound"))
	})
	s.mu.Lock()
	_, err := ha.ReplaceSourceBound(s.etcdCli, source, oldWorker.BaseInfo().Name, worker)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if err2 := oldWorker.Unbound(); err2 != nil {
		s.logger.DPanic("the oldWorker is get from s.bound, so there should not be an error", zap.Error(err2))
	}
	if err2 := s.updateStatusToBound(w, ha.NewSourceBound(source, worker)); err2 != nil {
		s.logger.DPanic("we have checked w.stage is free, so there should not be an error", zap.Error(err2))
	}
	// 6. now this old worker is free, try bound source to it
	_, err = s.tryBoundForWorker(oldWorker)
	if err != nil {
		s.logger.Warn("in transfer source, error when try bound the old worker", zap.Error(err))
	}
	s.mu.Unlock()
	return nil
}

// BatchOperateTaskOnWorker batch operate tasks in one worker and use query-status to make sure all tasks are in expected stage if needWait=true.
func (s *Scheduler) BatchOperateTaskOnWorker(
	ctx context.Context, worker *Worker, tasks []string, source string, stage pb.Stage, needWait bool,
) error {
	if len(tasks) == 0 {
		return nil
	}
	for _, taskName := range tasks {
		if err := s.UpdateExpectSubTaskStage(stage, taskName, source); err != nil {
			return err
		}
	}
	if !needWait {
		return nil
	}
	// wait all tasks are in expected stage before actually starting scheduling
WaitLoop:
	for retry := 0; retry < maxQueryWorkerRetryNum; retry++ {
		resp, err := worker.queryStatus(ctx)
		if err != nil {
			return terror.Annotatef(err, "failed to query worker: %s status", worker.baseInfo.Name)
		}

		failpoint.Inject("batchOperateTaskOnWorkerMustRetry", func(v failpoint.Value) {
			if retry < v.(int) {
				resp.QueryStatus.SubTaskStatus[0].Stage = pb.Stage_InvalidStage
				log.L().Info("batchOperateTaskOnWorkerMustRetry failpoint triggered", zap.Int("retry", retry))
			} else {
				log.L().Info("batchOperateTaskOnWorkerMustRetry passed", zap.Int("retry", retry))
			}
		})

		for _, status := range resp.QueryStatus.GetSubTaskStatus() {
			if status == nil {
				// this should not happen when rpc logic in server side not changed
				return errors.Errorf("expect a query-status with subtask status but got a nil, resp %v", resp)
			}
			if status.Stage != stage {
				// NOTE: the defaultRPCTimeout is 10m, use 1s * retry times to increase the waiting time
				sleepTime := time.Second * time.Duration(maxQueryWorkerRetryNum-retry)
				s.logger.Info(
					"waiting task",
					zap.String("task", status.Name),
					zap.Int("retry times", retry),
					zap.Duration("sleep time", sleepTime),
					zap.String("want stage", stage.String()),
					zap.String("current stage", status.Stage.String()),
				)
				failpoint.Inject("skipBatchOperateTaskOnWorkerSleep", func(_ failpoint.Value) {
					failpoint.Continue("WaitLoop")
				})
				select {
				case <-ctx.Done():
					return terror.Annotatef(err, "failed to wait task on worker: %s because context is canceled", worker.baseInfo.Name)
				case <-time.After(sleepTime):
					continue WaitLoop
				}
			}
		}
		return nil // all task are in expected stage
	}
	return terror.ErrSchedulerPauseTaskForTransferSource.Generate(tasks) // failed to pause tasks, need user to handle it manually
}

// AcquireSubtaskLatch tries acquiring a latch for subtask name.
func (s *Scheduler) AcquireSubtaskLatch(name string) (ReleaseFunc, error) {
	return s.subtaskLatch.tryAcquire(name)
}

// AddSubTasks adds the information of one or more subtasks for one task.
// use s.mu.RLock() to protect s.bound, and s.subtaskLatch to protect subtask related members.
// setting `latched` to true means caller has acquired latch.
func (s *Scheduler) AddSubTasks(latched bool, expectStage pb.Stage, cfgs ...config.SubTaskConfig) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	if len(cfgs) == 0 {
		return nil // no subtasks need to add, this should not happen.
	}

	var (
		taskNamesM    = make(map[string]struct{}, 1)
		existSourcesM = make(map[string]struct{}, len(cfgs))
	)

	for _, cfg := range cfgs {
		taskNamesM[cfg.Name] = struct{}{}
	}
	taskNames := strMapToSlice(taskNamesM)
	if len(taskNames) > 1 {
		// only subtasks from one task supported now.
		return terror.ErrSchedulerMultiTask.Generate(taskNames)
	}

	if !latched {
		release, err := s.subtaskLatch.tryAcquire(taskNames[0])
		if err != nil {
			return terror.ErrSchedulerLatchInUse.Generate("AddSubTasks", taskNames[0])
		}
		defer release()
	}

	// 1. check whether exists.
	for _, cfg := range cfgs {
		v, ok := s.subTaskCfgs.Load(cfg.Name)
		if !ok {
			continue
		}
		cfgM := v.(map[string]config.SubTaskConfig)
		_, ok = cfgM[cfg.SourceID]
		if !ok {
			continue
		}
		existSourcesM[cfg.SourceID] = struct{}{}
	}

	existSources := strMapToSlice(existSourcesM)
	switch {
	case len(existSources) == len(cfgs):
		// all subtasks already exist, return an error.
		return terror.ErrSchedulerSubTaskExist.Generate(taskNames[0], existSources)
	case len(existSources) > 0:
		// some subtasks already exists, log a warn.
		s.logger.Warn("some subtasks already exist", zap.String("task", taskNames[0]), zap.Strings("sources", existSources))
	}

	// 2. construct `Running` stages when adding.
	newCfgs := make([]config.SubTaskConfig, 0, len(cfgs)-len(existSources))
	newStages := make([]ha.Stage, 0, cap(newCfgs))
	validatorStages := make([]ha.Stage, 0, cap(newCfgs))
	unbounds := make([]string, 0)
	for _, cfg := range cfgs {
		if _, ok := existSourcesM[cfg.SourceID]; ok {
			continue
		}
		newCfgs = append(newCfgs, cfg)
		newStages = append(newStages, ha.NewSubTaskStage(expectStage, cfg.SourceID, cfg.Name))
		if cfg.ValidatorCfg.Mode != config.ValidationNone {
			validatorStages = append(validatorStages, ha.NewValidatorStage(pb.Stage_Running, cfg.SourceID, cfg.Name))
		}
		if _, ok := s.bounds[cfg.SourceID]; !ok {
			unbounds = append(unbounds, cfg.SourceID)
		}
	}

	// 3. check whether any sources unbound.
	if len(unbounds) > 0 {
		return terror.ErrSchedulerSourcesUnbound.Generate(unbounds)
	}

	// 4. put the configs and stages into etcd.
	_, err := ha.PutSubTaskCfgStage(s.etcdCli, newCfgs, newStages, validatorStages)
	if err != nil {
		return err
	}

	// 5. record the config and the expectant stage.
	for _, cfg := range newCfgs {
		v, _ := s.subTaskCfgs.LoadOrStore(cfg.Name, map[string]config.SubTaskConfig{})
		m := v.(map[string]config.SubTaskConfig)
		m[cfg.SourceID] = cfg
	}
	for _, stage := range newStages {
		v, _ := s.expectSubTaskStages.LoadOrStore(stage.Task, map[string]ha.Stage{})
		m := v.(map[string]ha.Stage)
		m[stage.Source] = stage
	}
	for _, stage := range validatorStages {
		v, _ := s.expectValidatorStages.LoadOrStore(stage.Task, map[string]ha.Stage{})
		m := v.(map[string]ha.Stage)
		m[stage.Source] = stage
	}

	return nil
}

// RemoveSubTasks removes the information of one or more subtasks for one task.
func (s *Scheduler) RemoveSubTasks(task string, sources ...string) error {
	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	if task == "" || len(sources) == 0 {
		return nil // no subtask need to stop, this should not happen.
	}

	release, err := s.subtaskLatch.tryAcquire(task)
	if err != nil {
		return terror.ErrSchedulerLatchInUse.Generate("RemoveSubTasks", task)
	}
	defer release()

	// 1. check the task exists.
	stagesMapV, ok1 := s.expectSubTaskStages.Load(task)
	cfgsMapV, ok2 := s.subTaskCfgs.Load(task)
	if !ok1 || !ok2 {
		return terror.ErrSchedulerSubTaskOpTaskNotExist.Generate(task)
	}

	var validatorStageM map[string]ha.Stage
	if validatorStageV, ok := s.expectValidatorStages.Load(task); ok {
		validatorStageM = validatorStageV.(map[string]ha.Stage)
	}

	var (
		stagesM          = stagesMapV.(map[string]ha.Stage)
		cfgsM            = cfgsMapV.(map[string]config.SubTaskConfig)
		notExistSourcesM = make(map[string]struct{})
		stages           = make([]ha.Stage, 0, len(sources))
		validatorStages  = make([]ha.Stage, 0, len(sources))
		cfgs             = make([]config.SubTaskConfig, 0, len(sources))
	)
	for _, source := range sources {
		if stage, ok := stagesM[source]; !ok {
			notExistSourcesM[source] = struct{}{}
		} else {
			stages = append(stages, stage)
		}
		if stage, ok := validatorStageM[source]; ok {
			validatorStages = append(validatorStages, stage)
		}
		if cfg, ok := cfgsM[source]; ok {
			cfgs = append(cfgs, cfg)
		}
	}
	notExistSources := strMapToSlice(notExistSourcesM)
	if len(notExistSources) > 0 {
		// some sources not exist, reject the request.
		return terror.ErrSchedulerSubTaskOpSourceNotExist.Generate(notExistSources)
	}

	// 2. delete the configs and the stages.
	_, err = ha.DeleteSubTaskCfgStage(s.etcdCli, cfgs, stages, validatorStages)
	if err != nil {
		return err
	}

	// 3. clear the config and the expectant stage.
	for _, cfg := range cfgs {
		delete(cfgsM, cfg.SourceID)
	}
	if len(cfgsM) == 0 {
		s.subTaskCfgs.Delete(task)
	}
	for _, stage := range stages {
		delete(stagesM, stage.Source)
	}
	if len(stagesM) == 0 {
		s.expectSubTaskStages.Delete(task)
	}
	for _, stage := range validatorStages {
		delete(validatorStageM, stage.Source)
	}
	if len(validatorStageM) == 0 {
		s.expectValidatorStages.Delete(task)
	}

	return nil
}

// UpdateSubTasks update the information of one or more subtasks for one task.
func (s *Scheduler) UpdateSubTasks(ctx context.Context, cfgs ...config.SubTaskConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}
	if len(cfgs) == 0 {
		return nil // no subtasks need to add, this should not happen.
	}
	taskNamesM := make(map[string]struct{}, 1)
	for _, cfg := range cfgs {
		taskNamesM[cfg.Name] = struct{}{}
	}
	if len(taskNamesM) > 1 {
		// only subtasks from one task supported now.
		return terror.ErrSchedulerMultiTask.Generate(strMapToSlice(taskNamesM))
	}
	// check whether exists.
	cfg := cfgs[0]
	v, ok := s.subTaskCfgs.Load(cfg.Name)
	if !ok {
		return terror.ErrSchedulerTaskNotExist.Generate(cfg.Name)
	}
	cfgM := v.(map[string]config.SubTaskConfig)
	for _, cfg := range cfgs {
		_, ok = cfgM[cfg.SourceID]
		if !ok {
			return terror.ErrSchedulerSubTaskNotExist.Generate(cfg.Name, cfg.SourceID)
		}
	}
	// check whether in running stage
	stage := s.GetExpectSubTaskStage(cfg.Name, cfg.SourceID)
	if stage.Expect == pb.Stage_Running {
		return terror.ErrSchedulerSubTaskCfgUpdate.Generate(cfg.Name, cfg.SourceID)
	}

	// check by workers todo batch
	for _, cfg := range cfgs {
		worker := s.bounds[cfg.SourceID]
		if worker == nil {
			return terror.ErrSchedulerSubTaskCfgUpdate.Generatef("this source: %s have not bound to worker", cfg.SourceID)
		}
		resp, err := worker.checkSubtasksCanUpdate(ctx, &cfg)
		if err != nil {
			return err
		}
		if !resp.CheckSubtasksCanUpdate.Success {
			return terror.ErrSchedulerSubTaskCfgUpdate.Generatef("can not update because %s", resp.CheckSubtasksCanUpdate.Msg)
		}
	}
	// put the configs and stages into etcd.
	_, err := ha.PutSubTaskCfgStage(s.etcdCli, cfgs, []ha.Stage{}, []ha.Stage{})
	if err != nil {
		return err
	}
	// record the config
	for _, cfg := range cfgs {
		v, _ := s.subTaskCfgs.LoadOrStore(cfg.Name, map[string]config.SubTaskConfig{})
		m := v.(map[string]config.SubTaskConfig)
		m[cfg.SourceID] = cfg
	}
	return nil
}

// getSubTaskCfgByTaskSource gets subtask config by task name and source ID. Only used in tests.
func (s *Scheduler) getSubTaskCfgByTaskSource(task, source string) *config.SubTaskConfig {
	v, ok := s.subTaskCfgs.Load(task)
	if !ok {
		return nil
	}

	cfgM := v.(map[string]config.SubTaskConfig)
	cfg, ok := cfgM[source]
	if !ok {
		return nil
	}
	clone := cfg
	return &clone
}

// GetDownstreamMetaByTask gets downstream db config and meta config by task name.
func (s *Scheduler) GetDownstreamMetaByTask(task string) (*config.DBConfig, string) {
	v, ok := s.subTaskCfgs.Load(task)
	if !ok {
		return nil, ""
	}
	cfgM := v.(map[string]config.SubTaskConfig)
	for _, cfg := range cfgM {
		return cfg.To.Clone(), cfg.MetaSchema
	}
	return nil, ""
}

// GetSubTaskCfgsByTask gets subtask configs' map by task name.
func (s *Scheduler) GetSubTaskCfgsByTask(task string) map[string]*config.SubTaskConfig {
	v, ok := s.subTaskCfgs.Load(task)
	if !ok {
		return nil
	}

	cfgM := v.(map[string]config.SubTaskConfig)
	cloneM := make(map[string]*config.SubTaskConfig, len(cfgM))
	for source, cfg := range cfgM {
		clone := cfg
		cloneM[source] = &clone
	}
	return cloneM
}

func (s *Scheduler) GetSubTaskCfgsByTaskAndSource(taskName string, sources []string) map[string]map[string]config.SubTaskConfig {
	var ret map[string]map[string]config.SubTaskConfig // task-name->sourceID->*config.SubTaskConfig
	if len(taskName) == 0 {
		ret = s.GetSubTaskCfgs()
	} else {
		// get subtask by name
		ret = map[string]map[string]config.SubTaskConfig{}
		tmp := s.GetSubTaskCfgsByTask(taskName)
		if tmp == nil {
			// no subtask matches the `task-name`
			return ret
		}
		ret[taskName] = map[string]config.SubTaskConfig{}
		for source, cfg := range tmp {
			ret[taskName][source] = *cfg
		}
	}
	// filter the source that we don't want
	if len(sources) > 0 {
		filterSource := map[string]interface{}{}
		for _, source := range sources {
			filterSource[source] = true // the source we want
		}
		for taskName, sourceCfgs := range ret {
			for source := range sourceCfgs {
				if _, ok := filterSource[source]; !ok {
					delete(sourceCfgs, source)
				}
			}
			if len(ret[taskName]) == 0 {
				delete(ret, taskName)
			}
		}
	}
	return ret
}

// GetSubTaskCfgs gets all subconfig, return nil when error happens.
func (s *Scheduler) GetSubTaskCfgs() map[string]map[string]config.SubTaskConfig {
	// taskName -> sourceName -> SubTaskConfig
	clone := make(map[string]map[string]config.SubTaskConfig)
	s.subTaskCfgs.Range(func(k, v interface{}) bool {
		task := k.(string)
		m := v.(map[string]config.SubTaskConfig)
		clone2 := make(map[string]config.SubTaskConfig, len(m))
		for source, cfg := range m {
			cfg2, err := cfg.Clone()
			if err != nil {
				return true
			}
			clone2[source] = *cfg2
		}
		clone[task] = clone2
		return true
	})
	return clone
}

// GetSubTaskCfgs gets all subTask config pointer, return nil when error happens.
func (s *Scheduler) GetALlSubTaskCfgs() map[string]map[string]*config.SubTaskConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// taskName -> sourceName -> SubTaskConfig
	clone := make(map[string]map[string]*config.SubTaskConfig)
	s.subTaskCfgs.Range(func(k, v interface{}) bool {
		task := k.(string)
		m := v.(map[string]config.SubTaskConfig)
		clone2 := make(map[string]*config.SubTaskConfig, len(m))
		for source, cfg := range m {
			cfg2, err := cfg.Clone()
			if err != nil {
				return true
			}
			clone2[source] = cfg2
		}
		clone[task] = clone2
		return true
	})
	return clone
}

// GetTaskNameListBySourceName gets task name list by source name.
func (s *Scheduler) GetTaskNameListBySourceName(sourceName string, expectStage *pb.Stage) []string {
	var taskNameList []string
	s.expectSubTaskStages.Range(func(k, v interface{}) bool {
		subtaskM := v.(map[string]ha.Stage)
		subtaskStage, ok2 := subtaskM[sourceName]
		if !ok2 {
			return true
		}
		task := k.(string)
		if expectStage == nil {
			taskNameList = append(taskNameList, task)
		} else if subtaskStage.Expect == *expectStage {
			taskNameList = append(taskNameList, task)
		}
		return true
	})
	return taskNameList
}

// AddWorker adds the information of the DM-worker when registering a new instance.
// This only adds the information of the DM-worker,
// in order to know whether it's online (ready to handle works),
// we need to wait for its healthy status through keep-alive.
func (s *Scheduler) AddWorker(name, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	// 1. check whether exists.
	if w, ok := s.workers[name]; ok {
		// NOTE: we do not support add the worker with different address now, support if needed later.
		// but we support add the worker with all the same information multiple times, and only the first one take effect,
		// because this is needed when restarting the worker.
		if addr == w.BaseInfo().Addr {
			s.logger.Warn("add the same worker again", zap.Stringer("worker info", w.BaseInfo()))
			return nil
		}
		return terror.ErrSchedulerWorkerExist.Generate(w.BaseInfo())
	}

	// 2. put the base info into etcd.
	info := ha.NewWorkerInfo(name, addr)
	_, err := ha.PutWorkerInfo(s.etcdCli, info)
	if err != nil {
		return err
	}

	// generate an agent of DM-worker (with Offline stage) and keep it in the scheduler.
	_, err = s.recordWorker(info)
	return err
}

// RemoveWorker removes the information of the DM-worker when removing the instance manually.
// The user should shutdown the DM-worker instance before removing its information.
func (s *Scheduler) RemoveWorker(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	w, ok := s.workers[name]
	if !ok {
		return terror.ErrSchedulerWorkerNotExist.Generate(name)
	} else if w.Stage() != WorkerOffline {
		return terror.ErrSchedulerWorkerOnline.Generate(name)
	}

	// delete the info in etcd.
	_, err := ha.DeleteWorkerInfoRelayConfig(s.etcdCli, name)
	if err != nil {
		return err
	}
	s.deleteWorker(name)
	return nil
}

// GetAllWorkers gets all worker agent.
func (s *Scheduler) GetAllWorkers() ([]*Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started.Load() {
		return nil, terror.ErrSchedulerNotStarted.Generate()
	}

	workers := make([]*Worker, 0, len(s.workers))
	for _, value := range s.workers {
		workers = append(workers, value)
	}
	return workers, nil
}

// GetWorkerByName gets worker agent by worker name.
func (s *Scheduler) GetWorkerByName(name string) *Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.workers[name]
}

// GetWorkerBySource gets the current bound worker agent by source ID,
// returns nil if the source not bound.
func (s *Scheduler) GetWorkerBySource(source string) *Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bounds[source]
}

// BoundSources returns all bound source IDs in increasing order.
func (s *Scheduler) BoundSources() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	IDs := make([]string, 0, len(s.bounds))
	for ID := range s.bounds {
		IDs = append(IDs, ID)
	}
	sort.Strings(IDs)
	return IDs
}

// UnboundSources returns all unbound source IDs in increasing order.
func (s *Scheduler) UnboundSources() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	IDs := make([]string, 0, len(s.unbounds))
	for ID := range s.unbounds {
		IDs = append(IDs, ID)
	}
	sort.Strings(IDs)
	return IDs
}

// StartRelay puts etcd key-value pairs to start relay on some workers.
func (s *Scheduler) StartRelay(source string, workers []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	// 1. precheck
	sourceCfg, ok := s.sourceCfgs[source]
	if !ok {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(source)
	}
	startedWorkers := s.relayWorkers[source]

	// quick path for `start-relay` without worker name
	if len(workers) == 0 {
		if len(startedWorkers) != 0 {
			return terror.ErrSchedulerStartRelayOnSpecified.Generate(utils.SetToSlice(startedWorkers))
		}
		// update enable-relay in source config
		sourceCfg.EnableRelay = true
		_, err := ha.PutSourceCfg(s.etcdCli, sourceCfg)
		if err != nil {
			return err
		}
		s.sourceCfgs[source] = sourceCfg
		// notify bound worker
		w, ok2 := s.bounds[source]
		if !ok2 {
			return nil
		}
		stage := ha.NewRelayStage(pb.Stage_Running, source)
		_, err = ha.PutRelayStageSourceBound(s.etcdCli, stage, w.Bound())
		return err
	} else if sourceCfg.EnableRelay {
		// error when `enable-relay` and `start-relay` with worker name
		return terror.ErrSchedulerStartRelayOnBound.Generate()
	}

	if startedWorkers == nil {
		startedWorkers = map[string]struct{}{}
		s.relayWorkers[source] = startedWorkers
	}
	var (
		notExistWorkers []string
		// below two list means the worker that requested start-relay has bound to another source
		boundWorkers, boundSources []string
		alreadyStarted             []string
		// currently we forbid one worker starting multiple relay
		busyWorkers, busySources []string
	)
	for _, workerName := range workers {
		var (
			worker *Worker
			ok     bool
		)
		if worker, ok = s.workers[workerName]; !ok {
			notExistWorkers = append(notExistWorkers, workerName)
			continue
		}
		if _, ok = startedWorkers[workerName]; ok {
			alreadyStarted = append(alreadyStarted, workerName)
		}

		// for Bound and Offline worker
		if worker.Bound().Source != "" && worker.Bound().Source != source {
			boundWorkers = append(boundWorkers, workerName)
			boundSources = append(boundSources, worker.Bound().Source)
		}
		if relaySource := worker.RelaySourceID(); relaySource != "" && relaySource != source {
			busyWorkers = append(busyWorkers, workerName)
			busySources = append(busySources, relaySource)
		}
	}

	if len(notExistWorkers) > 0 {
		return terror.ErrSchedulerWorkerNotExist.Generate(notExistWorkers)
	}
	if len(boundWorkers) > 0 {
		return terror.ErrSchedulerRelayWorkersWrongBound.Generate(boundWorkers, boundSources)
	}
	if len(busyWorkers) > 0 {
		return terror.ErrSchedulerRelayWorkersBusy.Generate(busyWorkers, busySources)
	}
	if len(alreadyStarted) > 0 {
		s.logger.Warn("some workers already started relay",
			zap.String("source", source),
			zap.Strings("already started workers", alreadyStarted))
	}

	// 2. put etcd and update memory cache
	// if there's no relay stage, create a running one. otherwise we should respect paused stage
	if len(startedWorkers) == 0 {
		stage := ha.NewRelayStage(pb.Stage_Running, source)
		if _, err := ha.PutRelayStage(s.etcdCli, stage); err != nil {
			return err
		}
		s.expectRelayStages[source] = stage
	}
	if _, err := ha.PutRelayConfig(s.etcdCli, source, workers...); err != nil {
		return err
	}
	for _, workerName := range workers {
		s.relayWorkers[source][workerName] = struct{}{}
		if err := s.workers[workerName].StartRelay(source); err != nil {
			s.logger.DPanic("we have checked the prerequisite and updated etcd, so should be no error",
				zap.Error(err))
		}
	}
	return nil
}

// StopRelay deletes etcd key-value pairs to stop relay on some workers.
func (s *Scheduler) StopRelay(source string, workers []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	// 1. precheck
	sourceCfg, ok := s.sourceCfgs[source]
	if !ok {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(source)
	}
	// quick path for `stop-relay` without worker name
	if len(workers) == 0 {
		startedWorker := s.relayWorkers[source]
		if len(startedWorker) != 0 {
			return terror.ErrSchedulerStopRelayOnSpecified.Generate(utils.SetToSlice(startedWorker))
		}
		// update enable-relay in source config
		sourceCfg.EnableRelay = false
		_, err := ha.PutSourceCfg(s.etcdCli, sourceCfg)
		if err != nil {
			return err
		}
		s.sourceCfgs[source] = sourceCfg
		// notify bound worker
		w, ok2 := s.bounds[source]
		if !ok2 {
			return nil
		}
		// TODO: remove orphan relay stage
		_, err = ha.PutSourceBound(s.etcdCli, w.Bound())
		return err
	} else if sourceCfg.EnableRelay {
		// error when `enable-relay` and `stop-relay` with worker name
		return terror.ErrSchedulerStopRelayOnBound.Generate()
	}

	var (
		notExistWorkers                    []string
		unmatchedWorkers, unmatchedSources []string
		alreadyStopped                     []string
	)
	for _, workerName := range workers {
		var (
			worker *Worker
			ok     bool
		)

		if worker, ok = s.workers[workerName]; !ok {
			notExistWorkers = append(notExistWorkers, workerName)
			continue
		}

		startedRelay := worker.RelaySourceID()
		if startedRelay == "" {
			alreadyStopped = append(alreadyStopped, workerName)
			continue
		}

		if startedRelay != source {
			unmatchedWorkers = append(unmatchedWorkers, workerName)
			unmatchedSources = append(unmatchedSources, startedRelay)
		}
	}
	if len(notExistWorkers) > 0 {
		return terror.ErrSchedulerWorkerNotExist.Generate(notExistWorkers)
	}
	if len(unmatchedWorkers) > 0 {
		return terror.ErrSchedulerRelayWorkersWrongRelay.Generate(unmatchedWorkers, unmatchedSources)
	}
	if len(alreadyStopped) > 0 {
		s.logger.Warn("some workers already stopped relay",
			zap.String("source", source),
			zap.Strings("already stopped workers", alreadyStopped))
	}

	// 2. delete from etcd and update memory cache
	if _, err := ha.DeleteRelayConfig(s.etcdCli, workers...); err != nil {
		return err
	}
	for _, workerName := range workers {
		delete(s.relayWorkers[source], workerName)
		s.workers[workerName].StopRelay()
	}
	if len(s.relayWorkers[source]) == 0 {
		if _, err := ha.DeleteRelayStage(s.etcdCli, source); err != nil {
			return err
		}
		delete(s.relayWorkers, source)
		delete(s.expectRelayStages, source)
	}
	return nil
}

// GetRelayWorkers returns all alive worker instances for a relay source.
func (s *Scheduler) GetRelayWorkers(source string) ([]*Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.started.Load() {
		return nil, terror.ErrSchedulerNotStarted.Generate()
	}
	workers := s.relayWorkers[source]
	ret := make([]*Worker, 0, len(workers))
	for w := range workers {
		worker, ok := s.workers[w]
		if !ok {
			// should not happen
			s.logger.Error("worker instance for relay worker not found", zap.String("worker", w))
			continue
		}
		ret = append(ret, worker)
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].baseInfo.Name < ret[j].baseInfo.Name
	})
	return ret, nil
}

// UpdateExpectRelayStage updates the current expect relay stage.
// now, only support updates:
// - from `Running` to `Paused`.
// - from `Paused` to `Running`.
// NOTE: from `Running` to `Running` and `Paused` to `Paused` still update the data in etcd,
// because some user may want to update `{Running, Paused, ...}` to `{Running, Running, ...}`.
// so, this should be also supported in DM-worker.
func (s *Scheduler) UpdateExpectRelayStage(newStage pb.Stage, sources ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	if len(sources) == 0 {
		return nil // no sources need to update the stage, this should not happen.
	}

	// 1. check the new expectant stage.
	switch newStage {
	case pb.Stage_Running, pb.Stage_Paused:
	default:
		return terror.ErrSchedulerRelayStageInvalidUpdate.Generate(newStage)
	}

	var (
		notExistSourcesM = make(map[string]struct{})
		currStagesM      = make(map[string]struct{})
		stages           = make([]ha.Stage, 0, len(sources))
	)
	for _, source := range sources {
		if _, ok := s.sourceCfgs[source]; !ok {
			notExistSourcesM[source] = struct{}{}
			continue
		}

		if currStage, ok := s.expectRelayStages[source]; ok {
			currStagesM[currStage.Expect.String()] = struct{}{}
		} else {
			s.logger.Warn("will write relay stage for a source that doesn't have previous stage",
				zap.String("source", source))
		}
		stages = append(stages, ha.NewRelayStage(newStage, source))
	}
	notExistSources := strMapToSlice(notExistSourcesM)
	currStages := strMapToSlice(currStagesM)
	if len(notExistSources) > 0 {
		// some sources not exist, reject the request.
		return terror.ErrSchedulerRelayStageSourceNotExist.Generate(notExistSources)
	} else if len(currStages) > 1 {
		// more than one current relay stage exist, but need to update to the same one, log a warn.
		s.logger.Warn("update more than one current expectant relay stage to the same one",
			zap.Strings("from", currStages), zap.Stringer("to", newStage))
	}

	// 2. put the stages into etcd.
	_, err := ha.PutRelayStage(s.etcdCli, stages...)
	if err != nil {
		return err
	}

	// 3. update the stages in the scheduler.
	for _, stage := range stages {
		s.expectRelayStages[stage.Source] = stage
	}

	return nil
}

// GetExpectRelayStage returns the current expect relay stage.
// If the stage not exists, an invalid stage is returned.
// This func is used for testing.
func (s *Scheduler) GetExpectRelayStage(source string) ha.Stage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if stage, ok := s.expectRelayStages[source]; ok {
		return stage
	}
	return ha.NewRelayStage(pb.Stage_InvalidStage, source)
}

// UpdateExpectSubTaskStage updates the current expect subtask stage.
// now, only support updates:
// - from `Running` to `Paused/Stopped`.
// - from `Paused/Stopped` to `Running`.
// NOTE: from `Running` to `Running` and `Paused` to `Paused` still update the data in etcd,
// because some user may want to update `{Running, Paused, ...}` to `{Running, Running, ...}`.
// so, this should be also supported in DM-worker.
func (s *Scheduler) UpdateExpectSubTaskStage(newStage pb.Stage, taskName string, sources ...string) error {
	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	if taskName == "" || len(sources) == 0 {
		return nil // no subtask need to update, this should not happen.
	}

	// 1. check the new expectant stage.
	switch newStage {
	case pb.Stage_Running, pb.Stage_Paused, pb.Stage_Stopped:
	default:
		return terror.ErrSchedulerSubTaskStageInvalidUpdate.Generate(newStage)
	}

	release, err := s.subtaskLatch.tryAcquire(taskName)
	if err != nil {
		return terror.ErrSchedulerLatchInUse.Generate("UpdateExpectSubTaskStage", taskName)
	}
	defer release()

	// 2. check the task exists.
	v, ok := s.expectSubTaskStages.Load(taskName)
	if !ok {
		return terror.ErrSchedulerSubTaskOpTaskNotExist.Generate(taskName)
	}

	var (
		stagesM          = v.(map[string]ha.Stage)
		notExistSourcesM = make(map[string]struct{})
		currStagesM      = make(map[string]struct{})
		stages           = make([]ha.Stage, 0, len(sources))
	)
	for _, source := range sources {
		if currStage, ok := stagesM[source]; !ok {
			notExistSourcesM[source] = struct{}{}
		} else {
			currStagesM[currStage.Expect.String()] = struct{}{}
		}
		stages = append(stages, ha.NewSubTaskStage(newStage, source, taskName))
	}
	notExistSources := strMapToSlice(notExistSourcesM)
	currStages := strMapToSlice(currStagesM)
	if len(notExistSources) > 0 {
		// some sources not exist, reject the request.
		return terror.ErrSchedulerSubTaskOpSourceNotExist.Generate(notExistSources)
	} else if len(currStages) > 1 {
		// more than one current subtask stage exist, but need to update to the same one, log a warn.
		s.logger.Warn("update more than one current expectant subtask stage to the same one",
			zap.Strings("from", currStages), zap.Stringer("to", newStage))
	}

	// 3. put the stages into etcd.
	_, err = ha.PutSubTaskStage(s.etcdCli, stages...)
	if err != nil {
		return err
	}

	// 4. update the stages in the scheduler.
	for _, stage := range stages {
		stagesM[stage.Source] = stage
	}

	return nil
}

// GetExpectSubTaskStage returns the current expect subtask stage.
// If the stage not exists, an invalid stage is returned.
func (s *Scheduler) GetExpectSubTaskStage(task, source string) ha.Stage {
	invalidStage := ha.NewSubTaskStage(pb.Stage_InvalidStage, source, task)

	release, err := s.subtaskLatch.tryAcquire(task)
	if err != nil {
		return invalidStage
	}
	defer release()

	v, ok := s.expectSubTaskStages.Load(task)
	if !ok {
		return invalidStage
	}
	stageM := v.(map[string]ha.Stage)
	stage, ok := stageM[source]
	if !ok {
		return invalidStage
	}
	return stage
}

// Started returns if the scheduler is started.
func (s *Scheduler) Started() bool {
	return s.started.Load()
}

// recoverSourceCfgs recovers history source configs and expectant relay stages from etcd.
func (s *Scheduler) recoverSources() error {
	// get all source configs.
	cfgM, _, err := ha.GetSourceCfg(s.etcdCli, "", 0)
	if err != nil {
		return err
	}
	// get all relay stages.
	stageM, _, err := ha.GetAllRelayStage(s.etcdCli)
	if err != nil {
		return err
	}

	// recover in-memory data.
	for source, cfg := range cfgM {
		s.sourceCfgs[source] = cfg
	}
	for source, stage := range stageM {
		s.expectRelayStages[source] = stage
	}

	return nil
}

// recoverSubTasks recovers history subtask configs and expectant subtask stages from etcd.
func (s *Scheduler) recoverSubTasks() error {
	// get all subtask configs.
	cfgMM, _, err := ha.GetAllSubTaskCfg(s.etcdCli)
	if err != nil {
		return err
	}
	// get all subtask stages.
	stageMM, _, err := ha.GetAllSubTaskStage(s.etcdCli)
	if err != nil {
		return err
	}
	validatorStageMM, _, err := ha.GetAllValidatorStage(s.etcdCli)
	if err != nil {
		return err
	}

	// recover in-memory data.
	for source, cfgM := range cfgMM {
		for task, cfg := range cfgM {
			v, _ := s.subTaskCfgs.LoadOrStore(task, map[string]config.SubTaskConfig{})
			m := v.(map[string]config.SubTaskConfig)
			m[source] = cfg
		}
	}
	for source, stageM := range stageMM {
		for task, stage := range stageM {
			v, _ := s.expectSubTaskStages.LoadOrStore(task, map[string]ha.Stage{})
			m := v.(map[string]ha.Stage)
			m[source] = stage
		}
	}
	for source, stageM := range validatorStageMM {
		for task, stage := range stageM {
			v, _ := s.expectValidatorStages.LoadOrStore(task, map[string]ha.Stage{})
			m := v.(map[string]ha.Stage)
			m[source] = stage
		}
	}

	return nil
}

// recoverRelayConfigs recovers history relay configs for each worker from etcd.
// This function also removes conflicting relay schedule types, which means if a source has both `enable-relay` and
// (source, worker) relay config, we remove the latter.
// should be called after recoverSources.
func (s *Scheduler) recoverRelayConfigs() error {
	relayWorkers, _, err := ha.GetAllRelayConfig(s.etcdCli)
	if err != nil {
		return err
	}

	for source, workers := range relayWorkers {
		sourceCfg, ok := s.sourceCfgs[source]
		if !ok {
			s.logger.Warn("found a not existing source by relay config", zap.String("source", source))
			continue
		}
		if sourceCfg.EnableRelay {
			// current etcd max-txn-op is 2048
			_, err2 := ha.DeleteRelayConfig(s.etcdCli, utils.SetToSlice(workers)...)
			if err2 != nil {
				return err2
			}
			delete(relayWorkers, source)
		}
	}

	s.relayWorkers = relayWorkers
	return nil
}

// recoverLoadTasks recovers history load workers from etcd.
func (s *Scheduler) recoverLoadTasks(needLock bool) (int64, error) {
	if needLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	loadTasks, rev, err := ha.GetAllLoadTask(s.etcdCli)
	if err != nil {
		return 0, err
	}

	s.loadTasks = loadTasks
	return rev, nil
}

// recoverWorkersBounds recovers history DM-worker info and status from etcd.
// and it also recovers the bound/unbound relationship.
func (s *Scheduler) recoverWorkersBounds() (int64, error) {
	// 1. get all history base info.
	// it should no new DM-worker registered between this call and the below `GetKeepAliveWorkers`,
	// because no DM-master leader are handling DM-worker register requests.
	wim, _, err := ha.GetAllWorkerInfo(s.etcdCli)
	if err != nil {
		return 0, err
	}

	// 2. get all history bound relationships.
	// it should no new bound relationship added between this call and the below `GetKeepAliveWorkers`,
	// because no DM-master leader are doing the scheduler.
	sbm, _, err := ha.GetSourceBound(s.etcdCli, "")
	if err != nil {
		return 0, err
	}
	lastSourceBoundM, _, err := ha.GetLastSourceBounds(s.etcdCli)
	if err != nil {
		return 0, err
	}
	s.lastBound = lastSourceBoundM

	// 3. get all history offline status.
	kam, rev, err := ha.GetKeepAliveWorkers(s.etcdCli)
	if err != nil {
		return 0, err
	}

	scm := s.sourceCfgs
	boundsToTrigger := make([]ha.SourceBound, 0)

	// 4. recover DM-worker info and status.
	// prepare a worker -> relay source map
	relayInfo := map[string]string{}
	for source, workers := range s.relayWorkers {
		for worker := range workers {
			relayInfo[worker] = source
		}
	}

	for name, info := range wim {
		// create and record the worker agent.
		w, err2 := s.recordWorker(info)
		if err2 != nil {
			return 0, err2
		}
		// set the stage as Free if it's keep alive.
		if _, ok := kam[name]; ok {
			w.ToFree()
			if source, ok2 := relayInfo[name]; ok2 {
				if err3 := w.StartRelay(source); err3 != nil {
					s.logger.DPanic("", zap.Error(err3))
				}
			}

			// set the stage as Bound and record the bound relationship if exists.
			if bound, ok := sbm[name]; ok {
				// source bounds without source configuration should be deleted later
				if _, ok := scm[bound.Source]; ok {
					err2 = s.updateStatusToBound(w, bound)
					if err2 != nil {
						// if etcd has saved KV that worker1 started relay for source1, but bound to source2,
						// we remove the bound to avoid DM master leader failed to bootstrap
						if terror.ErrSchedulerBoundDiffWithStartedRelay.Equal(err2) {
							continue
						}
						return 0, err2
					}
					boundsToTrigger = append(boundsToTrigger, bound)
					delete(sbm, name)
				} else {
					s.logger.Warn("find source bound without config", zap.Stringer("bound", bound))
				}
			}
		}
	}

	failpoint.Inject("failToRecoverWorkersBounds", func(_ failpoint.Value) {
		log.L().Info("mock failure", zap.String("failpoint", "failToRecoverWorkersBounds"))
		failpoint.Return(0, errors.New("failToRecoverWorkersBounds"))
	})
	// 5. delete invalid source bound info in etcd
	if len(sbm) > 0 {
		invalidSourceBounds := make([]string, 0, len(sbm))
		for name := range sbm {
			invalidSourceBounds = append(invalidSourceBounds, name)
		}
		_, err = ha.DeleteSourceBound(s.etcdCli, invalidSourceBounds...)
		if err != nil {
			return 0, err
		}
	}

	// 6. put trigger source bounds info to etcd to order dm-workers to start source
	if len(boundsToTrigger) > 0 {
		_, err = ha.PutSourceBound(s.etcdCli, boundsToTrigger...)
		if err != nil {
			return 0, err
		}
	}

	// 7. recover bounds/unbounds, all sources which not in bounds should be in unbounds.
	for source := range s.sourceCfgs {
		if _, ok := s.bounds[source]; !ok {
			s.unbounds[source] = struct{}{}
		}
	}

	return rev, nil
}

func (s *Scheduler) resetWorkerEv() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rwm := s.workers
	kam, rev, err := ha.GetKeepAliveWorkers(s.etcdCli)
	if err != nil {
		return 0, err
	}

	// update all registered workers status
	for name := range rwm {
		ev := ha.WorkerEvent{WorkerName: name}
		// set the stage as Free if it's keep alive.
		if _, ok := kam[name]; ok {
			err = s.handleWorkerOnline(ev, false)
			if err != nil {
				return 0, err
			}
		} else {
			err = s.handleWorkerOffline(ev, false)
			if err != nil {
				return 0, err
			}
		}
	}
	return rev, nil
}

// handleWorkerEv handles the online/offline status change event of DM-worker instances.
func (s *Scheduler) handleWorkerEv(ctx context.Context, evCh <-chan ha.WorkerEvent, errCh <-chan error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-evCh:
			if !ok {
				return nil
			}
			s.logger.Info("receive worker status change event", zap.Bool("delete", ev.IsDeleted), zap.Stringer("event", ev))
			var err error
			if ev.IsDeleted {
				err = s.handleWorkerOffline(ev, true)
			} else {
				err = s.handleWorkerOnline(ev, true)
			}
			if err != nil {
				s.logger.Error("fail to handle worker status change event", zap.Bool("delete", ev.IsDeleted), zap.Stringer("event", ev), zap.Error(err))
				metrics.ReportWorkerEventErr(metrics.WorkerEventHandle)
			}
		case err, ok := <-errCh:
			if !ok {
				return nil
			}
			// error here are caused by etcd error or worker event decoding
			s.logger.Error("receive error when watching worker status change event", zap.Error(err))
			metrics.ReportWorkerEventErr(metrics.WorkerEventWatch)
			if etcdutil.IsRetryableError(err) {
				return err
			}
		}
	}
}

// nolint:dupl
func (s *Scheduler) observeWorkerEvent(ctx context.Context, rev int64) error {
	var wg sync.WaitGroup
	for {
		workerEvCh := make(chan ha.WorkerEvent, 10)
		workerErrCh := make(chan error, 10)
		wg.Add(1)
		// use ctx1, cancel1 to make sure old watcher has been released
		ctx1, cancel1 := context.WithCancel(ctx)
		go func() {
			defer func() {
				close(workerEvCh)
				close(workerErrCh)
				wg.Done()
			}()
			ha.WatchWorkerEvent(ctx1, s.etcdCli, rev+1, workerEvCh, workerErrCh)
		}()
		err := s.handleWorkerEv(ctx1, workerEvCh, workerErrCh)
		cancel1()
		wg.Wait()

		if etcdutil.IsRetryableError(err) {
			rev = 0
			retryNum := 1
			for rev == 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(500 * time.Millisecond):
					rev, err = s.resetWorkerEv()
					if err != nil {
						log.L().Error("resetWorkerEv is failed, will retry later", zap.Error(err), zap.Int("retryNum", retryNum))
					}
				}
				retryNum++
			}
		} else {
			if err != nil {
				log.L().Error("observeWorkerEvent is failed and will quit now", zap.Error(err))
			} else {
				log.L().Info("observeWorkerEvent will quit now")
			}
			return err
		}
	}
}

// handleWorkerOnline handles the scheduler when a DM-worker become online.
// This should try to bound an unbounded source to it.
// NOTE: this func need to hold the mutex.
func (s *Scheduler) handleWorkerOnline(ev ha.WorkerEvent, toLock bool) error {
	if toLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	// 1. find the worker.
	w, ok := s.workers[ev.WorkerName]
	if !ok {
		s.logger.Warn("worker for the event not exists", zap.Stringer("event", ev))
		return nil
	}

	// 2. check whether is bound.
	if w.Stage() == WorkerBound {
		// also put identical relay config for this worker
		if source := w.RelaySourceID(); source != "" {
			_, err := ha.PutRelayConfig(s.etcdCli, source, w.BaseInfo().Name)
			if err != nil {
				return err
			}
		}
		// TODO: When dm-worker keepalive is broken, it will turn off its own running source
		// After keepalive is restored, this dm-worker should continue to run the previously bound source
		// So we PutSourceBound here to trigger dm-worker to get this event and start source again.
		// If this worker still start a source, it doesn't matter. dm-worker will omit same source and reject source with different name
		s.logger.Warn("worker already bound", zap.Stringer("bound", w.Bound()))
		_, err := ha.PutSourceBound(s.etcdCli, w.Bound())
		return err
	}

	// 3. change the stage (from Offline) to Free or Relay.
	lastRelaySource := w.RelaySourceID()
	if lastRelaySource == "" {
		// when worker is removed (for example lost keepalive when master scheduler boots up), w.RelaySourceID() is
		// of course nothing, so we find the relay source from a better place
		for source, workerM := range s.relayWorkers {
			if _, ok2 := workerM[w.BaseInfo().Name]; ok2 {
				lastRelaySource = source
				break
			}
		}
	}
	w.ToFree()
	// TODO: rename ToFree to Online and move below logic inside it
	if lastRelaySource != "" {
		if err := w.StartRelay(lastRelaySource); err != nil {
			s.logger.DPanic("", zap.Error(err))
		}
	}

	// 4. try to bound an unbounded source.
	_, err := s.tryBoundForWorker(w)
	return err
}

// handleWorkerOffline handles the scheduler when a DM-worker become offline.
// This should unbound any previous bounded source.
// NOTE: this func need to hold the mutex.
func (s *Scheduler) handleWorkerOffline(ev ha.WorkerEvent, toLock bool) error {
	if toLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	// 1. find the worker.
	w, ok := s.workers[ev.WorkerName]
	if !ok {
		s.logger.Warn("worker for the event not exists", zap.Stringer("event", ev))
		return nil
	}

	// 2. find the bound relationship.
	bound := w.Bound()

	// 3. check whether bound before.
	if bound.Source == "" {
		// 3.1. change the stage (from Free) to Offline.
		w.ToOffline()
		s.logger.Info("worker not bound, no need to unbound", zap.Stringer("event", ev))
		return nil
	}

	// 4. delete the bound relationship in etcd.
	_, err := ha.DeleteSourceBound(s.etcdCli, bound.Worker)
	if err != nil {
		return err
	}

	// 5. unbound for the source.
	s.updateStatusToUnbound(bound.Source)

	// 6. change the stage (from Free) to Offline.
	w.ToOffline()

	s.logger.Info("unbound the worker for source", zap.Stringer("bound", bound), zap.Stringer("event", ev))

	// 7. try to bound the source to a Free worker again.
	_, err = s.tryBoundForSource(bound.Source)
	return err
}

// tryBoundForWorker tries to bind a source to the given worker. The order of picking source is
// - try to bind sources on which the worker has unfinished load task
// - try to bind the last bound source
// - if enabled relay, bind to the relay source or keep unbound
// - try to bind any unbound sources
// if the source is bound to a relay enabled worker, we must check that the source is also the relay source of worker.
// pulling binlog using relay or not is determined by whether the worker has enabled relay.
func (s *Scheduler) tryBoundForWorker(w *Worker) (bounded bool, err error) {
	// 1. handle this worker has unfinished load task.
	worker, sourceID := s.getNextLoadTaskTransfer(w.BaseInfo().Name, "")
	if sourceID != "" {
		s.logger.Info("found unfinished load task source when worker bound",
			zap.String("worker", w.BaseInfo().Name),
			zap.String("source", sourceID))
		// TODO: tolerate a failed transfer because of start-relay conflicts with loadTask
		err = s.transferWorkerAndSource(w.BaseInfo().Name, "", worker, sourceID)
		return err == nil, err
	}

	// check if last bound is still available.
	// NOTE: if worker isn't in lastBound, we'll get "zero" SourceBound and it's OK, because "zero" string is not in
	// unbounds
	source := s.lastBound[w.baseInfo.Name].Source
	if _, ok := s.unbounds[source]; !ok {
		source = ""
	}

	if source != "" {
		relaySource := w.RelaySourceID()
		if relaySource != "" && relaySource != source {
			source = ""
		} else {
			// worker not enable relay or last bound is relay source
			s.logger.Info("found history source when worker bound",
				zap.String("worker", w.BaseInfo().Name),
				zap.String("source", source))
		}
	}

	// try to find its relay source (currently only one relay source)
	if source == "" {
		source = w.RelaySourceID()
		if source != "" {
			s.logger.Info("found relay source when worker bound",
				zap.String("worker", w.BaseInfo().Name),
				zap.String("source", source))
			// currently worker can only handle same relay source and source bound, so we don't try bound another source
			if oldWorker, ok := s.bounds[source]; ok {
				s.logger.Info("worker has started relay for a source, but that source is bound to another worker, so we let this worker free",
					zap.String("worker", w.BaseInfo().Name),
					zap.String("relay source", source),
					zap.String("bound worker for its relay source", oldWorker.BaseInfo().Name))
				return false, nil
			}
		}
	}

	// randomly pick one from unbounds
	if source == "" {
		for source = range s.unbounds {
			s.logger.Info("found unbound source when worker bound",
				zap.String("worker", w.BaseInfo().Name),
				zap.String("source", source))
			break // got a source.
		}
	}

	if source == "" {
		s.logger.Info("no unbound sources need to bound", zap.Stringer("worker", w.BaseInfo()))
		return false, nil
	}

	// 2. try to bound them.
	err = s.boundSourceToWorker(source, w)
	if err != nil {
		return false, err
	}
	return true, nil
}

// tryBoundForSource tries to bound a source to a random Free worker. The order of picking worker is
// - try to bind a worker which has unfinished load task
// - try to bind a relay worker which has be bound to this source before
// - try to bind any relay worker
// - try to bind any worker which has be bound to this source before
// - try to bind any free worker
// pulling binlog using relay or not is determined by whether the worker has enabled relay.
// caller should update the s.unbounds.
// caller should make sure this source has source config.
func (s *Scheduler) tryBoundForSource(source string) (bool, error) {
	var worker *Worker

	// pick a worker which has subtask in load stage.
	workerName, sourceID := s.getNextLoadTaskTransfer("", source)
	if workerName != "" {
		// TODO: check relay source conflict
		err := s.transferWorkerAndSource("", source, workerName, sourceID)
		return err == nil, err
	}

	relayWorkers := s.relayWorkers[source]
	// 1. try to find a history worker in relay workers...
	if len(relayWorkers) > 0 {
		for workerName, bound := range s.lastBound {
			if bound.Source == source {
				w, ok := s.workers[workerName]
				if !ok {
					// a not found worker
					continue
				}
				// the worker is not Offline
				if _, ok2 := relayWorkers[workerName]; ok2 && w.Stage() == WorkerRelay {
					worker = w
					s.logger.Info("found history relay worker when source bound",
						zap.String("worker", workerName),
						zap.String("source", source))
					break
				}
			}
		}
	}
	// then a relay worker for this source...
	if worker == nil {
		for workerName := range relayWorkers {
			w, ok := s.workers[workerName]
			if !ok {
				// a not found worker, should not happen
				s.logger.DPanic("worker instance not found for relay worker", zap.String("worker", workerName))
				continue
			}
			// the worker is not Offline
			if w.Stage() == WorkerRelay {
				worker = w
				s.logger.Info("found relay worker when source bound",
					zap.String("worker", workerName),
					zap.String("source", source))
				break
			}
		}
	}
	// then a history worker for this source...
	if worker == nil {
		for workerName, bound := range s.lastBound {
			if bound.Source == source {
				w, ok := s.workers[workerName]
				if !ok {
					// a not found worker
					continue
				}
				if w.Stage() == WorkerFree {
					worker = w
					s.logger.Info("found history worker when source bound",
						zap.String("worker", workerName),
						zap.String("source", source))
					break
				}
			}
		}
	}

	// and then a random Free worker.
	if worker == nil {
		for _, w := range s.workers {
			if w.Stage() == WorkerFree {
				worker = w
				s.logger.Info("found free worker when source bound",
					zap.String("worker", w.BaseInfo().Name),
					zap.String("source", source))
				break
			}
		}
	}

	if worker == nil {
		s.logger.Info("no free worker exists for bound", zap.String("source", source))
		return false, nil
	}

	// 2. try to bound them.
	err := s.boundSourceToWorker(source, worker)
	if err != nil {
		return false, err
	}
	return true, nil
}

// boundSourceToWorker bounds the source and worker together.
// we should check the bound relationship of the source and the stage of the worker in the caller.
func (s *Scheduler) boundSourceToWorker(source string, w *Worker) error {
	// 1. put the bound relationship into etcd.
	var err error
	bound := ha.NewSourceBound(source, w.BaseInfo().Name)
	sourceCfg, ok := s.sourceCfgs[source]
	if ok && sourceCfg.EnableRelay {
		stage := ha.NewRelayStage(pb.Stage_Running, source)
		_, err = ha.PutRelayStageSourceBound(s.etcdCli, stage, bound)
	} else {
		_, err = ha.PutSourceBound(s.etcdCli, bound)
	}
	if err != nil {
		return err
	}

	// 2. update the bound relationship in the scheduler.
	err = s.updateStatusToBound(w, bound)
	if err != nil {
		return err
	}

	s.logger.Info("bound the source to worker", zap.Stringer("bound", bound))
	return nil
}

// recordWorker creates the worker agent (with Offline stage) and records in the scheduler.
// this func is used when adding a new worker.
// NOTE: trigger scheduler when the worker become online, not when added.
func (s *Scheduler) recordWorker(info ha.WorkerInfo) (*Worker, error) {
	w, err := NewWorker(info, s.securityCfg)
	if err != nil {
		return nil, err
	}
	s.workers[info.Name] = w
	return w, nil
}

// deleteWorker deletes the recorded worker and bound.
// this func is used when removing the worker.
// NOTE: trigger scheduler when the worker become offline, not when deleted.
func (s *Scheduler) deleteWorker(name string) {
	for _, workers := range s.relayWorkers {
		delete(workers, name)
	}
	w, ok := s.workers[name]
	if !ok {
		return
	}
	w.Close()
	delete(s.workers, name)
	metrics.RemoveWorkerState(w.baseInfo.Name)
}

// updateStatusToBound updates the in-memory status for bound, including:
// - update the stage of worker to `Bound`.
// - record the bound relationship and last bound relationship in the scheduler.
// - remove the unbound relationship in the scheduler.
// this func is called after the bound relationship existed in etcd.
func (s *Scheduler) updateStatusToBound(w *Worker, b ha.SourceBound) error {
	if err := w.ToBound(b); err != nil {
		return err
	}
	s.bounds[b.Source] = w
	s.lastBound[b.Worker] = b
	delete(s.unbounds, b.Source)
	return nil
}

// updateStatusToUnbound updates the in-memory status for unbound, including:
// - update the stage of worker to `Free` or `Relay`.
// - remove the bound relationship in the scheduler.
// - record the unbound relationship in the scheduler.
// this func is called after the bound relationship removed from etcd.
func (s *Scheduler) updateStatusToUnbound(source string) {
	s.unbounds[source] = struct{}{}
	w, ok := s.bounds[source]
	if !ok {
		return
	}
	if err := w.Unbound(); err != nil {
		s.logger.DPanic("cannot updateStatusToUnbound", zap.Error(err))
	}
	delete(s.bounds, source)
}

// reset resets the internal status.
func (s *Scheduler) reset() {
	s.subtaskLatch = newLatches()
	s.sourceCfgs = make(map[string]*config.SourceConfig)
	s.subTaskCfgs = sync.Map{}
	s.workers = make(map[string]*Worker)
	s.bounds = make(map[string]*Worker)
	s.unbounds = make(map[string]struct{})
	s.expectRelayStages = make(map[string]ha.Stage)
	s.expectSubTaskStages = sync.Map{}
	s.loadTasks = make(map[string]map[string]string)
}

// strMapToSlice converts a `map[string]struct{}` to `[]string` in increasing order.
func strMapToSlice(m map[string]struct{}) []string {
	ret := make([]string, 0, len(m))
	for s := range m {
		ret = append(ret, s)
	}
	sort.Strings(ret)
	return ret
}

// SetWorkerClientForTest sets mockWorkerClient for specified worker, only used for test.
func (s *Scheduler) SetWorkerClientForTest(name string, mockCli workerrpc.Client) {
	if _, ok := s.workers[name]; ok {
		s.workers[name].cli = mockCli
	}
}

// nolint:dupl
func (s *Scheduler) observeLoadTask(ctx context.Context, rev int64) error {
	var wg sync.WaitGroup
	for {
		loadTaskCh := make(chan ha.LoadTask, 10)
		loadTaskErrCh := make(chan error, 10)
		wg.Add(1)
		// use ctx1, cancel1 to make sure old watcher has been released
		ctx1, cancel1 := context.WithCancel(ctx)
		go func() {
			defer func() {
				close(loadTaskCh)
				close(loadTaskErrCh)
				wg.Done()
			}()
			ha.WatchLoadTask(ctx1, s.etcdCli, rev+1, loadTaskCh, loadTaskErrCh)
		}()
		err := s.handleLoadTask(ctx1, loadTaskCh, loadTaskErrCh)
		cancel1()
		wg.Wait()

		if etcdutil.IsRetryableError(err) {
			rev = 0
			retryNum := 1
			for rev == 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(500 * time.Millisecond):
					rev, err = s.recoverLoadTasks(true)
					if err != nil {
						log.L().Error("resetLoadTask is failed, will retry later", zap.Error(err), zap.Int("retryNum", retryNum))
					}
				}
				retryNum++
			}
		} else {
			if err != nil {
				log.L().Error("observeLoadTask is failed and will quit now", zap.Error(err))
			} else {
				log.L().Info("observeLoadTask will quit now")
			}
			return err
		}
	}
}

// RemoveLoadTask removes the loadtask by task.
func (s *Scheduler) RemoveLoadTask(task string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}
	_, _, err := ha.DelLoadTaskByTask(s.etcdCli, task)
	if err != nil {
		return err
	}
	delete(s.loadTasks, task)
	return nil
}

// getTransferWorkerAndSource tries to get transfer worker and source.
// return (worker, source) that is used by transferWorkerAndSource, to try to resolve a paused load task that the source can't be bound to the worker which has its dump files.
// worker, source	This means a subtask finish load stage, often called by handleLoadTaskDel.
// worker, ""		This means a free worker online, often called by tryBoundForWorker.
// "", source		This means a unbounded source online, often called by tryBoundForSource.
func (s *Scheduler) getNextLoadTaskTransfer(worker, source string) (string, string) {
	// origin worker not free, try to get a source.
	if worker != "" {
		// try to get a unbounded source
		for sourceID := range s.unbounds {
			if sourceID != source && s.hasLoadTaskByWorkerAndSource(worker, sourceID) {
				return "", sourceID
			}
		}
		// try to get a bounded source
		for sourceID, w := range s.bounds {
			if sourceID != source && s.hasLoadTaskByWorkerAndSource(worker, sourceID) && !s.hasLoadTaskByWorkerAndSource(w.baseInfo.Name, sourceID) {
				return w.baseInfo.Name, sourceID
			}
		}
	}

	// origin source bounded, try to get a worker
	if source != "" {
		// try to get a free worker
		for _, w := range s.workers {
			workerName := w.baseInfo.Name
			if workerName != worker && w.Stage() == WorkerFree && s.hasLoadTaskByWorkerAndSource(workerName, source) {
				return workerName, ""
			}
		}

		// try to get a bounded worker
		for _, w := range s.workers {
			workerName := w.baseInfo.Name
			if workerName != worker && w.Stage() == WorkerBound {
				if s.hasLoadTaskByWorkerAndSource(workerName, source) && !s.hasLoadTaskByWorkerAndSource(workerName, w.bound.Source) {
					return workerName, w.bound.Source
				}
			}
		}
	}

	return "", ""
}

// hasLoadTaskByWorkerAndSource check whether there is an existing load subtask for the worker and source.
func (s *Scheduler) hasLoadTaskByWorkerAndSource(worker, source string) bool {
	for taskName, sourceWorkerMap := range s.loadTasks {
		// don't consider removed subtask
		subtasksV, ok := s.subTaskCfgs.Load(taskName)
		if !ok {
			continue
		}
		subtasks := subtasksV.(map[string]config.SubTaskConfig)
		if _, ok2 := subtasks[source]; !ok2 {
			continue
		}

		if workerName, ok2 := sourceWorkerMap[source]; ok2 && workerName == worker {
			return true
		}
	}
	return false
}

// TryResolveLoadTask checks if there are sources whose load task has local files and not bound to the worker which is
// accessible to the local files. If so, trigger a transfer source.
func (s *Scheduler) TryResolveLoadTask(sources []string) {
	for _, source := range sources {
		s.mu.Lock()
		worker, ok := s.bounds[source]
		if !ok {
			s.mu.Unlock()
			continue
		}
		if err := s.tryResolveLoadTask(worker.baseInfo.Name, source); err != nil {
			s.logger.Error("tryResolveLoadTask failed", zap.Error(err))
		}
		s.mu.Unlock()
	}
}

func (s *Scheduler) tryResolveLoadTask(originWorker, originSource string) error {
	if s.hasLoadTaskByWorkerAndSource(originWorker, originSource) {
		return nil
	}

	worker, source := s.getNextLoadTaskTransfer(originWorker, originSource)
	if worker == "" && source == "" {
		return nil
	}

	return s.transferWorkerAndSource(originWorker, originSource, worker, source)
}

func (s *Scheduler) handleLoadTaskDel(loadTask ha.LoadTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.loadTasks[loadTask.Task]; !ok {
		return nil
	}
	if _, ok := s.loadTasks[loadTask.Task][loadTask.Source]; !ok {
		return nil
	}

	originWorker := s.loadTasks[loadTask.Task][loadTask.Source]
	delete(s.loadTasks[loadTask.Task], loadTask.Source)
	if len(s.loadTasks[loadTask.Task]) == 0 {
		delete(s.loadTasks, loadTask.Task)
	}

	return s.tryResolveLoadTask(originWorker, loadTask.Source)
}

func (s *Scheduler) handleLoadTaskPut(loadTask ha.LoadTask) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.loadTasks[loadTask.Task]; !ok {
		s.loadTasks[loadTask.Task] = make(map[string]string)
	}
	s.loadTasks[loadTask.Task][loadTask.Source] = loadTask.Worker
}

// handleLoadTask handles the load worker status change event.
func (s *Scheduler) handleLoadTask(ctx context.Context, loadTaskCh <-chan ha.LoadTask, errCh <-chan error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case loadTask, ok := <-loadTaskCh:
			if !ok {
				return nil
			}
			s.logger.Info("receive load task", zap.Bool("delete", loadTask.IsDelete), zap.String("task", loadTask.Task), zap.String("source", loadTask.Source), zap.String("worker", loadTask.Worker))
			var err error
			if loadTask.IsDelete {
				err = s.handleLoadTaskDel(loadTask)
			} else {
				s.handleLoadTaskPut(loadTask)
			}
			if err != nil {
				s.logger.Error("fail to handle worker status change event", zap.Error(err))
			}
		case err, ok := <-errCh:
			if !ok {
				return nil
			}
			// error here are caused by etcd error or load worker decoding
			s.logger.Error("receive error when watching load worker", zap.Error(err))
			if etcdutil.IsRetryableError(err) {
				return err
			}
		}
	}
}

// OperateValidationTask operate validator of subtask.
// 	tasks: tasks need to operate
// 	validatorStages: stage info of subtask validators
// 	changedSubtaskCfgs: changed subtask configs
// see server.StartValidation/StopValidation for more detail.
func (s *Scheduler) OperateValidationTask(validatorStages []ha.Stage, changedSubtaskCfgs []config.SubTaskConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started.Load() {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	// 2. setting subtask stage in etcd
	if len(changedSubtaskCfgs) > 0 || len(validatorStages) > 0 {
		_, err := ha.PutSubTaskCfgStage(s.etcdCli, changedSubtaskCfgs, []ha.Stage{}, validatorStages)
		if err != nil {
			return terror.Annotate(err, "fail to set new validator stage")
		}
	}
	// 3. cache validator stage
	for _, stage := range validatorStages {
		v, _ := s.expectValidatorStages.LoadOrStore(stage.Task, map[string]ha.Stage{})
		m := v.(map[string]ha.Stage)
		m[stage.Source] = stage
	}
	for _, cfg := range changedSubtaskCfgs {
		v, _ := s.subTaskCfgs.LoadOrStore(cfg.Name, map[string]config.SubTaskConfig{})
		m := v.(map[string]config.SubTaskConfig)
		m[cfg.SourceID] = cfg
	}
	return nil
}

// ValidatorEnabled returns true when validator of task-source pair has enabled, i.e. validation mode is not none.
// enabled validator can be in running or stopped stage.
func (s *Scheduler) ValidatorEnabled(task, source string) bool {
	return s.GetValidatorStage(task, source) != nil
}

// GetValidatorStage get validator stage of task-source pair.
func (s *Scheduler) GetValidatorStage(task, source string) *ha.Stage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.expectValidatorStages.Load(task)
	if !ok {
		return nil
	}
	m := v.(map[string]ha.Stage)
	if stage, ok2 := m[source]; ok2 {
		return &stage
	}
	return nil
}
