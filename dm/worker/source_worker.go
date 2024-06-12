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
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/streamer"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/relay"
	bf "github.com/pingcap/tiflow/pkg/binlog-filter"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// SourceWorker manages a source(upstream) which is mainly related to subtasks and relay.
type SourceWorker struct {
	// ensure no other operation can be done when closing (we can use `WaitGroup`/`Context` to archive this)
	// TODO: check what does it guards. Now it's used to guard relayHolder and relayPurger (maybe subTaskHolder?) since
	// query-status maybe access them when closing/disable functionalities
	// This lock is used to guards source worker's source config and subtask holder(subtask configs)
	sync.RWMutex

	wg     sync.WaitGroup
	closed atomic.Bool

	// context created when SourceWorker created, and canceled when closing
	ctx    context.Context
	cancel context.CancelFunc

	cfg        *config.SourceConfig
	sourceDB   *conn.BaseDB
	sourceDBMu sync.Mutex // if the sourceDB can't be connected at start time, we try to re-connect before using it.

	l log.Logger

	sourceStatus atomic.Value // stores a pointer to SourceStatus

	// subtask functionality
	subTaskEnabled atomic.Bool
	subTaskCtx     context.Context
	subTaskCancel  context.CancelFunc
	subTaskWg      sync.WaitGroup
	subTaskHolder  *subTaskHolder

	// relay functionality
	// during relayEnabled == true, relayHolder and relayPurger should not be nil
	relayEnabled atomic.Bool
	relayCtx     context.Context
	relayCancel  context.CancelFunc
	relayWg      sync.WaitGroup
	relayHolder  RelayHolder
	relayPurger  relay.Purger
	relayDir     string

	startedRelayBySourceCfg bool

	taskStatusChecker TaskStatusChecker

	etcdClient *clientv3.Client

	name string
}

// NewSourceWorker creates a new SourceWorker. The functionality of relay and subtask is disabled by default, need call EnableRelay
// and EnableSubtask later.
func NewSourceWorker(
	cfg *config.SourceConfig,
	etcdClient *clientv3.Client,
	name string,
	relayDir string,
) (w *SourceWorker, err error) {
	w = &SourceWorker{
		cfg:           cfg,
		subTaskHolder: newSubTaskHolder(),
		l:             log.With(zap.String("component", "worker controller")),
		etcdClient:    etcdClient,
		name:          name,
		relayDir:      relayDir,
	}
	// keep running until canceled in `Close`.
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.closed.Store(true)
	w.subTaskEnabled.Store(false)
	w.relayEnabled.Store(false)

	defer func(w2 *SourceWorker) {
		if err != nil { // when err != nil, `w` will become nil in this func, so we pass `w` in defer.
			// release resources, NOTE: we need to refactor New/Init/Start/Close for components later.
			w2.cancel()
			w2.subTaskHolder.closeAllSubTasks()
		}
	}(w)

	// initial task status checker
	if w.cfg.Checker.CheckEnable {
		tsc := NewTaskStatusChecker(w.cfg.Checker, w)
		err = tsc.Init()
		if err != nil {
			return nil, err
		}
		w.taskStatusChecker = tsc
	}

	InitConditionHub(w)

	w.l.Info("initialized", zap.Stringer("cfg", cfg))

	return w, nil
}

// Start starts working, but the functionalities should be turned on separately.
func (w *SourceWorker) Start() {
	// start task status checker
	if w.cfg.Checker.CheckEnable {
		w.taskStatusChecker.Start()
	}

	var err error
	w.sourceDB, err = conn.GetUpstreamDB(&w.cfg.GetDecryptedClone().From)
	if err != nil {
		w.l.Error("can't connected to upstream", zap.Error(err))
	}

	w.wg.Add(1)
	defer w.wg.Done()

	w.l.Info("start running")

	printTaskInterval := 30 * time.Second
	failpoint.Inject("PrintStatusCheckSeconds", func(val failpoint.Value) {
		if seconds, ok := val.(int); ok {
			printTaskInterval = time.Duration(seconds) * time.Second
			log.L().Info("set printStatusInterval",
				zap.String("failpoint", "PrintStatusCheckSeconds"),
				zap.Int("value", seconds))
		}
	})

	ticker := time.NewTicker(printTaskInterval)
	w.closed.Store(false)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			w.l.Info("status print process exits!")
			return
		case <-ticker.C:
			old := w.sourceStatus.Load()
			if old != nil {
				status := old.(*binlog.SourceStatus)
				if time.Since(status.UpdateTime) < printTaskInterval/2 {
					w.l.Info("we just updated the source status, skip once",
						zap.Time("last update time", status.UpdateTime))
					continue
				}
			}
			if err2 := w.updateSourceStatus(w.ctx, true); err2 != nil {
				if terror.ErrNoMasterStatus.Equal(err2) {
					w.l.Warn("This source's bin_log is OFF, so it only supports full_mode.", zap.String("sourceID", w.cfg.SourceID), zap.Error(err2))
				} else {
					w.l.Error("failed to update source status", zap.Error(err2))
				}
				continue
			}

			sourceStatus := w.sourceStatus.Load().(*binlog.SourceStatus)
			if w.l.Core().Enabled(zap.DebugLevel) {
				w.l.Debug("runtime status", zap.String("status", w.GetUnitAndSourceStatusJSON("", sourceStatus)))
			}

			// periodically print the status and update metrics
			w.Status("", sourceStatus)
		}
	}
}

// Stop stops working and releases resources.
func (w *SourceWorker) Stop(graceful bool) {
	if w.closed.Load() {
		w.l.Warn("already closed")
		return
	}

	// cancel status output ticker and wait for return
	w.cancel()
	w.wg.Wait()
	w.relayWg.Wait()
	w.subTaskWg.Wait()

	w.Lock()
	defer w.Unlock()

	// close or kill all subtasks
	if graceful {
		w.subTaskHolder.closeAllSubTasks()
	} else {
		w.subTaskHolder.killAllSubTasks()
	}

	if w.relayHolder != nil {
		w.relayHolder.Close()
	}

	if w.relayPurger != nil {
		w.relayPurger.Close()
	}

	// close task status checker
	if w.cfg.Checker.CheckEnable {
		w.taskStatusChecker.Close()
	}

	w.sourceDB.Close()
	w.sourceDB = nil

	w.closed.Store(true)

	w.l.Info("Stop worker")
}

// updateSourceStatus updates w.sourceStatus.
func (w *SourceWorker) updateSourceStatus(ctx context.Context, needLock bool) error {
	var cfg *config.SourceConfig
	if needLock {
		w.RLock()
		cfg = w.cfg
		w.RUnlock()
	} else {
		cfg = w.cfg
	}
	w.sourceDBMu.Lock()
	if w.sourceDB == nil {
		var err error
		w.sourceDB, err = conn.GetUpstreamDB(&cfg.GetDecryptedClone().From)
		if err != nil {
			w.sourceDBMu.Unlock()
			return err
		}
	}
	w.sourceDBMu.Unlock()

	status, err := binlog.GetSourceStatus(tcontext.NewContext(ctx, w.l), w.sourceDB, cfg.Flavor)
	if err != nil {
		return err
	}

	w.sourceStatus.Store(status)
	return nil
}

// EnableRelay enables the functionality of start/watch/handle relay.
// According to relay schedule of DM-master, a source worker will enable relay in two scenarios: its bound source has
// `enable-relay: true` in config, or it has a UpstreamRelayWorkerKeyAdapter etcd KV.
// The paths to EnableRelay are:
// - source config `enable-relay: true`, which is checked in enableHandleSubtasks
//   - when DM-worker Server.Start
//   - when DM-worker Server watches a SourceBound change, which is to turn a free source worker to bound or notify a
//     bound worker that source config has changed
//   - when DM-worker Server fails watching and recovers from a snapshot
//
// - UpstreamRelayWorkerKeyAdapter
//   - when DM-worker Server.Start
//   - when DM-worker Server watches a UpstreamRelayWorkerKeyAdapter change
//   - when DM-worker Server fails watching and recovers from a snapshot
func (w *SourceWorker) EnableRelay(startBySourceCfg bool) (err error) {
	w.l.Info("enter EnableRelay")
	w.Lock()
	defer w.Unlock()
	if w.relayEnabled.Load() {
		w.l.Warn("already enabled relay")
		return nil
	}

	w.startedRelayBySourceCfg = startBySourceCfg

	failpoint.Inject("MockGetSourceCfgFromETCD", func(_ failpoint.Value) {
		failpoint.Goto("bypass")
	})
	// we need update config from etcd first in case this cfg is updated by master
	if refreshErr := w.refreshSourceCfg(); refreshErr != nil {
		return refreshErr
	}
	failpoint.Label("bypass")

	w.relayCtx, w.relayCancel = context.WithCancel(w.ctx)
	// 1. adjust relay starting position, to the earliest of subtasks
	var subTaskCfgs map[string]config.SubTaskConfig
	//nolint:dogsled
	_, _, subTaskCfgs, _, err = w.fetchSubTasksAndAdjust()
	if err != nil {
		return err
	}

	dctx, dcancel := context.WithTimeout(w.etcdClient.Ctx(), time.Duration(len(subTaskCfgs)*3)*time.Second)
	defer dcancel()
	minLoc, err1 := getMinLocInAllSubTasks(dctx, subTaskCfgs)
	if err1 != nil {
		w.l.Error("meet error when EnableRelay", zap.Error(err1))
	}

	if minLoc != nil {
		w.l.Info("get min location in all subtasks", zap.Stringer("location", *minLoc))
		w.cfg.RelayBinLogName = binlog.RemoveRelaySubDirSuffix(minLoc.Position).Name
		w.cfg.RelayBinlogGTID = minLoc.GTIDSetStr()
		// set UUIDSuffix when bound to a source
		w.cfg.UUIDSuffix, err = binlog.ExtractSuffix(minLoc.Position.Name)
		if err != nil {
			return err
		}
	} else {
		// set UUIDSuffix even not checkpoint exist
		// so we will still remove relay dir
		w.cfg.UUIDSuffix = binlog.MinRelaySubDirSuffix
	}

	// 2. initial relay holder, the cfg's password need decrypt
	// worker's relay-dir has higher priority than source's relay-dir
	if w.relayDir != "" {
		workerRelayDir := filepath.Join(w.relayDir, w.name)
		log.L().Info("use worker's relay-dir", zap.String("RelayDir", workerRelayDir))
		w.cfg.RelayDir = workerRelayDir
	}

	w.relayHolder = NewRelayHolder(w.cfg)
	relayPurger, err := w.relayHolder.Init(w.relayCtx, []relay.PurgeInterceptor{
		w,
	})
	if err != nil {
		return err
	}
	w.relayPurger = relayPurger

	// 3. get relay stage from etcd and check if need starting
	// we get the newest relay stages directly which will omit the relay stage PUT/DELETE event
	// because triggering these events is useless now
	relayStage, revRelay, err := ha.GetRelayStage(w.etcdClient, w.cfg.SourceID)
	if err != nil {
		// TODO: need retry
		return err
	}
	startImmediately := !relayStage.IsDeleted && relayStage.Expect == pb.Stage_Running
	if startImmediately {
		w.l.Info("start relay for existing relay stage")
		w.relayHolder.Start()
		w.relayPurger.Start()
	}

	// 4. watch relay stage
	w.relayWg.Add(1)
	go func() {
		defer w.relayWg.Done()
		// TODO: handle fatal error from observeRelayStage
		//nolint:errcheck
		w.observeRelayStage(w.relayCtx, w.etcdClient, revRelay)
	}()

	w.relayEnabled.Store(true)
	w.l.Info("relay enabled")
	w.subTaskHolder.resetAllSubTasks(w.getRelayWithoutLock())
	return nil
}

// DisableRelay disables the functionality of start/watch/handle relay.
// a source worker will disable relay by the reason of EnableRelay is no longer valid.
// The paths to DisableRelay are:
// - source config `enable-relay: true` no longer valid
//   - when DM-worker Server watches a SourceBound change, which is to notify that source config has changed, and the
//     worker has started relay by that bound
//   - when the source worker is unbound and has started relay by that bound
//
// - UpstreamRelayWorkerKeyAdapter no longer valid
//   - when DM-worker Server watches a UpstreamRelayWorkerKeyAdapter change
//   - when DM-worker Server fails watching and recovers from a snapshot
func (w *SourceWorker) DisableRelay() {
	w.l.Info("enter DisableRelay")
	w.Lock()
	defer w.Unlock()

	w.startedRelayBySourceCfg = false
	if !w.relayEnabled.CAS(true, false) {
		w.l.Warn("already disabled relay")
		return
	}

	w.relayCancel()
	w.relayWg.Wait()

	// refresh task checker know latest relayEnabled, to avoid accessing relayHolder
	if w.cfg.Checker.CheckEnable {
		w.l.Info("refresh task checker")
		w.taskStatusChecker.Close()
		w.taskStatusChecker.Start()
		w.l.Info("finish refreshing task checker")
	}

	w.subTaskHolder.resetAllSubTasks(nil)

	if w.relayHolder != nil {
		r := w.relayHolder
		w.relayHolder = nil
		r.Close()
	}
	if w.relayPurger != nil {
		r := w.relayPurger
		w.relayPurger = nil
		r.Close()
	}
	w.l.Info("relay disabled")
}

// EnableHandleSubtasks enables the functionality of start/watch/handle subtasks.
func (w *SourceWorker) EnableHandleSubtasks() error {
	w.l.Info("enter EnableHandleSubtasks")
	w.Lock()
	defer w.Unlock()
	if w.subTaskEnabled.Load() {
		w.l.Warn("already enabled handling subtasks")
		return nil
	}
	w.subTaskCtx, w.subTaskCancel = context.WithCancel(w.ctx)

	// we get the newest subtask stages directly which will omit the subtask stage PUT/DELETE event
	// because triggering these events is useless now
	subTaskStages, validatorStages, subTaskCfgM, revSubTask, err := w.fetchSubTasksAndAdjust()
	if err != nil {
		return err
	}

	w.l.Info("starting to handle mysql source", zap.String("sourceCfg", w.cfg.String()), zap.Any("subTasks", subTaskCfgM))

	for _, subTaskCfg := range subTaskCfgM {
		expectStage := subTaskStages[subTaskCfg.Name]
		if expectStage.IsDeleted {
			continue
		}
		validatorStage := pb.Stage_InvalidStage
		if s, ok := validatorStages[subTaskCfg.Name]; ok {
			validatorStage = s.Expect
		}
		w.l.Info("start to create subtask in EnableHandleSubtasks", zap.String("sourceID", subTaskCfg.SourceID), zap.String("task", subTaskCfg.Name))
		// "for range" of a map will use same value address, so we'd better not pass value address to other function
		clone := subTaskCfg
		if err2 := w.StartSubTask(&clone, expectStage.Expect, validatorStage, false); err2 != nil {
			w.subTaskHolder.closeAllSubTasks()
			return err2
		}
	}

	w.subTaskWg.Add(1)
	go func() {
		defer w.subTaskWg.Done()
		// TODO: handle fatal error from observeSubtaskStage
		//nolint:errcheck
		w.observeSubtaskStage(w.subTaskCtx, w.etcdClient, revSubTask)
	}()
	w.subTaskWg.Add(1)
	go func() {
		defer w.subTaskWg.Done()
		// TODO: handle fatal error from observeValidatorStage
		//nolint:errcheck
		w.observeValidatorStage(w.subTaskCtx, revSubTask)
	}()

	w.subTaskEnabled.Store(true)
	w.l.Info("handling subtask enabled")
	return nil
}

// DisableHandleSubtasks disables the functionality of start/watch/handle subtasks.
func (w *SourceWorker) DisableHandleSubtasks() {
	w.l.Info("enter DisableHandleSubtasks")
	if !w.subTaskEnabled.CAS(true, false) {
		w.l.Warn("already disabled handling subtasks")
		return
	}

	w.subTaskCancel()
	w.subTaskWg.Wait()

	w.Lock()
	defer w.Unlock()

	// close all sub tasks
	w.subTaskHolder.closeAllSubTasks()
	w.l.Info("handling subtask disabled")
}

// fetchSubTasksAndAdjust gets source's subtask stages and configs, adjust some values by worker's config and status
// source **must not be empty**
// return map{task name -> subtask stage}, map{task name -> validator stage}, map{task name -> subtask config}, revision, error.
func (w *SourceWorker) fetchSubTasksAndAdjust() (map[string]ha.Stage, map[string]ha.Stage, map[string]config.SubTaskConfig, int64, error) {
	// we get the newest subtask stages directly which will omit the subtask stage PUT/DELETE event
	// because triggering these events is useless now
	subTaskStages, validatorStages, subTaskCfgM, revSubTask, err := ha.GetSubTaskStageConfig(w.etcdClient, w.cfg.SourceID)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	if err = copyConfigFromSourceForEach(subTaskCfgM, w.cfg, w.relayEnabled.Load()); err != nil {
		return nil, nil, nil, 0, err
	}
	return subTaskStages, validatorStages, subTaskCfgM, revSubTask, nil
}

// StartSubTask creates a subtask and run it.
// TODO(ehco) rename this func.
func (w *SourceWorker) StartSubTask(cfg *config.SubTaskConfig, expectStage, validatorStage pb.Stage, needLock bool) error {
	if needLock {
		w.Lock()
		defer w.Unlock()
	}

	// copy some config item from dm-worker's source config
	err := copyConfigFromSource(cfg, w.cfg, w.relayEnabled.Load())
	if err != nil {
		return err
	}

	// directly put cfg into subTaskHolder
	// the uniqueness of subtask should be assured by etcd
	st := NewSubTask(cfg, w.etcdClient, w.name)
	w.subTaskHolder.recordSubTask(st)
	if w.closed.Load() {
		st.fail(terror.ErrWorkerAlreadyClosed.Generate())
		return nil
	}

	cfg2, err := cfg.DecryptedClone()
	if err != nil {
		st.fail(errors.Annotate(err, "start sub task"))
		return nil
	}
	st.cfg = cfg2
	// inject worker name to this subtask config
	st.cfg.WorkerName = w.name

	if w.relayEnabled.Load() && w.relayPurger.Purging() {
		// TODO: retry until purged finished
		st.fail(terror.ErrWorkerRelayIsPurging.Generate(cfg.Name))
		return nil
	}

	w.l.Info("subtask created", zap.Stringer("config", cfg2))
	st.Run(expectStage, validatorStage, w.getRelayWithoutLock())
	return nil
}

// caller should make sure w.Lock is locked before calling this method.
func (w *SourceWorker) getRelayWithoutLock() relay.Process {
	if w.relayHolder != nil {
		return w.relayHolder.Relay()
	}
	return nil
}

// UpdateSubTask update config for a sub task.
func (w *SourceWorker) UpdateSubTask(ctx context.Context, cfg *config.SubTaskConfig, needLock bool) error {
	if needLock {
		w.Lock()
		defer w.Unlock()
	}

	if w.closed.Load() {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(cfg.Name)
	if st == nil {
		return terror.ErrWorkerSubTaskNotFound.Generate(cfg.Name)
	}

	w.l.Info("update sub task", zap.String("task", cfg.Name))
	return st.Update(ctx, cfg)
}

// OperateSubTask stop/resume/pause sub task.
func (w *SourceWorker) OperateSubTask(name string, op pb.TaskOp) error {
	w.Lock()
	defer w.Unlock()

	if w.closed.Load() {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(name)
	if st == nil {
		return terror.ErrWorkerSubTaskNotFound.Generate(name)
	}

	w.l.Info("OperateSubTask start", zap.Stringer("op", op), zap.String("task", name))
	var err error
	switch op {
	case pb.TaskOp_Delete:
		w.l.Info("delete subtask", zap.String("task", name))
		st.Close()
		w.subTaskHolder.removeSubTask(name)
	case pb.TaskOp_Pause, pb.TaskOp_Stop:
		w.l.Info("pause subtask", zap.String("task", name))
		err = st.Pause()
	case pb.TaskOp_Resume:
		failpoint.Inject("SkipRefreshFromETCDInUT", func(_ failpoint.Value) {
			failpoint.Goto("bypassRefresh")
		})
		if refreshErr := w.tryRefreshSubTaskAndSourceConfig(st); refreshErr != nil {
			// NOTE: for current unit is not syncer unit or is in shard merge.
			w.l.Warn("can not update subtask config now", zap.Error(refreshErr))
		}
		failpoint.Label("bypassRefresh")
		w.l.Info("resume subtask", zap.String("task", name))
		err = st.Resume(w.getRelayWithoutLock())
	case pb.TaskOp_AutoResume:
		// TODO(ehco) change to auto_restart
		w.l.Info("auto_resume subtask", zap.String("task", name))
		err = st.Resume(w.getRelayWithoutLock())
	default:
		err = terror.ErrWorkerUpdateTaskStage.Generatef("invalid operate %s on subtask %v", op, name)
	}
	w.l.Info("OperateSubTask finished", zap.Stringer("op", op), zap.String("task", name))
	return err
}

// QueryStatus query worker's sub tasks' status. If relay enabled, also return source status.
func (w *SourceWorker) QueryStatus(ctx context.Context, name string) ([]*pb.SubTaskStatus, *pb.RelayStatus, error) {
	w.RLock()
	defer w.RUnlock()

	if w.closed.Load() {
		w.l.Warn("querying status from a closed worker")
		return nil, nil, nil
	}

	var (
		sourceStatus *binlog.SourceStatus
		relayStatus  *pb.RelayStatus
	)

	if err := w.updateSourceStatus(ctx, false); err != nil {
		if terror.ErrNoMasterStatus.Equal(err) {
			w.l.Warn("This source's bin_log is OFF, so it only supports full_mode.", zap.String("sourceID", w.cfg.SourceID), zap.Error(err))
		} else {
			w.l.Error("failed to update source status", zap.Error(err))
		}
	} else {
		sourceStatus = w.sourceStatus.Load().(*binlog.SourceStatus)
	}

	subtaskStatus := w.Status(name, sourceStatus)
	if w.relayEnabled.Load() {
		relayStatus = w.relayHolder.Status(sourceStatus)
	}
	return subtaskStatus, relayStatus, nil
}

func (w *SourceWorker) resetSubtaskStage() (int64, error) {
	subTaskStages, _, subTaskCfgm, revSubTask, err := w.fetchSubTasksAndAdjust()
	if err != nil {
		return 0, err
	}
	// use sts to check which subtask has no subtaskCfg or subtaskStage now
	sts := w.subTaskHolder.getAllSubTasks()
	for name, subtaskCfg := range subTaskCfgm {
		stage, ok := subTaskStages[name]
		if ok {
			// TODO: right operation sequences may get error when we get etcdErrCompact, need to handle it later
			// For example, Expect: Running -(pause)-> Paused -(resume)-> Running
			// we get an etcd compact error at the first running. If we try to "resume" it now, we will get an error
			opType, err2 := w.operateSubTaskStage(stage, subtaskCfg)
			if err2 != nil {
				opErrCounter.WithLabelValues(w.name, opType).Inc()
				log.L().Error("fail to operate subtask stage", zap.Stringer("stage", stage),
					zap.String("task", subtaskCfg.Name), zap.Error(err2))
			}
			delete(sts, name)
		}
	}
	// remove subtasks without subtask config or subtask stage
	for name := range sts {
		err = w.OperateSubTask(name, pb.TaskOp_Delete)
		if err != nil {
			opErrCounter.WithLabelValues(w.name, pb.TaskOp_Delete.String()).Inc()
			log.L().Error("fail to stop subtask", zap.String("task", name), zap.Error(err))
		}
	}
	return revSubTask, nil
}

func (w *SourceWorker) observeSubtaskStage(ctx context.Context, etcdCli *clientv3.Client, rev int64) error {
	var wg sync.WaitGroup

	for {
		subTaskStageCh := make(chan ha.Stage, 10)
		subTaskErrCh := make(chan error, 10)
		wg.Add(1)
		// use ctx1, cancel1 to make sure old watcher has been released
		ctx1, cancel1 := context.WithCancel(ctx)
		go func() {
			defer func() {
				close(subTaskStageCh)
				close(subTaskErrCh)
				wg.Done()
			}()
			ha.WatchSubTaskStage(ctx1, etcdCli, w.cfg.SourceID, rev+1, subTaskStageCh, subTaskErrCh)
		}()
		err := w.handleSubTaskStage(ctx1, subTaskStageCh, subTaskErrCh)
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
					rev, err = w.resetSubtaskStage()
					if err != nil {
						log.L().Error("resetSubtaskStage is failed, will retry later", zap.Error(err), zap.Int("retryNum", retryNum))
					}
				}
				retryNum++
			}
		} else {
			if err != nil {
				log.L().Error("observeSubtaskStage is failed and will quit now", zap.Error(err))
			} else {
				log.L().Info("observeSubtaskStage will quit now")
			}
			return err
		}
	}
}

func (w *SourceWorker) handleSubTaskStage(ctx context.Context, stageCh chan ha.Stage, errCh chan error) error {
	closed := false
	for {
		select {
		case <-ctx.Done():
			closed = true
		case stage, ok := <-stageCh:
			if !ok {
				closed = true
				break
			}
			log.L().Info("receive subtask stage change", zap.Stringer("stage", stage), zap.Bool("is deleted", stage.IsDeleted))
			opType, err := w.operateSubTaskStageWithoutConfig(stage)
			if err != nil {
				opErrCounter.WithLabelValues(w.name, opType).Inc()
				log.L().Error("fail to operate subtask stage", zap.Stringer("stage", stage), zap.Bool("is deleted", stage.IsDeleted), zap.Error(err))
				if etcdutil.IsRetryableError(err) {
					return err
				}
			}
		case err, ok := <-errCh:
			if !ok {
				closed = true
				break
			}
			// TODO: deal with err
			log.L().Error("WatchSubTaskStage received an error", zap.Error(err))
			if etcdutil.IsRetryableError(err) {
				return err
			}
		}
		if closed {
			log.L().Info("worker is closed, handleSubTaskStage will quit now")
			return nil
		}
	}
}

// operateSubTaskStage returns TaskOp.String() additionally to record metrics.
func (w *SourceWorker) operateSubTaskStage(stage ha.Stage, subTaskCfg config.SubTaskConfig) (string, error) {
	var op pb.TaskOp
	log.L().Info("operateSubTaskStage",
		zap.String("sourceID", subTaskCfg.SourceID),
		zap.String("task", subTaskCfg.Name),
		zap.Stringer("stage", stage))

	// for new added subtask
	if st := w.subTaskHolder.findSubTask(stage.Task); st == nil {
		switch stage.Expect {
		case pb.Stage_Running, pb.Stage_Paused, pb.Stage_Stopped:
			// todo refactor here deciding if the expected stage is valid should be put inside StartSubTask and OperateSubTask
			log.L().Info("start to create subtask in operateSubTaskStage", zap.String("sourceID", subTaskCfg.SourceID), zap.String("task", subTaskCfg.Name))
			expectValidatorStage, err := getExpectValidatorStage(subTaskCfg.ValidatorCfg, w.etcdClient, stage.Source, stage.Task, stage.Revision)
			if err != nil {
				return opErrTypeBeforeOp, terror.Annotate(err, "fail to get validator stage from etcd")
			}
			return opErrTypeBeforeOp, w.StartSubTask(&subTaskCfg, stage.Expect, expectValidatorStage, true)
		default:
			// not valid stage
			return op.String(), w.OperateSubTask(stage.Task, op)
		}
	}
	// todo(ehco) remove pause and resume after using openapi to impl dmctl
	switch stage.Expect {
	case pb.Stage_Stopped, pb.Stage_Paused:
		op = pb.TaskOp_Pause
	case pb.Stage_Running:
		op = pb.TaskOp_Resume
	}
	if stage.IsDeleted {
		op = pb.TaskOp_Delete
	}
	return op.String(), w.OperateSubTask(stage.Task, op)
}

// operateSubTaskStageWithoutConfig returns TaskOp additionally to record metrics.
func (w *SourceWorker) operateSubTaskStageWithoutConfig(stage ha.Stage) (string, error) {
	var subTaskCfg config.SubTaskConfig
	if stage.Expect == pb.Stage_Running || stage.Expect == pb.Stage_Stopped {
		if st := w.subTaskHolder.findSubTask(stage.Task); st == nil {
			tsm, _, err := ha.GetSubTaskCfg(w.etcdClient, stage.Source, stage.Task, stage.Revision)
			if err != nil {
				// TODO: need retry
				return opErrTypeBeforeOp, terror.Annotate(err, "fail to get subtask config from etcd")
			}
			var ok bool
			if subTaskCfg, ok = tsm[stage.Task]; !ok {
				return opErrTypeBeforeOp, terror.ErrWorkerFailToGetSubtaskConfigFromEtcd.Generate(stage.Task)
			}
		}
	}
	return w.operateSubTaskStage(stage, subTaskCfg)
}

func (w *SourceWorker) observeRelayStage(ctx context.Context, etcdCli *clientv3.Client, rev int64) error {
	var wg sync.WaitGroup
	for {
		relayStageCh := make(chan ha.Stage, 10)
		relayErrCh := make(chan error, 10)
		wg.Add(1)
		// use ctx1, cancel1 to make sure old watcher has been released
		ctx1, cancel1 := context.WithCancel(ctx)
		go func() {
			defer func() {
				close(relayStageCh)
				close(relayErrCh)
				wg.Done()
			}()
			ha.WatchRelayStage(ctx1, etcdCli, w.cfg.SourceID, rev+1, relayStageCh, relayErrCh)
		}()
		err := w.handleRelayStage(ctx1, relayStageCh, relayErrCh)
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
					stage, rev1, err1 := ha.GetRelayStage(etcdCli, w.cfg.SourceID)
					if err1 != nil {
						log.L().Error("get source bound from etcd failed, will retry later", zap.Error(err1), zap.Int("retryNum", retryNum))
						break
					}
					rev = rev1
					if stage.IsEmpty() {
						stage.IsDeleted = true
					}
					opType, err1 := w.operateRelayStage(ctx, stage)
					if err1 != nil {
						opErrCounter.WithLabelValues(w.name, opType).Inc()
						log.L().Error("fail to operate relay", zap.Stringer("stage", stage), zap.Bool("is deleted", stage.IsDeleted), zap.Error(err1))
					}
				}
				retryNum++
			}
		} else {
			if err != nil {
				log.L().Error("observeRelayStage is failed and will quit now", zap.Error(err))
			} else {
				log.L().Info("observeRelayStage will quit now")
			}
			return err
		}
	}
}

func (w *SourceWorker) handleRelayStage(ctx context.Context, stageCh chan ha.Stage, errCh chan error) error {
OUTER:
	for {
		select {
		case <-ctx.Done():
			break OUTER
		case stage, ok := <-stageCh:
			if !ok {
				break OUTER
			}
			log.L().Info("receive relay stage change", zap.Stringer("stage", stage), zap.Bool("is deleted", stage.IsDeleted))
			opType, err := w.operateRelayStage(ctx, stage)
			if err != nil {
				opErrCounter.WithLabelValues(w.name, opType).Inc()
				log.L().Error("fail to operate relay", zap.Stringer("stage", stage), zap.Bool("is deleted", stage.IsDeleted), zap.Error(err))
			}
		case err, ok := <-errCh:
			if !ok {
				break OUTER
			}
			log.L().Error("WatchRelayStage received an error", zap.Error(err))
			if etcdutil.IsRetryableError(err) {
				return err
			}
		}
	}
	log.L().Info("worker is closed, handleRelayStage will quit now")
	return nil
}

// operateRelayStage returns RelayOp.String() additionally to record metrics
// *RelayOp is nil only when error is nil, so record on error will not meet nil-pointer deference.
func (w *SourceWorker) operateRelayStage(ctx context.Context, stage ha.Stage) (string, error) {
	var op pb.RelayOp
	switch {
	case stage.Expect == pb.Stage_Running:
		if w.relayHolder.Stage() == pb.Stage_New {
			w.relayHolder.Start()
			w.relayPurger.Start()
			return opErrTypeBeforeOp, nil
		}
		op = pb.RelayOp_ResumeRelay
	case stage.Expect == pb.Stage_Paused:
		op = pb.RelayOp_PauseRelay
	case stage.IsDeleted:
		op = pb.RelayOp_StopRelay
	}
	return op.String(), w.operateRelay(ctx, op)
}

// OperateRelay operates relay unit.
func (w *SourceWorker) operateRelay(ctx context.Context, op pb.RelayOp) error {
	if w.closed.Load() {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	if w.relayEnabled.Load() {
		// TODO: lock the worker?
		return w.relayHolder.Operate(ctx, op)
	}

	w.l.Warn("enable-relay is false, ignore operate relay")
	return nil
}

// PurgeRelay purges relay log files.
func (w *SourceWorker) PurgeRelay(ctx context.Context, req *pb.PurgeRelayRequest) error {
	if w.closed.Load() {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	if !w.relayEnabled.Load() {
		w.l.Warn("enable-relay is false, ignore purge relay")
		return nil
	}

	if !w.subTaskEnabled.Load() {
		w.l.Info("worker received purge-relay but didn't handling subtasks, read global checkpoint to decided active relay log")

		subDir := w.relayHolder.Status(nil).RelaySubDir

		_, _, subTaskCfgs, _, err := w.fetchSubTasksAndAdjust()
		if err != nil {
			return err
		}
		for _, subTaskCfg := range subTaskCfgs {
			loc, err2 := getMinLocForSubTaskFunc(ctx, subTaskCfg)
			if err2 != nil {
				return err2
			}
			w.l.Info("update active relay log with",
				zap.String("task name", subTaskCfg.Name),
				zap.String("subDir", subDir),
				zap.String("binlog name", loc.Position.Name))
			if err3 := streamer.GetReaderHub().UpdateActiveRelayLog(subTaskCfg.Name, subDir, loc.Position.Name); err3 != nil {
				w.l.Error("Error when update active relay log", zap.Error(err3))
			}
		}
	}
	return w.relayPurger.Do(ctx, req)
}

// ForbidPurge implements PurgeInterceptor.ForbidPurge.
func (w *SourceWorker) ForbidPurge() (bool, string) {
	if w.closed.Load() {
		return false, ""
	}

	// forbid purging if some sub tasks are paused, so we can debug the system easily
	// This function is not protected by `w.RWMutex`, which may lead to sub tasks information
	// not up to date, but do not affect correctness.
	for _, st := range w.subTaskHolder.getAllSubTasks() {
		stage := st.Stage()
		if stage == pb.Stage_New || stage == pb.Stage_Paused {
			return true, fmt.Sprintf("sub task %s current stage is %s", st.cfg.Name, stage.String())
		}
	}
	return false, ""
}

// OperateSchema operates schema for an upstream table.
func (w *SourceWorker) OperateSchema(ctx context.Context, req *pb.OperateWorkerSchemaRequest) (schema string, err error) {
	w.Lock()
	defer w.Unlock()

	if w.closed.Load() {
		return "", terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(req.Task)
	if st == nil {
		return "", terror.ErrWorkerSubTaskNotFound.Generate(req.Task)
	}

	return st.OperateSchema(ctx, req)
}

// copyConfigFromSource copies config items from source config and worker's relayEnabled to sub task.
func copyConfigFromSource(cfg *config.SubTaskConfig, sourceCfg *config.SourceConfig, enableRelay bool) error {
	cfg.From = sourceCfg.From

	cfg.Flavor = sourceCfg.Flavor
	cfg.ServerID = sourceCfg.ServerID
	cfg.RelayDir = sourceCfg.RelayDir
	cfg.EnableGTID = sourceCfg.EnableGTID
	cfg.UseRelay = enableRelay

	if cfg.CaseSensitive != sourceCfg.CaseSensitive {
		log.L().Warn("different case-sensitive config between task config and source config, use `true` for it.")
	}
	cfg.CaseSensitive = cfg.CaseSensitive || sourceCfg.CaseSensitive
	filter, err := bf.NewBinlogEvent(cfg.CaseSensitive, cfg.FilterRules)
	if err != nil {
		return err
	}

	for _, filterRule := range sourceCfg.Filters {
		if err = filter.AddRule(filterRule); err != nil {
			// task level config has higher priority
			if errors.IsAlreadyExists(errors.Cause(err)) {
				log.L().Warn("filter config already exist in source config, overwrite it", log.ShortError(err))
				continue
			}
			return err
		}
		cfg.FilterRules = append(cfg.FilterRules, filterRule)
	}
	return nil
}

// copyConfigFromSourceForEach do copyConfigFromSource for each value in subTaskCfgM and change subTaskCfgM in-place.
func copyConfigFromSourceForEach(
	subTaskCfgM map[string]config.SubTaskConfig,
	sourceCfg *config.SourceConfig,
	enableRelay bool,
) error {
	for k, subTaskCfg := range subTaskCfgM {
		if err2 := copyConfigFromSource(&subTaskCfg, sourceCfg, enableRelay); err2 != nil {
			return err2
		}
		subTaskCfgM[k] = subTaskCfg
	}
	return nil
}

// getAllSubTaskStatus returns all subtask status of this worker, note the field
// in subtask status is not completed, only includes `Name`, `Stage` and `Result` now.
func (w *SourceWorker) getAllSubTaskStatus() map[string]*pb.SubTaskStatus {
	sts := w.subTaskHolder.getAllSubTasks()
	result := make(map[string]*pb.SubTaskStatus, len(sts))
	for name, st := range sts {
		st.RLock()
		result[name] = &pb.SubTaskStatus{
			Name:   name,
			Stage:  st.stage,
			Result: proto.Clone(st.result).(*pb.ProcessResult),
		}
		st.RUnlock()
	}
	return result
}

// HandleError handle worker error.
func (w *SourceWorker) HandleError(ctx context.Context, req *pb.HandleWorkerErrorRequest) (string, error) {
	w.Lock()
	defer w.Unlock()

	if w.closed.Load() {
		return "", terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(req.Task)
	if st == nil {
		return "", terror.ErrWorkerSubTaskNotFound.Generate(req.Task)
	}

	return st.HandleError(ctx, req, w.getRelayWithoutLock())
}

func (w *SourceWorker) observeValidatorStage(ctx context.Context, lastUsedRev int64) error {
	var wg sync.WaitGroup

	startRevision := lastUsedRev + 1
	for {
		stageCh := make(chan ha.Stage, 10)
		errCh := make(chan error, 10)
		wg.Add(1)
		watchCtx, watchCancel := context.WithCancel(ctx)
		go func() {
			defer func() {
				close(stageCh)
				close(errCh)
				wg.Done()
			}()
			ha.WatchValidatorStage(watchCtx, w.etcdClient, w.cfg.SourceID, startRevision, stageCh, errCh)
		}()
		err := w.handleValidatorStage(watchCtx, stageCh, errCh)
		watchCancel()
		wg.Wait()

		if etcdutil.IsRetryableError(err) {
			startRevision = 0
			retryNum := 1
			for startRevision == 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(500 * time.Millisecond):
					w.RLock()
					sourceID := w.cfg.SourceID
					w.RUnlock()
					startRevision, err = w.getCurrentValidatorRevision(sourceID)
					if err != nil {
						log.L().Error("reset validator stage failed, will retry later", zap.Error(err), zap.Int("retryNum", retryNum))
					}
				}
				retryNum++
			}
		} else {
			if err != nil {
				log.L().Error("observe validator stage failed, quit now", zap.Error(err))
			} else {
				log.L().Info("observe validator stage will quit now")
			}
			return err
		}
	}
}

func (w *SourceWorker) handleValidatorStage(ctx context.Context, stageCh chan ha.Stage, errCh chan error) error {
	closed := false
	for {
		select {
		case <-ctx.Done():
			closed = true
		case stage, ok := <-stageCh:
			if !ok {
				closed = true
				break
			}
			log.L().Info("receive validator stage change", zap.Stringer("stage", stage), zap.Bool("is deleted", stage.IsDeleted))
			err := w.operateValidatorStage(stage)
			if err != nil {
				opType := w.getValidatorOp(stage)
				opErrCounter.WithLabelValues(w.name, opType).Inc()
				log.L().Error("fail to operate validator stage", zap.Stringer("stage", stage), zap.Bool("is deleted", stage.IsDeleted), zap.Error(err))
				if etcdutil.IsRetryableError(err) {
					return err
				}
			}
		case err, ok := <-errCh:
			if !ok {
				closed = true
				break
			}
			// TODO: deal with err
			log.L().Error("watch validator stage received an error", zap.Error(err))
			if etcdutil.IsRetryableError(err) {
				return err
			}
		}
		if closed {
			log.L().Info("worker is closed, handle validator stage will quit now")
			return nil
		}
	}
}

func (w *SourceWorker) getCurrentValidatorRevision(source string) (int64, error) {
	_, rev, err := ha.GetValidatorStage(w.etcdClient, source, "", 0)
	if err != nil {
		return 0, err
	}
	return rev, nil
}

func (w *SourceWorker) getValidatorOp(stage ha.Stage) string {
	if stage.IsDeleted {
		return "validator-delete"
	}
	if stage.Expect == pb.Stage_Running {
		return pb.ValidatorOp_StartValidator.String()
	} else if stage.Expect == pb.Stage_Stopped {
		return pb.ValidatorOp_StopValidator.String()
	}
	// should not happen
	return ""
}

func (w *SourceWorker) operateValidatorStage(stage ha.Stage) error {
	// if the key it's deleted, the subtask is deleted too, let subtask clean it up.
	if stage.IsDeleted {
		return nil
	}

	subtask := w.subTaskHolder.findSubTask(stage.Task)
	if subtask == nil {
		// when a new subtask start with validator, both subtask and validator stage observer will observe it,
		// if validator observe it first, we may not have the subtask.
		log.L().Info("cannot find subtask. maybe it's a new task, let subtask stage observer handles it")
		return nil
	}

	// stage of validator can only be Running or Stopped
	switch stage.Expect {
	case pb.Stage_Stopped:
		subtask.StopValidator()
	case pb.Stage_Running:
		// validator's config is stored with subtask config, we need to update subtask config as validator may start
		// on the fly.
		subTaskCfg, _, err := ha.GetSubTaskCfg(w.etcdClient, stage.Source, stage.Task, stage.Revision)
		if err != nil {
			return err
		}
		targetCfg, ok := subTaskCfg[stage.Task]
		if !ok {
			log.L().Error("failed to get subtask config", zap.Reflect("stage", stage))
			return errors.New("failed to get subtask config")
		}
		subtask.UpdateValidatorCfg(targetCfg.ValidatorCfg)
		subtask.StartValidator(stage.Expect, false)
	default:
		// should not happen
		log.L().Warn("invalid validator stage", zap.Reflect("stage", stage))
	}
	return nil
}

func (w *SourceWorker) refreshSourceCfg() error {
	oldCfg := w.cfg
	sourceCfgM, _, err := ha.GetSourceCfg(w.etcdClient, oldCfg.SourceID, 0)
	if err != nil {
		return err
	}
	w.cfg = sourceCfgM[oldCfg.SourceID]
	return nil
}

func (w *SourceWorker) tryRefreshSubTaskAndSourceConfig(subTask *SubTask) error {
	// try refresh source config first
	if err := w.refreshSourceCfg(); err != nil {
		return err
	}
	sourceName := subTask.cfg.SourceID
	taskName := subTask.cfg.Name
	tsm, _, err := ha.GetSubTaskCfg(w.etcdClient, sourceName, taskName, 0)
	if err != nil {
		return terror.Annotate(err, "fail to get subtask config from etcd")
	}

	var cfg config.SubTaskConfig
	var ok bool
	if cfg, ok = tsm[taskName]; !ok {
		return terror.ErrWorkerFailToGetSubtaskConfigFromEtcd.Generate(taskName)
	}

	// copy some config item from dm-worker's source config
	if err := copyConfigFromSource(&cfg, w.cfg, w.relayEnabled.Load()); err != nil {
		return err
	}
	if checkErr := subTask.CheckUnitCfgCanUpdate(&cfg); checkErr != nil {
		return checkErr
	}
	return w.UpdateSubTask(w.ctx, &cfg, false)
}

// CheckCfgCanUpdated check if current subtask config can be updated.
func (w *SourceWorker) CheckCfgCanUpdated(cfg *config.SubTaskConfig) error {
	w.RLock()
	defer w.RUnlock()

	subTask := w.subTaskHolder.findSubTask(cfg.Name)
	if subTask == nil {
		return terror.ErrWorkerSubTaskNotFound.Generate(cfg.Name)
	}
	// copy some config item from dm-worker's source config
	if err := copyConfigFromSource(cfg, w.cfg, w.relayEnabled.Load()); err != nil {
		return err
	}
	return subTask.CheckUnitCfgCanUpdate(cfg)
}

func (w *SourceWorker) GetWorkerValidatorErr(taskName string, errState pb.ValidateErrorState) ([]*pb.ValidationError, error) {
	st := w.subTaskHolder.findSubTask(taskName)
	if st != nil {
		return st.GetValidatorError(errState)
	}
	return nil, terror.ErrWorkerSubTaskNotFound.Generate(taskName)
}

func (w *SourceWorker) OperateWorkerValidatorErr(taskName string, op pb.ValidationErrOp, errID uint64, isAll bool) error {
	st := w.subTaskHolder.findSubTask(taskName)
	if st != nil {
		return st.OperateValidatorError(op, errID, isAll)
	}
	return terror.ErrWorkerSubTaskNotFound.Generate(taskName)
}

func (w *SourceWorker) GetValidatorStatus(taskName string) (*pb.ValidationStatus, error) {
	st := w.subTaskHolder.findSubTask(taskName)
	if st == nil {
		return nil, terror.ErrWorkerSubTaskNotFound.Generate(taskName)
	}
	return st.GetValidatorStatus()
}

func (w *SourceWorker) GetValidatorTableStatus(taskName string, filterStatus pb.Stage) ([]*pb.ValidationTableStatus, error) {
	st := w.subTaskHolder.findSubTask(taskName)
	if st == nil {
		return nil, terror.ErrWorkerSubTaskNotFound.Generate(taskName)
	}
	return st.GetValidatorTableStatus(filterStatus)
}

func (w *SourceWorker) UpdateWorkerValidator(req *pb.UpdateValidationWorkerRequest) error {
	st := w.subTaskHolder.findSubTask(req.TaskName)
	if st == nil {
		return terror.ErrWorkerSubTaskNotFound.Generate(req.TaskName)
	}
	return st.UpdateValidator(req)
}
