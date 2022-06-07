// Copyright 2022 PingCAP, Inc.
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

package dm

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/dm/config"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/dm/worker"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pkg/backoff"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/lib"
	"go.uber.org/zap"
)

// unitHolder hold a unit of DM
type unitHolder interface {
	Init(ctx context.Context) error
	Tick(ctx context.Context) error
	Stage() metadata.TaskStage
	Status(ctx context.Context) interface{}
	Close(ctx context.Context) error
}

// unitHolderImpl wrap the dm-worker unit.
type unitHolderImpl struct {
	autoResume *worker.AutoResumeInfo

	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.RWMutex
	unit       unit.Unit
	resultCh   chan pb.ProcessResult
	lastResult *pb.ProcessResult // TODO: check if framework can persist result
}

// newUnitHolderImpl creates a UnitHolderImpl
func newUnitHolderImpl(workerType lib.WorkerType, cfg *dmconfig.SubTaskConfig) *unitHolderImpl {
	// TODO: support config later
	// nolint:errcheck
	bf, _ := backoff.NewBackoff(
		config.DefaultBackoffFactor,
		config.DefaultBackoffJitter,
		config.DefaultBackoffMin,
		config.DefaultBackoffMax)
	autoResume := &worker.AutoResumeInfo{
		Backoff:          bf,
		LatestPausedTime: time.Now(),
		LatestResumeTime: time.Now(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	unitHolder := &unitHolderImpl{
		ctx:        ctx,
		cancel:     cancel,
		autoResume: autoResume,
		resultCh:   make(chan pb.ProcessResult, 1),
	}
	switch workerType {
	case lib.WorkerDMDump:
		unitHolder.unit = dumpling.NewDumpling(cfg)
	case lib.WorkerDMLoad:
		unitHolder.unit = loader.NewLightning(cfg, nil, "dataflow-worker")
	case lib.WorkerDMSync:
		unitHolder.unit = syncer.NewSyncer(cfg, nil, nil)
	}
	return unitHolder
}

// Init implement UnitHolder.Init
func (u *unitHolderImpl) Init(ctx context.Context) error {
	if err := u.unit.Init(ctx); err != nil {
		return err
	}

	go u.unit.Process(u.ctx, u.resultCh)
	return nil
}

func filterErrors(r *pb.ProcessResult) {
	errs := make([]*pb.ProcessError, 0, 2)
	for _, err := range r.Errors {
		if !unit.IsCtxCanceledProcessErr(err) {
			errs = append(errs, err)
		}
	}
	r.Errors = errs
}

func (u *unitHolderImpl) fetchAndHandleResult() {
	if r := u.getResult(); r != nil {
		return
	}

	select {
	case r := <-u.resultCh:
		filterErrors(&r)
		if len(r.Errors) > 0 {
			// TODO: refactor unit.Syncer
			// unit needs to manage its own life cycle
			u.unit.Pause()
		}
		u.setResult(&r)
	default:
	}
}

func (u *unitHolderImpl) setResult(r *pb.ProcessResult) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.lastResult = r
}

func (u *unitHolderImpl) getResult() *pb.ProcessResult {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.lastResult
}

func (u *unitHolderImpl) checkAndAutoResume() {
	result := u.getResult()
	if result == nil || len(result.Errors) == 0 {
		return
	}

	log.L().Error("task runs with error", zap.Any("error msg", result.Errors))
	subtaskStage := &pb.SubTaskStatus{
		Stage:  pb.Stage_Paused,
		Result: result,
	}
	strategy := u.autoResume.CheckResumeSubtask(subtaskStage, config.DefaultBackoffRollback)
	log.L().Info("got auto resume strategy", zap.Stringer("strategy", strategy))

	if strategy == worker.ResumeDispatch {
		log.L().Info("dispatch auto resume task")
		u.setResult(nil)
		// TODO: manage goroutines
		go u.unit.Resume(u.ctx, u.resultCh)
	}
}

// Tick implement UnitHolder.Tick
func (u *unitHolderImpl) Tick(ctx context.Context) error {
	u.fetchAndHandleResult()
	u.checkAndAutoResume()
	return nil
}

// Close implement UnitHolder.Close
func (u *unitHolderImpl) Close(ctx context.Context) error {
	u.cancel()
	u.unit.Close()
	return nil
}

// Stage implement UnitHolder.Stage
func (u *unitHolderImpl) Stage() metadata.TaskStage {
	result := u.getResult()
	switch {
	case result == nil:
		return metadata.StageRunning
	// TODO: support OperateTask
	case len(result.Errors) == 0:
		return metadata.StageFinished
	default:
		return metadata.StagePaused
	}
}

// Status implement UnitHolder.Status
func (u *unitHolderImpl) Status(ctx context.Context) interface{} {
	return u.unit.Status(nil)
}
