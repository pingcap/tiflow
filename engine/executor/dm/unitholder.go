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

	"github.com/pingcap/errors"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/lib"
)

// unitHolder hold a unit of DM
type unitHolder interface {
	Init(ctx context.Context) error
	Close(ctx context.Context) error
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
	Stage() (metadata.TaskStage, *pb.ProcessResult)
	Status(ctx context.Context) interface{}
}

// unitHolderImpl wrap the dm-worker unit.
type unitHolderImpl struct {
	unit unit.Unit

	// use to access process(init/close/pause/resume)
	processMu sync.RWMutex
	processWg sync.WaitGroup
	// use to access field(ctx/result)
	fieldMu   sync.RWMutex
	runCtx    context.Context
	runCancel context.CancelFunc
	result    *pb.ProcessResult // TODO: check if framework can persist result
}

// newUnitHolderImpl creates a UnitHolderImpl
func newUnitHolderImpl(workerType lib.WorkerType, cfg *dmconfig.SubTaskConfig) *unitHolderImpl {
	unitHolder := &unitHolderImpl{}
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
	u.processMu.Lock()
	defer u.processMu.Unlock()

	if err := u.unit.Init(ctx); err != nil {
		return err
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	u.fieldMu.Lock()
	u.runCtx, u.runCancel = runCtx, runCancel
	u.fieldMu.Unlock()

	resultCh := make(chan pb.ProcessResult, 1)
	u.processWg.Add(1)
	go func() {
		defer u.processWg.Done()
		u.unit.Process(runCtx, resultCh)
		u.fetchAndHandleResult(resultCh)
	}()
	return nil
}

func (u *unitHolderImpl) Pause(ctx context.Context) error {
	u.processMu.Lock()
	defer u.processMu.Unlock()

	stage, _ := u.Stage()
	if stage != metadata.StageRunning && stage != metadata.StageError {
		return errors.Errorf("failed to pause unit with stage %d", stage)
	}

	// cancel process
	u.fieldMu.Lock()
	u.runCancel()
	u.fieldMu.Unlock()
	u.processWg.Wait()
	// TODO: refactor unit.Syncer
	// unit needs to manage its own life cycle
	u.unit.Pause()
	return nil
}

func (u *unitHolderImpl) Resume(ctx context.Context) error {
	u.processMu.Lock()
	defer u.processMu.Unlock()

	stage, _ := u.Stage()
	if stage != metadata.StagePaused && stage != metadata.StageError {
		return errors.Errorf("failed to resume unit with stage %d", stage)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	// run new process
	u.fieldMu.Lock()
	u.runCtx, u.runCancel = runCtx, runCancel
	u.result = nil
	u.fieldMu.Unlock()

	resultCh := make(chan pb.ProcessResult, 1)
	u.processWg.Add(1)
	go func() {
		defer u.processWg.Done()
		u.unit.Resume(runCtx, resultCh)
		u.fetchAndHandleResult(resultCh)
	}()
	return nil
}

// Close implement UnitHolder.Close
func (u *unitHolderImpl) Close(ctx context.Context) error {
	u.processMu.Lock()
	defer u.processMu.Unlock()

	u.fieldMu.Lock()
	// cancel process
	u.runCancel()
	u.fieldMu.Unlock()

	u.processWg.Wait()
	u.unit.Close()
	return nil
}

// Stage implement UnitHolder.Stage
func (u *unitHolderImpl) Stage() (metadata.TaskStage, *pb.ProcessResult) {
	u.fieldMu.RLock()
	ctx := u.runCtx
	result := u.result
	u.fieldMu.RUnlock()

	var canceled bool
	select {
	case <-ctx.Done():
		canceled = true
	default:
	}

	switch {
	case canceled && result == nil:
		return metadata.StagePausing, nil
	case canceled && result != nil:
		return metadata.StagePaused, result
	case !canceled && result == nil:
		return metadata.StageRunning, nil
	// !canceled && result != nil
	case len(result.Errors) == 0:
		return metadata.StageFinished, result
	default:
		return metadata.StageError, result
	}
}

// Status implement UnitHolder.Status
func (u *unitHolderImpl) Status(ctx context.Context) interface{} {
	return u.unit.Status(nil)
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

func (u *unitHolderImpl) fetchAndHandleResult(resultCh chan pb.ProcessResult) {
	r := <-resultCh
	filterErrors(&r)
	if len(r.Errors) > 0 {
		// TODO: refactor unit.Syncer
		// unit needs to manage its own life cycle
		u.unit.Pause()
	}
	u.fieldMu.Lock()
	u.result = &r
	u.fieldMu.Unlock()
}
