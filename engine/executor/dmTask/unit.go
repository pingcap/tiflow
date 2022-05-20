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

package dmtask

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/dm/worker"
	"github.com/pingcap/tiflow/dm/pkg/backoff"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/lib"
	"go.uber.org/zap"
)

// unitHolder wrap the dm-worker unit.
type unitHolder struct {
	ctx    context.Context
	cancel context.CancelFunc

	autoResume *worker.AutoResumeInfo

	unit        unit.Unit
	resultCh    chan pb.ProcessResult
	lastResult  *pb.ProcessResult // TODO: check if framework can persist result
	processOnce sync.Once
}

func newUnitHolder(workerType lib.WorkerType, task string, u unit.Unit) *unitHolder {
	ctx, cancel := context.WithCancel(context.Background())
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
	return &unitHolder{
		ctx:        ctx,
		cancel:     cancel,
		autoResume: autoResume,
		unit:       u,
		resultCh:   make(chan pb.ProcessResult, 1),
	}
}

func (u *unitHolder) init(ctx context.Context) error {
	return u.unit.Init(ctx)
}

func (u *unitHolder) lazyProcess() {
	u.processOnce.Do(func() {
		go u.unit.Process(u.ctx, u.resultCh)
	})
}

func (u *unitHolder) getResult() (bool, *pb.ProcessResult) {
	if u.lastResult != nil {
		return true, u.lastResult
	}
	select {
	case r := <-u.resultCh:
		u.lastResult = &r
		return true, &r
	default:
		return false, nil
	}
}

func (u *unitHolder) checkAndAutoResume() {
	hasResult, result := u.getResult()
	if !hasResult || len(result.Errors) == 0 {
		return
	}

	log.L().Error("task runs with error", zap.Any("error msg", result.Errors))
	u.unit.Pause()

	subtaskStage := &pb.SubTaskStatus{
		Stage:  pb.Stage_Paused,
		Result: result,
	}
	strategy := u.autoResume.CheckResumeSubtask(subtaskStage, config.DefaultBackoffRollback)
	log.L().Info("got auto resume strategy",
		zap.Stringer("strategy", strategy))

	if strategy == worker.ResumeDispatch {
		log.L().Info("dispatch auto resume task")
		// TODO: manage goroutines
		go u.unit.Resume(u.ctx, u.resultCh)
	}
}

func (u *unitHolder) tick(ctx context.Context) error {
	u.checkAndAutoResume()
	return nil
}

func (u *unitHolder) close() {
	u.cancel()
	u.unit.Close()
}

func (u *unitHolder) Stage() metadata.TaskStage {
	hasResult, result := u.getResult()
	switch {
	case !hasResult:
		return metadata.StageRunning
	case len(result.Errors) == 0:
		return metadata.StageFinished
	default:
		return metadata.StagePaused
	}
}
