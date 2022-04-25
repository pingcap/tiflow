package dm

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/externalresource/broker"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/dm/worker"
	"github.com/pingcap/tiflow/dm/pkg/backoff"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// unitHolder wrap the dm-worker unit.
type unitHolder struct {
	ctx    context.Context
	cancel context.CancelFunc

	autoResume         *worker.AutoResumeInfo
	storageWriteHandle broker.Handle

	workerType  lib.WorkerType
	task        string
	unit        unit.Unit
	resultCh    chan pb.ProcessResult
	lastResult  *pb.ProcessResult // TODO: check if framework can persist result
	lastStage   worker.ResumeStrategy
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
		workerType: workerType,
		task:       task,
		autoResume: autoResume,
		unit:       u,
		lastStage:  -1, // -1 represents init stage, refactor later.
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

func (u *unitHolder) tryUpdateStatus(ctx context.Context, base lib.BaseWorker) error {
	// TODO: refactor in later pr
	status := runtime.DefaultTaskStatus{
		Unit:  u.workerType,
		Task:  u.task,
		Stage: metadata.StageRunning,
	}

	hasResult, result := u.getResult()
	if !hasResult {
		// update status when task first runs.
		if u.lastStage == 0 {
			return nil
		}
		statusBytes, err := json.Marshal(status)
		if err != nil {
			return err
		}
		s := libModel.WorkerStatus{
			Code:     libModel.WorkerStatusNormal,
			ExtBytes: statusBytes,
		}
		err = base.UpdateStatus(ctx, s)
		if err == nil {
			u.lastStage = 0 // 0 represents task is running
		}
		return nil
	}

	// if task is finished
	if len(result.Errors) == 0 {
		status.Stage = metadata.StageFinished
		statusBytes, err := json.Marshal(status)
		if err != nil {
			return err
		}
		if u.storageWriteHandle != nil {
			// try to persist storage, if failed, retry next tick
			err := u.storageWriteHandle.Persist(ctx)
			if err != nil {
				log.L().Error("persist storage failed", zap.Error(err))
				return nil
			}
		}

		s := libModel.WorkerStatus{
			Code:     libModel.WorkerStatusFinished,
			ExtBytes: statusBytes,
		}
		return base.Exit(ctx, s, nil)
	}

	log.L().Info("task runs with error", zap.Any("error msg", result.Errors))

	subtaskStage := &pb.SubTaskStatus{
		Stage:  pb.Stage_Paused,
		Result: result,
	}
	strategy := u.autoResume.CheckResumeSubtask(subtaskStage, config.DefaultBackoffRollback)
	log.L().Info("got auto resume strategy",
		zap.Stringer("strategy", strategy))

	switch strategy {
	case worker.ResumeSkip:
		// update status on first skip
		if u.lastStage == worker.ResumeSkip {
			return nil
		}
		u.unit.Pause()
		u.lastStage = worker.ResumeSkip
		// wait on next auto resume
		status.Stage = metadata.StagePaused
		statusBytes, err := json.Marshal(status)
		if err != nil {
			return err
		}
		s := libModel.WorkerStatus{
			Code:         libModel.WorkerStatusError,
			ErrorMessage: unit.JoinProcessErrors(result.Errors),
			ExtBytes:     statusBytes,
		}
		// TODO: UpdateStatus too frequently?
		// nolint:errcheck
		_ = base.UpdateStatus(ctx, s)
		return nil
	case worker.ResumeDispatch:
		u.unit.Pause()
		u.lastStage = worker.ResumeDispatch
		// can try auto resume
		u.lastResult = nil
		go u.unit.Resume(u.ctx, u.resultCh)
		return nil
	default:
		status.Stage = metadata.StagePaused
		statusBytes, err := json.Marshal(status)
		if err != nil {
			return err
		}
		s := libModel.WorkerStatus{
			Code:         libModel.WorkerStatusError,
			ErrorMessage: unit.JoinProcessErrors(result.Errors),
			ExtBytes:     statusBytes,
		}
		return base.Exit(ctx, s, nil)
	}
}

func (u *unitHolder) close() {
	u.cancel()
	u.unit.Close()
}
