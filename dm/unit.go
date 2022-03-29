package dm

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/dm/worker"
	"github.com/pingcap/tiflow/dm/pkg/backoff"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type unitHolder struct {
	ctx    context.Context
	cancel context.CancelFunc

	autoResume *worker.AutoResumeInfo

	unit        unit.Unit
	resultCh    chan pb.ProcessResult
	lastResult  *pb.ProcessResult // TODO: check if framework can persist result
	processOnce sync.Once
}

func newUnitHolder(u unit.Unit) *unitHolder {
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

func (u *unitHolder) tryUpdateStatus(ctx context.Context, base lib.BaseWorker) error {
	hasResult, result := u.getResult()
	if !hasResult {
		// also need to clean old result
		s := libModel.WorkerStatus{
			Code: libModel.WorkerStatusNormal,
		}
		// nolint:errcheck
		_ = base.UpdateStatus(ctx, s)
		return nil
	}

	// if task is finished
	if len(result.Errors) == 0 {
		s := libModel.WorkerStatus{
			Code: libModel.WorkerStatusFinished,
		}
		return base.Exit(ctx, s, nil)
	}

	u.unit.Pause()
	subtaskStage := &pb.SubTaskStatus{
		Stage:  pb.Stage_Paused,
		Result: result,
	}
	strategy := u.autoResume.CheckResumeSubtask(subtaskStage, config.DefaultBackoffRollback)
	log.L().Info("got auto resume strategy",
		zap.Stringer("strategy", strategy))

	switch strategy {
	case worker.ResumeSkip:
		// wait on next auto resume
		s := libModel.WorkerStatus{
			Code:         libModel.WorkerStatusError,
			ErrorMessage: unit.JoinProcessErrors(result.Errors),
		}
		// TODO: UpdateStatus too frequently?
		// nolint:errcheck
		_ = base.UpdateStatus(ctx, s)
		return nil
	case worker.ResumeDispatch:
		// can try auto resume
		u.lastResult = nil
		go u.unit.Resume(u.ctx, u.resultCh)
		return nil
	default:
		s := libModel.WorkerStatus{
			Code:         libModel.WorkerStatusError,
			ErrorMessage: unit.JoinProcessErrors(result.Errors),
		}
		return base.Exit(ctx, s, nil)
	}
}

func (u *unitHolder) close() {
	u.cancel()
	u.unit.Close()
}
