package dm

import (
	"context"
	"sync"

	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/lib"
)

type unitHolder struct {
	ctx    context.Context
	cancel context.CancelFunc

	unit        unit.Unit
	processCh   chan pb.ProcessResult
	lastResult  *pb.ProcessResult // TODO: check if framework can persist result
	processOnce sync.Once
}

func newUnitHolder(u unit.Unit) *unitHolder {
	ctx, cancel := context.WithCancel(context.Background())
	return &unitHolder{
		ctx:       ctx,
		cancel:    cancel,
		unit:      u,
		processCh: make(chan pb.ProcessResult, 1),
	}
}

func (u *unitHolder) init(ctx context.Context) error {
	return u.unit.Init(ctx)
}

func (u *unitHolder) lazyProcess() {
	u.processOnce.Do(func() {
		go u.unit.Process(u.ctx, u.processCh)
	})
}

func (u *unitHolder) getResult() (bool, *pb.ProcessResult) {
	if u.lastResult != nil {
		return true, u.lastResult
	}
	select {
	case r := <-u.processCh:
		u.lastResult = &r
		return true, &r
	default:
		return false, nil
	}
}

func (u *unitHolder) tryUpdateStatus(ctx context.Context, base lib.BaseWorker) error {
	hasResult, result := u.getResult()
	if !hasResult {
		return nil
	}
	var s lib.WorkerStatus
	if len(result.Errors) > 0 {
		s = lib.WorkerStatus{
			Code:         lib.WorkerStatusError,
			ErrorMessage: unit.JoinProcessErrors(result.Errors),
		}
	} else {
		s = lib.WorkerStatus{
			Code: lib.WorkerStatusFinished,
		}
	}
	err := base.UpdateStatus(ctx, s)
	if err != nil {
		log.L().Error("update status failed", zap.Error(err))
		return nil
	}
	// after UpdateStatus, return any not-nil error to kill the worker
	return lib.StopAfterTick
}

func (u *unitHolder) close() {
	u.cancel()
	u.unit.Close()
}
