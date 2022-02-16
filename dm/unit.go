package dm

import (
	"context"
	"sync"

	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
)

type unitHolder struct {
	ctx    context.Context
	cancel context.CancelFunc

	unit        unit.Unit
	processCh   chan pb.ProcessResult
	lastResult  *pb.ProcessResult
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

func (u *unitHolder) close() {
	u.cancel()
	u.unit.Close()
}
