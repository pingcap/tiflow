package sink

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// newBlackHoleSink creates a block hole sink
func newBlackHoleSink(opts map[string]string) *blackHoleSink {
	sink := &blackHoleSink{resolveCh: make(chan struct{})}
	if cid, ok := opts[OptChangefeedID]; ok {
		sink.changefeedID = cid
	}
	if cid, ok := opts[OptCaptureID]; ok {
		sink.captureID = cid
	}
	return sink

}

type blackHoleSink struct {
	checkpointTs     uint64
	resolveCh        chan struct{}
	resolvedTs       uint64
	globalResolvedTs uint64
	captureID        string
	changefeedID     string
	accumulated      uint64
}

func (b *blackHoleSink) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.resolveCh:
			globalResolvedTs := atomic.LoadUint64(&b.globalResolvedTs)
			resolvedTs := atomic.LoadUint64(&b.resolvedTs)
			for globalResolvedTs > resolvedTs {
				time.Sleep(5 * time.Millisecond)
				resolvedTs = atomic.LoadUint64(&b.resolvedTs)
			}
			execBatchHistogram.WithLabelValues(b.captureID, b.changefeedID).Observe(float64(atomic.LoadUint64(&b.accumulated)))
			atomic.StoreUint64(&b.accumulated, 0)
			atomic.StoreUint64(&b.checkpointTs, globalResolvedTs)
		}
	}
}

func (b *blackHoleSink) EmitResolvedEvent(ctx context.Context, ts uint64) error {
	atomic.StoreUint64(&b.globalResolvedTs, ts)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.resolveCh <- struct{}{}:
		log.Info("BlockHoleSink: Resolved Event", zap.Uint64("resolved ts", ts))
	}
	return nil
}

func (b *blackHoleSink) EmitCheckpointEvent(ctx context.Context, ts uint64) error {
	log.Info("BlockHoleSink: Checkpoint Event", zap.Uint64("checkpoint ts", ts))
	return nil
}

func (b *blackHoleSink) EmitRowChangedEvent(ctx context.Context, rows ...*model.RowChangedEvent) error {
	var count uint64 = 0
	for _, row := range rows {
		if row.Resolved {
			atomic.StoreUint64(&b.resolvedTs, row.Ts)
		} else {
			count += 1
		}
	}
	atomic.AddUint64(&b.accumulated, count)
	return nil
}

func (b *blackHoleSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	log.Info("BlockHoleSink: DDL Event", zap.Any("ddl", ddl))
	return nil
}

func (b *blackHoleSink) CheckpointTs() uint64 {
	return atomic.LoadUint64(&b.checkpointTs)
}

func (b *blackHoleSink) PrintStatus(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (b *blackHoleSink) Close() error {
	return nil
}
