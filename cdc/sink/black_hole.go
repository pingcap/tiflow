package sink

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// newBlackHoleSink creates a block hole sink
func newBlackHoleSink(opts map[string]string) *blackHoleSink {
	return &blackHoleSink{
		BaseSink: NewBaseSink("blackhole", opts),
	}
}

type blackHoleSink struct {
	*BaseSink
}

func (b *blackHoleSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	log.Info("BlockHoleSink: FlushRowChangedEvents", zap.Uint64("resolvedTs", resolvedTs))
	startTime := time.Now()
	// TODO: add some random replication latency
	b.metricExecTxnHis.Observe(time.Since(startTime).Seconds())
	return b.BaseSink.FlushRowChangedEvents(ctx, resolvedTs)
}

func (b *blackHoleSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	log.Info("BlockHoleSink: Checkpoint Event", zap.Uint64("ts", ts))
	return b.BaseSink.EmitCheckpointTs(ctx, ts)
}

func (b *blackHoleSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	log.Info("BlockHoleSink: DDL Event", zap.Any("ddl", ddl))
	return b.BaseSink.EmitDDLEvent(ctx, ddl)
}
