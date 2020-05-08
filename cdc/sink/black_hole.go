package sink

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// newBlackHoleSink creates a block hole sink
func newBlackHoleSink(opts map[string]string) *blackHoleSink {
	return &blackHoleSink{
		statistics: NewStatistics("blackhole", opts),
	}
}

type blackHoleSink struct {
	statistics *Statistics

	accumulated     uint64
	lastAccumulated uint64
}

func (b *blackHoleSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	atomic.AddUint64(&b.accumulated, uint64(len(rows)))
	return nil
}

func (b *blackHoleSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	log.Info("BlockHoleSink: FlushRowChangedEvents", zap.Uint64("resolvedTs", resolvedTs))
	err := b.statistics.RecordBatchExecution(func() (int, error) {
		// TODO: add some random replication latency
		accumulated := atomic.LoadUint64(&b.accumulated)
		batchSize := accumulated - b.lastAccumulated
		b.lastAccumulated = accumulated
		return int(batchSize), nil
	})
	b.statistics.PrintStatus()
	return err
}

func (b *blackHoleSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	log.Info("BlockHoleSink: Checkpoint Event", zap.Uint64("ts", ts))
	return nil
}

func (b *blackHoleSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	log.Info("BlockHoleSink: DDL Event", zap.Any("ddl", ddl))
	return nil
}

func (b *blackHoleSink) Close() error {
	return nil
}
