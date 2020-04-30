package sink

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// newBlackHoleSink creates a block hole sink
func NewBaseSink(name string, opts map[string]string) *BaseSink {
	sink := &BaseSink{name: name, lastPrintStatusTime: time.Now()}
	if cid, ok := opts[OptChangefeedID]; ok {
		sink.changefeedID = cid
	}
	if cid, ok := opts[OptCaptureID]; ok {
		sink.captureID = cid
	}
	sink.metricExecTxnHis = execTxnHistogram.WithLabelValues(sink.captureID, sink.changefeedID)
	sink.metricExecBatchHis = execBatchHistogram.WithLabelValues(sink.captureID, sink.changefeedID)
	return sink
}

type BaseSink struct {
	name         string
	captureID    string
	changefeedID string
	accumulated  uint64

	lastFlushAccumulated       uint64
	lastPrintStatusAccumulated uint64
	lastPrintStatusTime        time.Time

	metricExecTxnHis   prometheus.Observer
	metricExecBatchHis prometheus.Observer
}

func (b *BaseSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	atomic.AddUint64(&b.accumulated, uint64(len(rows)))
	return nil
}

func (b *BaseSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	accumulated := atomic.LoadUint64(&b.accumulated)
	b.metricExecBatchHis.Observe(float64(accumulated - b.lastFlushAccumulated))
	b.lastFlushAccumulated = accumulated
	b.printStatus()
	return nil
}

func (b *BaseSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	return nil
}

func (b *BaseSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return nil
}

func (b *BaseSink) printStatus() {
	since := time.Since(b.lastPrintStatusTime)
	if since < 10*time.Second {
		return
	}
	accumulated := atomic.LoadUint64(&b.accumulated)
	count := accumulated - b.lastPrintStatusAccumulated
	seconds := since.Seconds()
	var qps uint64
	if seconds > 0 {
		qps = count / uint64(seconds)
	}
	b.lastPrintStatusTime = time.Now()
	b.lastPrintStatusAccumulated = accumulated
	log.Info("sink replication status",
		zap.String("name", b.name),
		zap.String("changefeed", b.changefeedID),
		zap.String("captureID", b.captureID),
		zap.Uint64("count", count),
		zap.Uint64("qps", qps))
}

func (b *BaseSink) Close() error {
	return nil
}
