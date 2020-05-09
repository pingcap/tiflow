package sink

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const printStatusInterval = 30 * time.Second

// NewStatistics creates a statistics
func NewStatistics(name string, opts map[string]string) *Statistics {
	statistics := &Statistics{name: name, lastPrintStatusTime: time.Now()}
	if cid, ok := opts[OptChangefeedID]; ok {
		statistics.changefeedID = cid
	}
	if cid, ok := opts[OptCaptureID]; ok {
		statistics.captureID = cid
	}
	statistics.metricExecTxnHis = execTxnHistogram.WithLabelValues(statistics.captureID, statistics.changefeedID)
	statistics.metricExecBatchHis = execBatchHistogram.WithLabelValues(statistics.captureID, statistics.changefeedID)
	statistics.metricExecErrCnt = executionErrorCounter.WithLabelValues(statistics.captureID, statistics.changefeedID)
	return statistics
}

// Statistics maintains some status and metrics of the Sink
type Statistics struct {
	name         string
	captureID    string
	changefeedID string
	accumulated  uint64

	lastPrintStatusAccumulated uint64
	lastPrintStatusTime        time.Time

	metricExecTxnHis   prometheus.Observer
	metricExecBatchHis prometheus.Observer
	metricExecErrCnt   prometheus.Counter
}

// RecordBatchExecution records the cost time of batch execution and batch size
func (b *Statistics) RecordBatchExecution(executer func() (int, error)) error {
	startTime := time.Now()
	batchSize, err := executer()
	if err != nil {
		b.metricExecErrCnt.Inc()
		return err
	}
	castTime := time.Since(startTime).Seconds()
	b.metricExecTxnHis.Observe(castTime)
	b.metricExecBatchHis.Observe(float64(batchSize))
	atomic.AddUint64(&b.accumulated, uint64(batchSize))
	return nil
}

// PrintStatus prints the status of the Sink
func (b *Statistics) PrintStatus() {
	since := time.Since(b.lastPrintStatusTime)
	if since < printStatusInterval {
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
