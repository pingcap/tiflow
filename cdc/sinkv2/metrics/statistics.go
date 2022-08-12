// Copyright 2020 PingCAP, Inc.
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

package metrics

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	printStatusInterval  = 10 * time.Minute
	flushMetricsInterval = 5 * time.Second
)

// NewStatistics creates a statistics
func NewStatistics(ctx context.Context, captureAddr string, sinkType sink.Type) *Statistics {
	statistics := &Statistics{
		sinkType:     sinkType,
		captureAddr:  captureAddr,
		changefeedID: contextutil.ChangefeedIDFromCtx(ctx),
	}
	statistics.lastPrintStatusTime.Store(time.Now())

	namespcae := statistics.changefeedID.Namespace
	changefeedID := statistics.changefeedID.ID
	s := sinkType.String()
	statistics.metricExecTxnHis = ExecTxnHistogram.
		WithLabelValues(namespcae, changefeedID, s)
	statistics.metricExecBatchHis = ExecBatchHistogram.
		WithLabelValues(namespcae, changefeedID, s)
	statistics.metricRowSizesHis = LargeRowSizeHistogram.
		WithLabelValues(namespcae, changefeedID, s)
	statistics.metricExecDDLHis = ExecDDLHistogram.
		WithLabelValues(namespcae, changefeedID, s)
	statistics.metricExecErrCnt = ExecutionErrorCounter.
		WithLabelValues(namespcae, changefeedID)

	// Flush metrics in background for better accuracy and efficiency.
	ticker := time.NewTicker(flushMetricsInterval)
	go func() {
		defer ticker.Stop()
		metricTotalRows := TotalRowsCountGauge.
			WithLabelValues(namespcae, changefeedID)
		metricTotalFlushedRows := TotalFlushedRowsCountGauge.
			WithLabelValues(namespcae, changefeedID)
		defer func() {
			TotalRowsCountGauge.
				DeleteLabelValues(namespcae, changefeedID)
			TotalFlushedRowsCountGauge.
				DeleteLabelValues(namespcae, changefeedID)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				metricTotalRows.Set(float64(atomic.LoadUint64(&statistics.totalRows)))
				metricTotalFlushedRows.Set(float64(atomic.LoadUint64(&statistics.totalFlushedRows)))
			}
		}
	}()

	return statistics
}

// Statistics maintains some status and metrics of the Sink
// Note: All methods of Statistics should be thread-safe.
type Statistics struct {
	sinkType         sink.Type
	captureAddr      string
	changefeedID     model.ChangeFeedID
	totalRows        uint64
	totalFlushedRows uint64
	totalDDLCount    uint64

	lastPrintStatusTotalRows uint64
	lastPrintStatusTime      atomic.Value

	metricExecTxnHis   prometheus.Observer
	metricExecDDLHis   prometheus.Observer
	metricExecBatchHis prometheus.Observer
	metricExecErrCnt   prometheus.Counter

	metricRowSizesHis prometheus.Observer
}

// AddRowsCount records total number of rows needs to flush
func (b *Statistics) AddRowsCount(count int) {
	atomic.AddUint64(&b.totalRows, uint64(count))
}

// SubRowsCount records total number of rows needs to flush
func (b *Statistics) SubRowsCount(count int) {
	atomic.AddUint64(&b.totalRows, ^uint64(count-1))
}

// TotalRowsCount returns total number of rows
func (b *Statistics) TotalRowsCount() uint64 {
	return atomic.LoadUint64(&b.totalRows)
}

// ObserveRows record the size of all received `RowChangedEvent`
func (b *Statistics) ObserveRows(rows ...*model.RowChangedEvent) {
	for _, row := range rows {
		// only track row with data size larger than `rowSizeLowBound` to reduce
		// the overhead of calling `Observe` method.
		if row.ApproximateDataSize >= rowSizeLowBound {
			b.metricRowSizesHis.Observe(float64(row.ApproximateDataSize))
		}
	}
}

// AddDDLCount records total number of ddl needs to flush
func (b *Statistics) AddDDLCount() {
	atomic.AddUint64(&b.totalDDLCount, 1)
}

// RecordBatchExecution records the cost time of batch execution and batch size
func (b *Statistics) RecordBatchExecution(executor func() (int, error)) error {
	startTime := time.Now()
	batchSize, err := executor()
	if err != nil {
		b.metricExecErrCnt.Inc()
		return err
	}
	b.metricExecTxnHis.Observe(time.Since(startTime).Seconds())
	b.metricExecBatchHis.Observe(float64(batchSize))
	atomic.AddUint64(&b.totalFlushedRows, uint64(batchSize))
	return nil
}

// RecordDDLExecution record the time cost of execute ddl
func (b *Statistics) RecordDDLExecution(executor func() error) error {
	start := time.Now()
	if err := executor(); err != nil {
		return err
	}

	b.metricExecDDLHis.Observe(time.Since(start).Seconds())
	return nil
}

// PrintStatus prints the status of the Sink.
func (b *Statistics) PrintStatus() {
	since := time.Since(b.lastPrintStatusTime.Load().(time.Time))
	if since < printStatusInterval {
		return
	}

	totalRows := atomic.LoadUint64(&b.totalRows)
	count := totalRows - atomic.LoadUint64(&b.lastPrintStatusTotalRows)
	seconds := since.Seconds()
	var qps uint64
	if seconds > 0 {
		qps = count / uint64(seconds)
	}
	b.lastPrintStatusTime.Store(time.Now())
	atomic.StoreUint64(&b.lastPrintStatusTotalRows, totalRows)

	totalDDLCount := atomic.LoadUint64(&b.totalDDLCount)
	atomic.StoreUint64(&b.totalDDLCount, 0)

	log.Info("DML sink replication status",
		zap.Stringer("sinkType", b.sinkType),
		zap.String("namespace", b.changefeedID.Namespace),
		zap.String("changefeed", b.changefeedID.ID),
		zap.String("capture", b.captureAddr),
		zap.Uint64("count", count),
		zap.Uint64("qps", qps),
		zap.Uint64("ddl", totalDDLCount))
}
