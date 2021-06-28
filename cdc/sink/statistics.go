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

package sink

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	printStatusInterval  = 10 * time.Minute
	flushMetricsInterval = 5 * time.Second
)

// NewStatistics creates a statistics
func NewStatistics(ctx context.Context, name string, opts map[string]string) *Statistics {
	statistics := &Statistics{name: name, lastPrintStatusTime: time.Now()}
	if cid, ok := opts[OptChangefeedID]; ok {
		statistics.changefeedID = cid
	}
	if cid, ok := opts[OptCaptureAddr]; ok {
		statistics.captureAddr = cid
	}
	statistics.metricExecTxnHis = execTxnHistogram.WithLabelValues(statistics.captureAddr, statistics.changefeedID)
	statistics.metricExecBatchHis = execBatchHistogram.WithLabelValues(statistics.captureAddr, statistics.changefeedID)
	statistics.metricExecErrCnt = executionErrorCounter.WithLabelValues(statistics.captureAddr, statistics.changefeedID)

	// Flush metrics in background for better accuracy and efficiency.
	ticker := time.NewTicker(flushMetricsInterval)
	metricTotalRows := totalRowsCountGauge.WithLabelValues(statistics.captureAddr, statistics.changefeedID)
	metricTotalFlushedRows := totalFlushedRowsCountGauge.WithLabelValues(statistics.captureAddr, statistics.changefeedID)
	go func() {
		defer ticker.Stop()
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
type Statistics struct {
	name             string
	captureAddr      string
	changefeedID     string
	totalRows        uint64
	totalFlushedRows uint64

	lastPrintStatusTotalRows uint64
	lastPrintStatusTime      time.Time

	metricExecTxnHis   prometheus.Observer
	metricExecBatchHis prometheus.Observer
	metricExecErrCnt   prometheus.Counter
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
	atomic.AddUint64(&b.totalFlushedRows, uint64(batchSize))
	return nil
}

// PrintStatus prints the status of the Sink
func (b *Statistics) PrintStatus(ctx context.Context) {
	since := time.Since(b.lastPrintStatusTime)
	if since < printStatusInterval {
		return
	}
	totalRows := atomic.LoadUint64(&b.totalRows)
	count := totalRows - b.lastPrintStatusTotalRows
	seconds := since.Seconds()
	var qps uint64
	if seconds > 0 {
		qps = count / uint64(seconds)
	}
	b.lastPrintStatusTime = time.Now()
	b.lastPrintStatusTotalRows = totalRows
	log.Info("sink replication status",
		zap.String("name", b.name),
		zap.String("changefeed", b.changefeedID),
		util.ZapFieldCapture(ctx),
		zap.Uint64("count", count),
		zap.Uint64("qps", qps))
}
