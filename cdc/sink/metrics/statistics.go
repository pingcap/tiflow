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
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// OptChangefeedID is the key for changefeed id.
	OptChangefeedID = "_changefeed_id"
	// OptCaptureAddr is the key for capture address.
	OptCaptureAddr = "_capture_addr"

	printStatusInterval  = 10 * time.Minute
	flushMetricsInterval = 5 * time.Second
)

type sinkType int

const (
	// SinkTypeDB is the type of sink for database.
	SinkTypeDB sinkType = iota
	// SinkTypeMQ is the type of sink for message queue.
	SinkTypeMQ
)

func (t sinkType) String() string {
	switch t {
	case SinkTypeDB:
		return "DB"
	case SinkTypeMQ:
		return "MQ"
	}
	return "unknown"
}

// NewStatistics creates a statistics
func NewStatistics(ctx context.Context, t sinkType) *Statistics {
	statistics := &Statistics{
		sinkType:            t,
		changefeedID:        util.ChangefeedIDFromCtx(ctx),
		lastPrintStatusTime: time.Now(),
	}

	s := t.String()
	statistics.metricExecTxnHis = ExecTxnHistogram.WithLabelValues(statistics.changefeedID, s)
	statistics.metricExecBatchHis = ExecBatchHistogram.WithLabelValues(statistics.changefeedID, s)
	statistics.metricExecDDLHis = ExecDDLHistogram.WithLabelValues(statistics.changefeedID)
	statistics.metricExecErrCnt = ExecutionErrorCounter.WithLabelValues(statistics.changefeedID)

	// Flush metrics in background for better accuracy and efficiency.
	changefeedID := statistics.changefeedID
	ticker := time.NewTicker(flushMetricsInterval)
	go func() {
		defer ticker.Stop()
		metricTotalRows := TotalRowsCountGauge.WithLabelValues(changefeedID)
		metricTotalFlushedRows := TotalFlushedRowsCountGauge.WithLabelValues(changefeedID)
		defer func() {
			TotalRowsCountGauge.DeleteLabelValues(changefeedID)
			TotalFlushedRowsCountGauge.DeleteLabelValues(changefeedID)
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
type Statistics struct {
	sinkType         sinkType
	changefeedID     string
	totalRows        uint64
	totalFlushedRows uint64
	totalDDLCount    uint64

	lastPrintStatusTotalRows uint64
	lastPrintStatusTime      time.Time

	metricExecTxnHis   prometheus.Observer
	metricExecDDLHis   prometheus.Observer
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

	totalDDLCount := atomic.LoadUint64(&b.totalDDLCount)
	atomic.StoreUint64(&b.totalDDLCount, 0)

	log.Info("sink replication status",
		zap.Stringer("sinkType", b.sinkType),
		zap.String("changefeed", b.changefeedID),
		util.ZapFieldCapture(ctx),
		zap.Uint64("count", count),
		zap.Uint64("qps", qps),
		zap.Uint64("ddl", totalDDLCount))
}
