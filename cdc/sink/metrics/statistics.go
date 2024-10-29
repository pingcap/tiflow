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
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/prometheus/client_golang/prometheus"
)

// NewStatistics creates a statistics
func NewStatistics(
	changefeed model.ChangeFeedID,
	sinkType sink.Type,
) *Statistics {
	s := sinkType.String()
	statistics := &Statistics{
		sinkType:     s,
		captureAddr:  config.GetGlobalServerConfig().AdvertiseAddr,
		changefeedID: changefeed,
	}

	namespace := statistics.changefeedID.Namespace
	changefeedID := statistics.changefeedID.ID
	statistics.metricExecDDLCount = ExecDDLCounter.WithLabelValues(namespace, changefeedID, s)
	statistics.metricExecDDLHis = ExecDDLHistogram.WithLabelValues(namespace, changefeedID, s)
	statistics.metricExecBatchHis = ExecBatchHistogram.WithLabelValues(namespace, changefeedID, s)
	statistics.metricTotalWriteBytesCnt = TotalWriteBytesCounter.WithLabelValues(namespace, changefeedID, s)
	statistics.metricRowSizeHis = LargeRowSizeHistogram.WithLabelValues(namespace, changefeedID, s)
	statistics.metricExecErrCnt = ExecutionErrorCounter.WithLabelValues(namespace, changefeedID, s)
	return statistics
}

// Statistics maintains some status and metrics of the Sink
// Note: All methods of Statistics should be thread-safe.
type Statistics struct {
	sinkType     string
	captureAddr  string
	changefeedID model.ChangeFeedID

	// Counter for DDL Executed.
	metricExecDDLCount prometheus.Counter
	// Histogram for DDL Executing duration.
	metricExecDDLHis prometheus.Observer
	// Histogram for DML batch size.
	metricExecBatchHis prometheus.Observer
	// Counter for total bytes of DML.
	metricTotalWriteBytesCnt prometheus.Counter
	// Histogram for Row size.
	metricRowSizeHis prometheus.Observer
	// Counter for sink error.
	metricExecErrCnt prometheus.Counter
}

// ObserveRows stats all received `RowChangedEvent`s.
func (b *Statistics) ObserveRows(rows ...*model.RowChangedEvent) {
	for _, row := range rows {
		// only track row with data size larger than `rowSizeLowBound` to reduce
		// the overhead of calling `Observe` method.
		if row.ApproximateDataSize >= largeRowSizeLowBound {
			b.metricRowSizeHis.Observe(float64(row.ApproximateDataSize))
		}
	}
}

// RecordBatchExecution stats batch executors which return (batchRowCount, error).
func (b *Statistics) RecordBatchExecution(executor func() (int, int64, error)) error {
	batchSize, batchWriteBytes, err := executor()
	if err != nil {
		b.metricExecErrCnt.Inc()
		return err
	}
	b.metricExecBatchHis.Observe(float64(batchSize))
	b.metricTotalWriteBytesCnt.Add(float64(batchWriteBytes))
	return nil
}

// RecordDDLExecution record the time cost of execute ddl
func (b *Statistics) RecordDDLExecution(executor func() error) error {
	start := time.Now()
	if err := executor(); err != nil {
		b.metricExecErrCnt.Inc()
		return err
	}
	b.metricExecDDLHis.Observe(time.Since(start).Seconds())
	b.metricExecDDLCount.Inc()
	return nil
}

// Close release some internal resources.
func (b *Statistics) Close() {
	ExecDDLCounter.DeleteLabelValues(b.changefeedID.Namespace, b.changefeedID.ID, b.sinkType)
	ExecDDLHistogram.DeleteLabelValues(b.changefeedID.Namespace, b.changefeedID.ID, b.sinkType)
	ExecBatchHistogram.DeleteLabelValues(b.changefeedID.Namespace, b.changefeedID.ID, b.sinkType)
	TotalWriteBytesCounter.DeleteLabelValues(b.changefeedID.Namespace, b.changefeedID.ID, b.sinkType)
	LargeRowSizeHistogram.DeleteLabelValues(b.changefeedID.Namespace, b.changefeedID.ID, b.sinkType)
	ExecutionErrorCounter.DeleteLabelValues(b.changefeedID.Namespace, b.changefeedID.ID, b.sinkType)
}
