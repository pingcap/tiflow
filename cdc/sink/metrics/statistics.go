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
	"time"

	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/prometheus/client_golang/prometheus"
)

// NewStatistics creates a statistics
func NewStatistics(ctx context.Context, sinkType sink.Type) *Statistics {
	statistics := &Statistics{
		sinkType:     sinkType,
		captureAddr:  contextutil.CaptureAddrFromCtx(ctx),
		changefeedID: contextutil.ChangefeedIDFromCtx(ctx),
	}

	namespcae := statistics.changefeedID.Namespace
	changefeedID := statistics.changefeedID.ID
	s := sinkType.String()
	statistics.metricExecDDLHis = ExecDDLHistogram.WithLabelValues(namespcae, changefeedID, s)
	statistics.metricExecBatchHis = ExecBatchHistogram.WithLabelValues(namespcae, changefeedID, s)
	statistics.metricRowSizeHis = LargeRowSizeHistogram.WithLabelValues(namespcae, changefeedID, s)
	statistics.metricExecErrCnt = ExecutionErrorCounter.WithLabelValues(namespcae, changefeedID, s)
	return statistics
}

// Statistics maintains some status and metrics of the Sink
// Note: All methods of Statistics should be thread-safe.
type Statistics struct {
	sinkType     sink.Type
	captureAddr  string
	changefeedID model.ChangeFeedID

	// Histogram for DDL Executing duration.
	metricExecDDLHis prometheus.Observer
	// Histogram for DML batch size.
	metricExecBatchHis prometheus.Observer
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
func (b *Statistics) RecordBatchExecution(executor func() (int, error)) error {
	batchSize, err := executor()
	if err != nil {
		b.metricExecErrCnt.Inc()
		return err
	}
	b.metricExecBatchHis.Observe(float64(batchSize))
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
	return nil
}

// Close release some internal resources.
func (b *Statistics) Close() {
	ExecDDLHistogram.DeleteLabelValues(b.changefeedID.Namespace, b.changefeedID.ID)
	ExecBatchHistogram.DeleteLabelValues(b.changefeedID.Namespace, b.changefeedID.ID)
	LargeRowSizeHistogram.DeleteLabelValues(b.changefeedID.Namespace, b.changefeedID.ID)
	ExecutionErrorCounter.DeleteLabelValues(b.changefeedID.Namespace, b.changefeedID.ID)
}
