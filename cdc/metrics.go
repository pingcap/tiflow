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

package cdc

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	defaultMetricInterval = time.Second * 15
)

var (
	resolvedTsGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "resolved_ts",
			Help:      "local resolved ts of processor",
		}, []string{"changefeed", "capture"})
	resolvedTsLagGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "resolved_ts_lag",
			Help:      "local resolved ts lag of processor",
		}, []string{"changefeed", "capture"})
	tableResolvedTsGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "table_resolved_ts",
			Help:      "local resolved ts of processor",
		}, []string{"changefeed", "capture", "table"})
	checkpointTsGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "checkpoint_ts",
			Help:      "global checkpoint ts of processor",
		}, []string{"changefeed", "capture"})
	checkpointTsLagGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "checkpoint_ts_lag",
			Help:      "global checkpoint ts lag of processor",
		}, []string{"changefeed", "capture"})
	syncTableNumGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "num_of_tables",
			Help:      "number of synchronized table of processor",
		}, []string{"changefeed", "capture"})
	txnCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "txn_count",
			Help:      "txn count received/executed by this processor",
		}, []string{"type", "changefeed", "capture"})
	updateInfoDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "update_info_duration_seconds",
			Help:      "The time it took to update sub changefeed info.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"capture"})
	waitEventPrepareDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "wait_event_prepare",
			Help:      "Bucketed histogram of processing time (s) of waiting event prepare in processor.",
			Buckets:   prometheus.ExponentialBuckets(0.000001, 10, 10),
		}, []string{"changefeed", "capture"})
	tableOutputChanSizeGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "txn_output_chan_size",
			Help:      "size of row changed event output channel from table to processor",
		}, []string{"changefeed", "capture"})
	processorErrorCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "exit_with_error_count",
			Help:      "counter for processor exits with error",
		}, []string{"changefeed", "capture"})
	sinkFlushRowChangedDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "flush_event_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of flushing events in processor",
			Buckets:   prometheus.ExponentialBuckets(0.002 /* 2ms */, 2, 20),
		}, []string{"changefeed", "capture"})
)
