// Copyright 2019 PingCAP, Inc.
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

package puller

import "github.com/prometheus/client_golang/prometheus"

var (
	kvEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "kv_event_count",
			Help:      "The number of events received from kv client event channel",
		}, []string{"capture", "changefeed", "type"})
	txnCollectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "txn_collect_event_count",
			Help:      "The number of events received from txn collector",
		}, []string{"capture", "changefeed", "type"})
	resolvedTxnsBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "resolved_txns_batch_size",
			Help:      "The number of txns resolved in one go.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
		})
	entryBufferSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "entry_buffer_size",
			Help:      "Puller entry buffer size",
		}, []string{"capture", "changefeed"})
	eventChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "event_chan_size",
			Help:      "Puller event channel size",
		}, []string{"capture", "changefeed"})
	entrySorterResolvedChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "entry_sorter_resolved_chan_size",
			Help:      "Puller entry sorter resolved channel size",
		}, []string{"capture", "changefeed"})
	entrySorterOutputChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "entry_sorter_output_chan_size",
			Help:      "Puller entry sorter output channel size",
		}, []string{"capture", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(kvEventCounter)
	registry.MustRegister(txnCollectCounter)
	registry.MustRegister(resolvedTxnsBatchSize)
	registry.MustRegister(entryBufferSizeGauge)
	registry.MustRegister(eventChanSizeGauge)
	registry.MustRegister(entrySorterResolvedChanSizeGauge)
	registry.MustRegister(entrySorterOutputChanSizeGauge)
}
