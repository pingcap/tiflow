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

package puller

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	kvEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "kv_event_count",
			Help:      "The number of events received from kv client event channel",
		}, []string{"namespace", "changefeed", "type"})
	txnCollectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "txn_collect_event_count",
			Help:      "The number of events received from txn collector",
		}, []string{"namespace", "changefeed", "type"})
	pullerResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "resolved_ts",
			Help:      "puller forward resolved ts",
		}, []string{"namespace", "changefeed"})
	outputChanSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "output_chan_size",
			Help:      "Puller entry buffer size",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 8),
		}, []string{"namespace", "changefeed"})
	memBufferSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "mem_buffer_size",
			Help:      "Puller in memory buffer size",
		}, []string{"namespace", "changefeed"})
	eventChanSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "event_chan_size",
			Help:      "Puller event channel size",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 8),
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(kvEventCounter)
	registry.MustRegister(txnCollectCounter)
	registry.MustRegister(pullerResolvedTsGauge)
	registry.MustRegister(memBufferSizeGauge)
	registry.MustRegister(outputChanSizeHistogram)
	registry.MustRegister(eventChanSizeHistogram)
}
