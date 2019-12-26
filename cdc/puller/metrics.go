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
	eventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "event_count",
			Help:      "The number of events received.",
		}, []string{"captureID", "type"})
	resolvedTxnsBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "resolved_txns_batch_size",
			Help:      "The number of txns resolved in one go.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
		})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(eventCounter)
	registry.MustRegister(resolvedTxnsBatchSize)
}
