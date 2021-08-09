// Copyright 2021 PingCAP, Inc.
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

package orchestrator

import "github.com/prometheus/client_golang/prometheus"

var (
	etcdTxnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "etcdworker",
			Name:      "txn_count",
			Help:      "count of transactions executed by EtcdWorker",
		}, []string{"capture", "type", "succeeded"})
	etcdTxnDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "etcdworker",
			Name:      "big_txn_duration",
			Help:      "durations of big transactions",
			Buckets:   prometheus.ExponentialBuckets(0.001 /* 1ms */, 2, 18),
		}, []string{"capture", "succeeded"})
	etcdTxnSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "etcdworker",
			Name:      "txn_size",
			Help:      "size of etcd transactions",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{"capture", "type", "succeeded"})
)

func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(etcdTxnCounter)
	registry.MustRegister(etcdTxnDurationHistogram)
	registry.MustRegister(etcdTxnSizeHistogram)
}
