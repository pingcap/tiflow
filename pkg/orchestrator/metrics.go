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
	etcdTxnSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "etcd_worker",
			Name:      "etcd_txn_size_bytes",
			Help:      "Bucketed histogram of a etcd txn size.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		})

	etcdTxnExecDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "etcd_worker",
			Name:      "etcd_txn_exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a etcd txn.",
			Buckets:   prometheus.ExponentialBuckets(0.002 /* 2 ms */, 2, 18),
		})

	etcdWorkerTickDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "etcd_worker",
			Name:      "tick_reactor_duration",
			Help:      "Bucketed histogram of etcdWorker tick reactor time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01 /* 10 ms */, 2, 18),
		})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(etcdTxnSize)
	registry.MustRegister(etcdTxnExecDuration)
	registry.MustRegister(etcdWorkerTickDuration)
}
