// Copyright 2023 PingCAP, Inc.
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

package observer

import "github.com/prometheus/client_golang/prometheus"

var (
	tidbConnIdleDurationGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "observer",
			Name:      "tidb_conn_idle_duration",
			Help:      "Diagnose data of idle time of tidb connections",
		}, []string{"instance", "in_txn", "quantile"})
	tidbConnCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "observer",
			Name:      "tidb_connection_count",
			Help:      "Diagnose data of tidb connection count",
		}, []string{"instance"})
	tidbQueryDurationGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "observer",
			Name:      "tidb_query_duration",
			Help:      "Diagnose data of tidb query duration(p90)",
		}, []string{"instance", "sql_type"})
	tidbTxnDurationGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "observer",
			Name:      "tidb_txn_duration",
			Help:      "Diagnose data of tidb transaction duration(p95)",
		}, []string{"instance", "type"})
)

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(tidbConnIdleDurationGauge)
	registry.MustRegister(tidbConnCountGauge)
	registry.MustRegister(tidbQueryDurationGauge)
	registry.MustRegister(tidbTxnDurationGauge)
}
