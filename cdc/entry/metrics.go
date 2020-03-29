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

package entry

import "github.com/prometheus/client_golang/prometheus"

var (
	tableMountedResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "entry",
			Name:      "table_mounted_resolved_ts",
			Help:      "real local resolved ts of processor",
		}, []string{"changefeed", "capture", "table"})
	ddlResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "entry",
			Name:      "table_ddl_resolved_ts",
			Help:      "real local resolved ts of processor",
		}, []string{"changefeed", "capture"})

	mounterOutputChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "mounter",
			Name:      "output_chan_size",
			Help:      "mounter output chan size",
		}, []string{"capture", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ddlResolvedTsGauge)
	registry.MustRegister(tableMountedResolvedTsGauge)
	registry.MustRegister(mounterOutputChanSizeGauge)
}
