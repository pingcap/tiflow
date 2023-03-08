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

package tablesink

import (
	"github.com/prometheus/client_golang/prometheus"
)

// TotalRowsCountCounter is the total count of rows that are processed by sink.
var TotalRowsCountCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "table_sink_total_rows_count",
		Help:      "The total count of rows that are processed by table sink",
	}, []string{"namespace", "changefeed"})

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(TotalRowsCountCounter)
}
