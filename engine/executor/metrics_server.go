// Copyright 2022 PingCAP, Inc.
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

package executor

import (
	"github.com/prometheus/client_golang/prometheus"
)

var executorTaskNumGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "dataflow",
		Subsystem: "executor",
		Name:      "task_num",
		Help:      "number of task in this executor",
	}, []string{"status"})

// initServerMetrics registers statistics of executor server
func initServerMetrics(registry *prometheus.Registry) {
	registry.MustRegister(executorTaskNumGauge)
}
