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

package dumpling

import (
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/prometheus/client_golang/prometheus"
)

type metricProxies struct {
	dumplingExitWithErrorCounter *prometheus.CounterVec
}

var f = &promutil.PromFactory{}

var defaultMetricProxies = &metricProxies{
	dumplingExitWithErrorCounter: f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "dumpling",
			Name:      "exit_with_error_count",
			Help:      "counter for dumpling exit with error",
		}, []string{"task", "source_id"}),
}

// RegisterMetrics registers metrics and saves the given registry for later use.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(defaultMetricProxies.dumplingExitWithErrorCounter)
}

func (m *Dumpling) removeLabelValuesWithTaskInMetrics(task, source string) {
	labels := prometheus.Labels{"task": task, "source_id": source}
	m.metricProxies.dumplingExitWithErrorCounter.DeletePartialMatch(labels)
}
