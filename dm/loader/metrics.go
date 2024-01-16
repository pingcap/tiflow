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

package loader

import (
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/prometheus/client_golang/prometheus"
)

type metricProxies struct {
	loaderExitWithErrorCounter *prometheus.CounterVec
}

func newMetricProxies(factory promutil.Factory) *metricProxies {
	return &metricProxies{
		loaderExitWithErrorCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dm",
				Subsystem: "loader",
				Name:      "exit_with_error_count",
				Help:      "counter for loader exits with error",
			}, []string{"task", "source_id", "resumable_err"}),
	}
}

var defaultMetricProxies = newMetricProxies(&promutil.PromFactory{})

// RegisterMetrics registers metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(defaultMetricProxies.loaderExitWithErrorCounter)
}

func (l *LightningLoader) removeLabelValuesWithTaskInMetrics(task, source string) {
	labels := prometheus.Labels{"task": task, "source_id": source}
	l.metricProxies.loaderExitWithErrorCounter.DeletePartialMatch(labels)
}
