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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/metricsproxy"
)

// should alert.
var dumplingExitWithErrorCounter = metricsproxy.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "dumpling",
		Name:      "exit_with_error_count",
		Help:      "counter for dumpling exit with error",
	}, []string{"task", "source_id"})

// RegisterMetrics registers metrics and saves the given registry for later use.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(dumplingExitWithErrorCounter)
	export.InitMetricsVector(prometheus.Labels{"task": "", "source_id": ""})
	export.RegisterMetrics(registry)
}

func (m *Dumpling) removeLabelValuesWithTaskInMetrics(task, source string) {
	labels := prometheus.Labels{"task": task, "source_id": source}
	dumplingExitWithErrorCounter.DeleteAllAboutLabels(labels)
	failpoint.Inject("SkipRemovingDumplingMetrics", func(_ failpoint.Value) {
		m.logger.Info("", zap.String("failpoint", "SkipRemovingDumplingMetrics"))
		failpoint.Return()
	})
	export.RemoveLabelValuesWithTaskInMetrics(labels)
}
