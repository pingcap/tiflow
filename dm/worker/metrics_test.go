// Copyright 2026 PingCAP, Inc.
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

package worker

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

var _ prometheus.Registerer = taskMetricGatherer{}

func TestTaskMetricGathererAddsLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "task_metric"}, []string{"task"})
	registry.MustRegister(gauge)
	gauge.WithLabelValues("task-1").Set(1)

	registerTaskMetricLabels("task-1", map[string]string{"project_id": "123"})
	defer unregisterTaskMetricLabels("task-1")

	families, err := (taskMetricGatherer{Registerer: registry, gatherer: registry}).Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, label := range families[0].Metric[0].GetLabel() {
		if label.GetName() == "project_id" && label.GetValue() == "123" {
			return
		}
	}
	t.Fatal("injected label not found")
}
