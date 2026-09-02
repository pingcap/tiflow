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
	"maps"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

var _ prometheus.Registerer = taskMetricGatherer{}

func TestTaskMetricGathererAddsLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	taskGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "task_metric"}, []string{"task", "type"})
	processGauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "process_metric"})
	registry.MustRegister(taskGauge, processGauge)
	taskGauge.WithLabelValues("task-1", "sync").Set(1)
	taskGauge.WithLabelValues("task-2", "sync").Set(1)
	processGauge.Set(1)

	registerTaskMetricLabels("task-1", map[string]string{"project_id": "123", "type": "user-value"})
	defer unregisterTaskMetricLabels("task-1")

	families, err := (taskMetricGatherer{Registerer: registry, gatherer: registry}).Gather()
	require.NoError(t, err)
	require.Equal(t, "123", metricLabelValue(t, families, "task_metric", "task", "task-1", "project_id"))
	require.Equal(t, "sync", metricLabelValue(t, families, "task_metric", "task", "task-1", "type"))
	require.Empty(t, metricLabelValue(t, families, "task_metric", "task", "task-2", "project_id"))
	require.Empty(t, metricLabelValue(t, families, "process_metric", "", "", "project_id"))
}

func TestTaskMetricLabelReferences(t *testing.T) {
	labels := map[string]string{"project_id": "123"}
	registerTaskMetricLabels("task-refs", labels)
	registerTaskMetricLabels("task-refs", labels)
	t.Cleanup(func() {
		unregisterTaskMetricLabels("task-refs")
		unregisterTaskMetricLabels("task-refs")
	})

	labels["project_id"] = "changed"
	metricLabelsMu.RLock()
	registeredLabels := maps.Clone(metricLabels["task-refs"])
	refs := metricRefs["task-refs"]
	metricLabelsMu.RUnlock()
	require.Equal(t, map[string]string{"project_id": "123"}, registeredLabels)
	require.Equal(t, 2, refs)

	unregisterTaskMetricLabels("task-refs")
	metricLabelsMu.RLock()
	_, labelsExist := metricLabels["task-refs"]
	refs = metricRefs["task-refs"]
	metricLabelsMu.RUnlock()
	require.True(t, labelsExist)
	require.Equal(t, 1, refs)

	unregisterTaskMetricLabels("task-refs")
	metricLabelsMu.RLock()
	_, labelsExist = metricLabels["task-refs"]
	_, refsExist := metricRefs["task-refs"]
	metricLabelsMu.RUnlock()
	require.False(t, labelsExist)
	require.False(t, refsExist)
}

func metricLabelValue(
	t *testing.T,
	families []*dto.MetricFamily,
	familyName, selectorName, selectorValue, labelName string,
) string {
	t.Helper()
	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		for _, metric := range family.Metric {
			if selectorName != "" && labelValue(metric, selectorName) != selectorValue {
				continue
			}
			return labelValue(metric, labelName)
		}
	}
	return ""
}

func labelValue(metric *dto.Metric, name string) string {
	for _, label := range metric.Label {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}
