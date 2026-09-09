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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/unit"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

var _ prometheus.Registerer = taskMetricGatherer{}

func TestTaskWorkerCPUUsage(t *testing.T) {
	registry := prometheus.NewRegistry()
	cpuGauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "dm_worker_cpu_usage"})
	state := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "dm_worker_task_state"},
		[]string{"task", "source_id", "worker"})
	registry.MustRegister(cpuGauge, state)
	gatherer := taskMetricGatherer{Registerer: registry, gatherer: registry}
	// Normalize label/series ordering for textual comparisons. Custom labels are
	// appended by the gatherer, so their exposition order need not be lexical.
	comparisonGatherer := prometheus.Gatherers{gatherer}
	const task = "cpu-task-1"
	cpuGauge.Set(125)
	state.WithLabelValues(task, "source-1", "worker-1").Set(float64(pb.Stage_Running))
	state.WithLabelValues("cpu-task-2", "source-1", "worker-1").Set(float64(pb.Stage_Paused))

	// The metric is also available when no task has custom labels. Both tasks
	// share the whole worker CPU sample, regardless of their different stages.
	expected := `
# HELP dm_task_worker_cpu_usage CPU usage of the worker hosting the task, in percent (100 means one CPU core). Shared worker CPU is repeated for each task, not attributed to the task.
# TYPE dm_task_worker_cpu_usage gauge
dm_task_worker_cpu_usage{source_id="source-1",task="cpu-task-1",worker="worker-1"} 125
dm_task_worker_cpu_usage{source_id="source-1",task="cpu-task-2",worker="worker-1"} 125
`
	require.NoError(t, testutil.GatherAndCompare(comparisonGatherer, strings.NewReader(expected), "dm_task_worker_cpu_usage"))

	registerTaskMetricLabels(task, map[string]string{"keyspace_name": "ks1"})
	t.Cleanup(func() { unregisterTaskMetricLabels(task) })
	expected = strings.Replace(expected,
		`{source_id="source-1",task="cpu-task-1"`, `{keyspace_name="ks1",source_id="source-1",task="cpu-task-1"`, 1)
	require.NoError(t, testutil.GatherAndCompare(comparisonGatherer, strings.NewReader(expected), "dm_task_worker_cpu_usage"))
	families, err := gatherer.Gather()
	require.NoError(t, err)
	require.Equal(t, "ks1", metricLabelValue(t, families, "dm_worker_task_state", "task", task, "keyspace_name"))
	require.Empty(t, metricLabelValue(t, families, "dm_worker_cpu_usage", "", "", "keyspace_name"))
	var names []string
	for _, family := range families {
		names = append(names, family.GetName())
	}
	require.True(t, slices.IsSorted(names))

	// Exercise the same HTTP exposition path used by the worker /metrics endpoint.
	recorder := httptest.NewRecorder()
	promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}).ServeHTTP(
		recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(),
		`dm_task_worker_cpu_usage{source_id="source-1",task="cpu-task-1",worker="worker-1",keyspace_name="ks1"} 125`)
	require.Contains(t, recorder.Body.String(), "dm_worker_cpu_usage 125\n")

	// A later sample and task label updates are reflected without cached series.
	cpuGauge.Set(80)
	replaceTaskMetricLabels(task, map[string]string{"keyspace_name": "ks2"})
	expected = strings.ReplaceAll(expected, "125", "80")
	expected = strings.ReplaceAll(expected, `keyspace_name="ks1"`, `keyspace_name="ks2"`)
	require.NoError(t, testutil.GatherAndCompare(comparisonGatherer, strings.NewReader(expected), "dm_task_worker_cpu_usage"))

	unregisterTaskMetricLabels(task)
	expected = strings.ReplaceAll(expected, `keyspace_name="ks2",`, "")
	require.NoError(t, testutil.GatherAndCompare(comparisonGatherer, strings.NewReader(expected), "dm_task_worker_cpu_usage"))
	state.Reset()
	require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(""), "dm_task_worker_cpu_usage"))
}

func TestTaskWorkerCPUUsageLifecycle(t *testing.T) {
	registry := prometheus.NewRegistry()
	cpuGauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "dm_worker_cpu_usage"})
	registry.MustRegister(cpuGauge, taskState)
	gatherer := taskMetricGatherer{Registerer: registry, gatherer: registry}
	const task, source = "cpu-lifecycle-task", "cpu-lifecycle-source"
	cpuGauge.Set(50)
	registerTaskMetricLabels(task, map[string]string{"keyspace_name": "ks-lifecycle"})
	t.Cleanup(func() {
		updateTaskMetric(task, source, pb.Stage_Stopped, "worker-2")
		unregisterTaskMetricLabels(task)
	})

	for _, tc := range []struct {
		stage  pb.Stage
		worker string
		exists bool
	}{
		{pb.Stage_New, "worker-1", true},
		{pb.Stage_Running, "worker-1", true},
		{pb.Stage_Paused, "worker-1", true},
		{pb.Stage_Stopped, "worker-1", false},
		{pb.Stage_Running, "worker-2", true}, // Rescheduled/restored on another worker.
		{pb.Stage_Finished, "worker-2", false},
	} {
		t.Run(tc.stage.String()+"-"+tc.worker, func(t *testing.T) {
			updateTaskMetric(task, source, tc.stage, tc.worker)
			families, err := gatherer.Gather()
			require.NoError(t, err)
			var matches []*dto.Metric
			for _, family := range families {
				if family.GetName() == "dm_task_worker_cpu_usage" {
					for _, metric := range family.Metric {
						if labelValue(metric, "task") == task {
							matches = append(matches, metric)
						}
					}
				}
			}
			if !tc.exists {
				require.Empty(t, matches)
				return
			}
			require.Len(t, matches, 1)
			require.Equal(t, float64(50), matches[0].GetGauge().GetValue())
			require.Equal(t, map[string]string{
				"task": task, "source_id": source, "worker": tc.worker, "keyspace_name": "ks-lifecycle",
			}, metricLabelPairsMap(matches[0].Label))
		})
	}
}

func TestTaskWorkerCPUUsageMissingInputs(t *testing.T) {
	for _, withCPU := range []bool{false, true} {
		t.Run(strconv.FormatBool(withCPU), func(t *testing.T) {
			registry := prometheus.NewRegistry()
			if withCPU {
				registry.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{Name: "dm_worker_cpu_usage"}))
			} else {
				state := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "dm_worker_task_state"},
					[]string{"task", "source_id", "worker"})
				registry.MustRegister(state)
				state.WithLabelValues("cpu-task", "source", "worker").Set(2)
			}
			gatherer := taskMetricGatherer{Registerer: registry, gatherer: registry}
			require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(""), "dm_task_worker_cpu_usage"))
		})
	}
}

func TestTaskMetricGathererAddsLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	taskGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "task_metric"}, []string{"task", "type"})
	taskHistogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "task_histogram", Buckets: []float64{0.5, 1}}, []string{"task"})
	taskSummary := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{Name: "task_summary", Objectives: map[float64]float64{0.5: 0.05}}, []string{"task"})
	processGauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "process_metric"})
	registry.MustRegister(taskGauge, taskHistogram, taskSummary, processGauge)
	taskGauge.WithLabelValues("task-1", "sync").Set(1)
	taskGauge.WithLabelValues("task-2", "sync").Set(1)
	taskHistogram.WithLabelValues("task-1").Observe(0.5)
	taskSummary.WithLabelValues("task-1").Observe(0.5)
	processGauge.Set(1)

	registerTaskMetricLabels("task-1", map[string]string{"project_id": "123"})
	defer unregisterTaskMetricLabels("task-1")

	gatherer := taskMetricGatherer{Registerer: registry, gatherer: registry}
	families, err := gatherer.Gather()
	require.NoError(t, err)
	require.Equal(t, "123", metricLabelValue(t, families, "task_metric", "task", "task-1", "project_id"))
	require.Equal(t, "sync", metricLabelValue(t, families, "task_metric", "task", "task-1", "type"))
	require.Equal(t, "123", metricLabelValue(t, families, "task_histogram", "task", "task-1", "project_id"))
	require.Equal(t, "123", metricLabelValue(t, families, "task_summary", "task", "task-1", "project_id"))
	require.Empty(t, metricLabelValue(t, families, "task_metric", "task", "task-2", "project_id"))
	require.Empty(t, metricLabelValue(t, families, "process_metric", "", "", "project_id"))

	recorder := httptest.NewRecorder()
	promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}).ServeHTTP(
		recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), "task_histogram_bucket")
	require.Contains(t, recorder.Body.String(), `le="`)
	require.Contains(t, recorder.Body.String(), "task_summary{")
	require.Contains(t, recorder.Body.String(), `quantile="`)
}

func TestTaskMetricGathererRejectsUnknownCollision(t *testing.T) {
	registry := prometheus.NewRegistry()
	gatherer := taskMetricGatherer{Registerer: registry, gatherer: registry}
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "future_task_metric"}, []string{"task", "future_label"})
	gatherer.MustRegister(gauge)
	gauge.WithLabelValues("collision-task", "internal").Set(1)

	registerTaskMetricLabels("collision-task", map[string]string{"future_label": "external"})
	defer unregisterTaskMetricLabels("collision-task")

	_, err := gatherer.Gather()
	require.ErrorContains(t, err, `task metric label "future_label" conflicts with metric family "future_task_metric"`)
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
	registeredLabels := metricLabelPairsMap(metricLabels["task-refs"])
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

func TestSubTaskUpdateMetricLabels(t *testing.T) {
	task := "task-update-metric-labels"
	st := NewSubTaskWithStage(&config.SubTaskConfig{
		Name:         task,
		MetricLabels: map[string]string{"project_id": "old"},
	}, pb.Stage_Paused, nil, "worker")
	st.units = []unit.Unit{NewMockUnit(pb.UnitType_Sync)}
	t.Cleanup(st.unregisterMetricLabels)

	require.NoError(t, st.Update(context.Background(), &config.SubTaskConfig{
		Name:         task,
		MetricLabels: map[string]string{"project_id": "new"},
	}))
	metricLabelsMu.RLock()
	registeredLabels := metricLabelPairsMap(metricLabels[task])
	metricLabelsMu.RUnlock()
	require.Equal(t, map[string]string{"project_id": "new"}, registeredLabels)

	require.NoError(t, st.Update(context.Background(), &config.SubTaskConfig{Name: task}))
	metricLabelsMu.RLock()
	_, labelsExist := metricLabels[task]
	_, refsExist := metricRefs[task]
	metricLabelsMu.RUnlock()
	require.False(t, labelsExist)
	require.False(t, refsExist)
}

func BenchmarkTaskMetricGatherer(b *testing.B) {
	testCases := []struct {
		name        string
		taskSeries  int
		labelCount  int
		processOnly bool
	}{
		{name: "zero_labels_1k_series", taskSeries: 1000},
		{name: "process_only_1_label", labelCount: 1, processOnly: true},
		{name: "1k_series_1_label", taskSeries: 1000, labelCount: 1},
		{name: "10k_series_1_label", taskSeries: 10000, labelCount: 1},
		{name: "1k_series_8_labels", taskSeries: 1000, labelCount: 8},
		{name: "10k_series_8_labels", taskSeries: 10000, labelCount: 8},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			registry := prometheus.NewRegistry()
			processGauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "benchmark_process_metric"})
			registry.MustRegister(processGauge)
			processGauge.Set(1)

			task := "benchmark-" + tc.name
			if !tc.processOnly {
				taskGauge := prometheus.NewGaugeVec(
					prometheus.GaugeOpts{Name: "benchmark_task_metric"}, []string{"task", "id"})
				registry.MustRegister(taskGauge)
				for i := 0; i < tc.taskSeries; i++ {
					taskGauge.WithLabelValues(task, strconv.Itoa(i)).Set(float64(i))
				}
			}

			labels := make(map[string]string, tc.labelCount)
			for i := 0; i < tc.labelCount; i++ {
				labels[fmt.Sprintf("custom_%d", i)] = strconv.Itoa(i)
			}
			registerTaskMetricLabels(task, labels)
			b.Cleanup(func() { unregisterTaskMetricLabels(task) })

			gatherer := taskMetricGatherer{Registerer: registry, gatherer: registry}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := gatherer.Gather()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func metricLabelPairsMap(pairs []*dto.LabelPair) map[string]string {
	labels := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		labels[pair.GetName()] = pair.GetValue()
	}
	return labels
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
