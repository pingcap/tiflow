package worker

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestTaskMetricGathererAddsLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "task_metric"}, []string{"task"})
	registry.MustRegister(gauge)
	gauge.WithLabelValues("task-1").Set(1)

	registerTaskMetricLabels("task-1", map[string]string{"project_id": "123"})
	defer unregisterTaskMetricLabels("task-1")

	families, err := (taskMetricGatherer{gatherer: registry}).Gather()
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
