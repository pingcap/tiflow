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
