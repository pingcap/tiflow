package servermaster

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	serverExecutorNumGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dataflow",
			Subsystem: "server_master",
			Name:      "executor_num",
			Help:      "number of executor servers in this cluster",
		}, []string{"status"})
	serverJobNumGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dataflow",
			Subsystem: "server_master",
			Name:      "job_num",
			Help:      "number of jobs in this cluster",
		}, []string{"status"})
)

// initServerMetrics registers statistics of server
func initServerMetrics(registry *prometheus.Registry) {
	registry.MustRegister(serverExecutorNumGauge)
	registry.MustRegister(serverJobNumGauge)
}
