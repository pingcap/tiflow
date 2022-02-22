package executor

import "github.com/prometheus/client_golang/prometheus"

// registerMetrics registers metrics for executor server
func registerMetrics() {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())

	initServerMetrics(registry)

	prometheus.DefaultGatherer = registry
}
