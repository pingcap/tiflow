package common

import "github.com/prometheus/client_golang/prometheus"

var (
	CompressionRatio = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kafka_sink",
			Name:      "compression_ratio",
			Help:      "The compression ratio of kafka sink",
			Buckets:   prometheus.LinearBuckets(0, 100, 20),
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(CompressionRatio)
}
