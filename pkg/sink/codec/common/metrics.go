package common

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	compressionRatio = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "compression",
			Name:      "ratio",
			Help:      "The compression ratio",
			Buckets:   prometheus.LinearBuckets(0, 100, 20),
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(compressionRatio)
}

// CleanMetrics remove metrics belong to the given changefeed.
func CleanMetrics(changefeedID model.ChangeFeedID) {
	compressionRatio.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID)
}
