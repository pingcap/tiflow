package v2

import "github.com/prometheus/client_golang/prometheus"

var (
	batchDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_duration",
			Help:      "Kafka client internal batch message time cost in milliseconds",
			Buckets:   prometheus.ExponentialBuckets(0.002
			, 2.0, 10),
		}, []string{"namespace", "changefeed"})

	batchMessageCountHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_message_count",
			Help:      "Kafka client internal batch message count",
			Buckets:   prometheus.ExponentialBuckets(8, 2.0, 11),
		}, []string{"namespace", "changefeed"})

	batchSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "Kafka client internal batch size in bytes",
			Buckets:   prometheus.ExponentialBuckets(1024, 2.0, 18),
		}, []string{"namespace", "changefeed"})

	requestRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_rate",
			Help:      "Kafka Client Requests/second sent to all brokers.",
		}, []string{"namespace", "changefeed"})
	requestLatencyInMsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_latency",
			Help:      "The request latency in ms",
		}, []string{"namespace", "changefeed"})

	retryCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_retry_count",
			Help:      "Kafka Client send request retry count",
		}, []string{"namespace", "changefeed"})

	errCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_err_count",
			Help:      "Kafka Client send request retry count",
		}, []string{"namespace", "changefeed"})
)

func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(batchDurationHistogram)
	registry.MustRegister(batchMessageCountHistogram)
	registry.MustRegister(batchSizeHistogram)
	registry.MustRegister(requestRateGauge)

	registry.MustRegister(requestLatencyInMsGauge)

	registry.MustRegister(retryCount)
	registry.MustRegister(errCount)
}
