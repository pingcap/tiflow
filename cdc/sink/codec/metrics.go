package codec

import "github.com/prometheus/client_golang/prometheus"

var (
	encoderGroupInputChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "encoder_group_input_chan_size",
			Help:      "The size of input channel of encoder group",
		}, []string{"namespace", "changefeed", "index"})

	EncoderGroupResponseChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "encoder_group_response_chan_size",
			Help:      "The size of response channel of encoder group",
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(encoderGroupInputChanSizeGauge)
	registry.MustRegister(EncoderGroupResponseChanSizeGauge)
}
