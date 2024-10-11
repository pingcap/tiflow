// Copyright 2022 PingCAP, Inc.
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

package codec

import (
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	encoderGroupInputChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "encoder_group_input_chan_size",
			Help:      "The size of input channel of encoder group",
		}, []string{"namespace", "changefeed"})
	// encoderGroupOutputChanSizeGauge tracks the size of output channel of encoder group
	encoderGroupOutputChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "encoder_group_output_chan_size",
			Help:      "The size of output channel of encoder group",
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(encoderGroupInputChanSizeGauge)
	registry.MustRegister(encoderGroupOutputChanSizeGauge)
	common.InitMetrics(registry)
}
