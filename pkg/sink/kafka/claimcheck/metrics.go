// Copyright 2023 PingCAP, Inc.
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

package claimcheck

import "github.com/prometheus/client_golang/prometheus"

var (
	// claimCheckSendMessageDuration records the duration of send message to the external claim-check storage.
	claimCheckSendMessageDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "mq_claim_check_send_message_duration",
			Help:      "Duration(s) for MQ worker send message to the external claim-check storage.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"namespace", "changefeed"})

	// claimCheckSendMessageCount records the total count of messages sent to the external claim-check storage.
	claimCheckSendMessageCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "mq_claim_check_send_message_count",
			Help:      "The total count of messages sent to the external claim-check storage.",
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all claim check related metrics
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(claimCheckSendMessageDuration)
	registry.MustRegister(claimCheckSendMessageCount)
}
