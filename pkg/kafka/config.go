// Copyright 2021 PingCAP, Inc.
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

package kafka

const (
	// BrokerMessageMaxBytesConfigName specifies the largest record batch size allowed by
	// Kafka brokers.
	// See: https://kafka.apache.org/documentation/#brokerconfigs_message.max.bytes
	BrokerMessageMaxBytesConfigName = "message.max.bytes"
	// TopicMaxMessageBytesConfigName specifies the largest record batch size allowed by
	// Kafka topics.
	// See: https://kafka.apache.org/documentation/#topicconfigs_max.message.bytes
	TopicMaxMessageBytesConfigName = "max.message.bytes"
	// MinInsyncReplicasConfigName the minimum number of replicas that must acknowledge a write
	// for the write to be considered successful. Only works if the producer's acks is "all" (or "-1").
	// See: https://kafka.apache.org/documentation/#brokerconfigs_min.insync.replicas and
	// https://kafka.apache.org/documentation/#topicconfigs_min.insync.replicas
	MinInsyncReplicasConfigName = "min.insync.replicas"
)
