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

package sink

// Type is the type of sink.
type Type int

const (
	// RowSink is the type of row sink. It is used to sink row events.
	RowSink Type = iota + 1
	// TxnSink is the type of txn sink. It is used to sink txn events.
	TxnSink
)

const (
	// KafkaSchema indicates the schema is kafka.
	KafkaSchema = "kafka"
	// KafkaSSLSchema indicates the schema is kafka+ssl.
	KafkaSSLSchema = "kafka+ssl"
	// PulsarSchema indicates the schema is pulsar.
	PulsarSchema = "pulsar"
	// PulsarSSLSchema indicates the schema is pulsar+ssl.
	PulsarSSLSchema = "pulsar+ssl"
	// BlackHoleSchema indicates the schema is blackhole.
	BlackHoleSchema = "blackhole"
)

// IsMQScheme returns true if the scheme belong to mq schema.
func IsMQScheme(scheme string) bool {
	return scheme == KafkaSchema || scheme == KafkaSSLSchema ||
		IsPulsarScheme(scheme)
}

// IsPulsarScheme returns true if the scheme belong to pulsar schema.
func IsPulsarScheme(scheme string) bool {
	return scheme == PulsarSchema || scheme == PulsarSSLSchema
}
