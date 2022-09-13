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

// String returns the string representation of the type.
func (t Type) String() string {
	switch t {
	case RowSink:
		return "RowSink"
	case TxnSink:
		return "TxnSink"
	}
	panic("unknown sink type")
}

const (
	// KafkaSchema indicates the schema is kafka.
	KafkaSchema = "kafka"
	// KafkaSSLSchema indicates the schema is kafka+ssl.
	KafkaSSLSchema = "kafka+ssl"
	// BlackHoleSchema indicates the schema is blackhole.
	BlackHoleSchema = "blackhole"
	// MySQLSchema indicates the schema is MySQL.
	MySQLSchema = "mysql"
	// MySQLSSLSchema indicates the schema is MySQL+ssl.
	MySQLSSLSchema = "mysql+ssl"
	// TiDBSchema indicates the schema is TiDB.
	TiDBSchema = "tidb"
	// TiDBSSLSchema indicates the schema is TiDB+ssl.
	TiDBSSLSchema = "tidb+ssl"
	// S3Schema indicates the schema is s3.
	S3Schema = "s3"
	// NFSSchema indicates the schema is nfs
	NFSSchema = "nfs"
	// LocalSchema indicates the schema is local
	LocalSchema = "local"
)

// IsMQScheme returns true if the scheme belong to mq schema.
func IsMQScheme(scheme string) bool {
	return scheme == KafkaSchema || scheme == KafkaSSLSchema
}

// IsMySQLCompatibleScheme returns true if the scheme is compatible with MySQL.
func IsMySQLCompatibleScheme(scheme string) bool {
	return scheme == MySQLSchema || scheme == MySQLSSLSchema ||
		scheme == TiDBSchema || scheme == TiDBSSLSchema
}
