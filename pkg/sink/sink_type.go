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
	// KafkaScheme indicates the scheme is kafka.
	KafkaScheme = "kafka"
	// KafkaSSLScheme indicates the scheme is kafka+ssl.
	KafkaSSLScheme = "kafka+ssl"
	// BlackHoleScheme indicates the scheme is blackhole.
	BlackHoleScheme = "blackhole"
	// MySQLScheme indicates the scheme is MySQL.
	MySQLScheme = "mysql"
	// MySQLSSLScheme indicates the scheme is MySQL+ssl.
	MySQLSSLScheme = "mysql+ssl"
	// TiDBScheme indicates the scheme is TiDB.
	TiDBScheme = "tidb"
	// TiDBSSLScheme indicates the scheme is TiDB+ssl.
	TiDBSSLScheme = "tidb+ssl"
	// S3Scheme indicates the scheme is s3.
	S3Scheme = "s3"
	// FileScheme indicates the scheme is local fs or NFS.
	FileScheme = "file"
	// GCSScheme indicates the scheme is gcs.
	GCSScheme = "gcs"
	// GSScheme is an alias for "gcs"
	GSScheme = "gs"
	// AzblobScheme indicates the scheme is azure blob storage.\
	AzblobScheme = "azblob"
	// AzureScheme is an alias for "azblob"
	AzureScheme = "azure"
	// CloudStorageNoopScheme indicates the scheme is noop.
	CloudStorageNoopScheme = "noop"
)

// IsMQScheme returns true if the scheme belong to mq scheme.
func IsMQScheme(scheme string) bool {
	return scheme == KafkaScheme || scheme == KafkaSSLScheme
}

// IsMySQLCompatibleScheme returns true if the scheme is compatible with MySQL.
func IsMySQLCompatibleScheme(scheme string) bool {
	return scheme == MySQLScheme || scheme == MySQLSSLScheme ||
		scheme == TiDBScheme || scheme == TiDBSSLScheme
}

// IsStorageScheme returns true if the scheme belong to storage scheme.
func IsStorageScheme(scheme string) bool {
	return scheme == FileScheme || scheme == S3Scheme || scheme == GCSScheme ||
		scheme == GSScheme || scheme == AzblobScheme || scheme == AzureScheme || scheme == CloudStorageNoopScheme
}
