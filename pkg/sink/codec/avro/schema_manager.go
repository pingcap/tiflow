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

package avro

import (
	"context"

	"github.com/linkedin/goavro/v2"
)

// SchemaManager is an interface for schema registry
type SchemaManager interface {
	Register(ctx context.Context, schemaName string, schemaDefinition string) (schemaID, error)
	Lookup(ctx context.Context, schemaName string, schemaID schemaID) (*goavro.Codec, error)
	GetCachedOrRegister(ctx context.Context, topicName string,
		tableVersion uint64, schemaGen SchemaGenerator) (*goavro.Codec, []byte, error)
	RegistryType() string
	ClearRegistry(ctx context.Context, schemaName string) error
}

// SchemaGenerator represents a function that returns an Avro schema in JSON.
// Used for lazy evaluation
type SchemaGenerator func() (string, error)

type schemaID struct {
	// confluentSchemaID is the Confluent Schema ID, it represents
	// a unique schema in Confluent Schema Registry
	confluentSchemaID int
	// glueSchemaID is the AWS Glue SchemaVersionID, it represents
	// a unique schema in AWS Glue Schema Registry
	glueSchemaID string
}

type schemaCacheEntry struct {
	// tableVersion is the table's version which the message associated with.
	// encoder use it as the cache key.
	tableVersion uint64
	// schemaID is the unique identifier of a schema in schema registry.
	// for each message should carry this id to allow the decoder fetch the corresponding schema
	// decoder use it as the cache key.
	schemaID schemaID
	// codec is associated with the schemaID, used to decode the message
	codec  *goavro.Codec
	header []byte
}
