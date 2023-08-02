package avro

import (
	"context"

	"github.com/linkedin/goavro/v2"
)

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
	// cID is the Confluent Schema ID, it represents
	// a unique schema in Confluent Schema Registry
	cID int
	// gID is the AWS Glue SchemaVersionID, it represents
	// a unique schema in AWS Glue Schema Registry
	gID string
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
