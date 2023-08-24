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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// ---------- Glue schema manager
// schemaManager is used to register Avro Schemas to the Registry server,
// look up local cache according to the table's name, and fetch from the Registry
// in cache the local cache entry is missing.
type glueSchemaManager struct {
	registryName string
	client       glueClient

	cacheRWLock  sync.RWMutex
	cache        map[string]*schemaCacheEntry
	registryType string
}

// NewGlueSchemaManager creates a new schema manager for AWS Glue Schema Registry
// It will load the default AWS credentials if no credentials are provided.
// It will check if the registry exists, if not, it will return an error.
func NewGlueSchemaManager(
	ctx context.Context,
	cfg *config.GlueSchemaRegistryConfig,
) (SchemaManager, error) {
	var awsCfg aws.Config
	var err error
	if cfg.NoCredentials() {
		awsCfg, err = awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			log.Info("LoadDefaultConfig failed", zap.Error(err))
			return nil, errors.Trace(err)
		}
	} else {
		awsCfg = *aws.NewConfig()
		awsCfg.Region = cfg.Region
		awsCfg.Credentials = credentials.
			NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretAccessKey, cfg.Token)
	}
	client := glue.NewFromConfig(awsCfg)
	res := &glueSchemaManager{
		registryName: cfg.RegistryName,
		client:       client,
		cache:        make(map[string]*schemaCacheEntry),
		registryType: common.SchemaRegistryTypeGlue,
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	registry, err := res.client.GetRegistry(
		ctx,
		&glue.GetRegistryInput{
			RegistryId: &types.RegistryId{
				RegistryName: &cfg.RegistryName,
			},
		},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("Glue schema registry initialized", zap.Any("registry", registry))
	return res, nil
}

// Register a schema into schema registry, no cache
func (m *glueSchemaManager) Register(
	ctx context.Context,
	schemaName string,
	schemaDefinition string,
) (schemaID, error) {
	id := schemaID{}
	ok, _, err := m.getSchemaByName(ctx, schemaName)
	if err != nil {
		return id, errors.Trace(err)
	}
	if ok {
		log.Info("Schema already exists in registry, update it", zap.String("schemaName", schemaName))
		schemaID, err := m.updateSchema(ctx, schemaName, schemaDefinition)
		if err != nil {
			return id, errors.Trace(err)
		}
		log.Info("Schema updated", zap.String("schemaName", schemaName),
			zap.String("schemaID", schemaID))
		id.glueSchemaID = schemaID
		return id, nil
	}
	log.Info("Schema does not exist, create it", zap.String("schemaName", schemaName))
	schemaID, err := m.createSchema(ctx, schemaName, schemaDefinition)
	if err != nil {
		return id, errors.Trace(err)
	}
	id.glueSchemaID = schemaID
	return id, nil
}

func (m *glueSchemaManager) Lookup(
	ctx context.Context,
	schemaName string,
	schemaID schemaID,
) (*goavro.Codec, error) {
	m.cacheRWLock.RLock()
	entry, exists := m.cache[schemaName]
	if exists && entry.schemaID.confluentSchemaID == schemaID.confluentSchemaID {
		log.Debug("Avro schema lookup cache hit",
			zap.String("key", schemaName),
			zap.Int("schemaID", entry.schemaID.confluentSchemaID))
		m.cacheRWLock.RUnlock()
		return entry.codec, nil
	}
	m.cacheRWLock.RUnlock()

	log.Info("Avro schema lookup cache miss",
		zap.String("key", schemaName),
		zap.Int("schemaID", schemaID.confluentSchemaID))

	ok, schema, err := m.getSchemaByID(ctx, schemaID.glueSchemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		return nil, cerror.ErrAvroSchemaAPIError.
			GenWithStackByArgs("schema not found in registry, name: %s, id: %s", schemaName, schemaID.glueSchemaID)
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Error("could not make goavro codec", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	header, err := m.getMsgHeader(schemaID.glueSchemaID)
	if err != nil {
		log.Error("could not get message header", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	m.cacheRWLock.Lock()
	defer m.cacheRWLock.Unlock()
	m.cache[schemaName] = &schemaCacheEntry{
		schemaID: schemaID,
		codec:    codec,
		header:   header,
	}

	return codec, nil
}

// GetCachedOrRegister checks if the suitable Avro schema has been cached.
// If not, a new schema is generated, registered and cached.
// Re-registering an existing schema shall return the same id(and version), so even if the
// cache is out-of-sync with schema registry, we could reload it.
func (m *glueSchemaManager) GetCachedOrRegister(
	ctx context.Context,
	schemaName string,
	tableVersion uint64,
	schemaGen SchemaGenerator,
) (*goavro.Codec, []byte, error) {
	m.cacheRWLock.RLock()
	if entry, exists := m.cache[schemaName]; exists && entry.tableVersion == tableVersion {
		log.Debug("Avro schema GetCachedOrRegister cache hit",
			zap.String("schemaName", schemaName),
			zap.Uint64("tableVersion", tableVersion),
			zap.String("schemaID", entry.schemaID.glueSchemaID))
		m.cacheRWLock.RUnlock()
		return entry.codec, entry.header, nil
	}
	m.cacheRWLock.RUnlock()

	log.Info("Avro schema lookup cache miss",
		zap.String("schemaName", schemaName),
		zap.Uint64("tableVersion", tableVersion))

	schema, err := schemaGen()
	if err != nil {
		return nil, nil, err
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Error("GetCachedOrRegister: Could not make goavro codec", zap.Error(err))
		return nil, nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	log.Info(fmt.Sprintf("The code to be registered: %#v", schema))

	id, err := m.Register(ctx, schemaName, schema)
	if err != nil {
		log.Error("GetCachedOrRegister: Could not register schema", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}

	header, err := m.getMsgHeader(id.glueSchemaID)
	if err != nil {
		log.Error("GetCachedOrRegister: Could not get message header", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}

	cacheEntry := &schemaCacheEntry{
		tableVersion: tableVersion,
		schemaID:     id,
		codec:        codec,
		header:       header,
	}

	m.cacheRWLock.Lock()
	m.cache[schemaName] = cacheEntry
	m.cacheRWLock.Unlock()

	log.Info("Avro schema GetCachedOrRegister successful with cache miss",
		zap.String("schemaName", schemaName),
		zap.Uint64("tableVersion", tableVersion),
		zap.String("schemaID", id.glueSchemaID))

	return codec, header, nil
}

// ClearRegistry implements SchemaManager, it is not used.
func (m *glueSchemaManager) ClearRegistry(ctx context.Context, schemaSubject string) error {
	return nil
}

func (m *glueSchemaManager) RegistryType() string {
	return m.registryType
}

func (m *glueSchemaManager) createSchema(ctx context.Context, schemaName, schemaDefinition string) (string, error) {
	createSchemaInput := &glue.CreateSchemaInput{
		RegistryId: &types.RegistryId{
			RegistryName: &m.registryName,
		},
		SchemaName:       aws.String(schemaName),
		DataFormat:       types.DataFormatAvro,
		SchemaDefinition: aws.String(schemaDefinition),
		// cdc don't need to set compatibility check for schema registry
		// TiDB do the schema compatibility check for us, we need to accept all schema changes
		// otherwise, we some schema changes will be failed
		Compatibility: types.CompatibilityNone,
	}

	output, err := m.client.CreateSchema(ctx, createSchemaInput)
	if err != nil {
		return "", errors.Trace(err)
	}
	return *output.SchemaVersionId, nil
}

func (m *glueSchemaManager) updateSchema(ctx context.Context, schemaName, schemaDefinition string) (string, error) {
	input := &glue.RegisterSchemaVersionInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(m.registryName),
			SchemaName:   &schemaName,
		},
		SchemaDefinition: aws.String(schemaDefinition),
	}

	resp, err := m.client.RegisterSchemaVersion(ctx, input)
	if err != nil {
		return "", errors.Trace(err)
	}
	return *resp.SchemaVersionId, nil
}

func (m *glueSchemaManager) getSchemaByName(ctx context.Context, schemaNAme string) (bool, string, error) {
	input := &glue.GetSchemaVersionInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(m.registryName),
			SchemaName:   aws.String(schemaNAme),
		},
		SchemaVersionNumber: &types.SchemaVersionNumber{LatestVersion: true},
	}
	result, err := m.client.GetSchemaVersion(ctx, input)
	if err != nil {
		if strings.Contains(err.Error(), "EntityNotFoundException") {
			return false, "", nil
		}
		return false, "", errors.Trace(err)
	}
	return true, *result.SchemaDefinition, nil
}

func (m *glueSchemaManager) getSchemaByID(ctx context.Context, schemaID string) (bool, string, error) {
	input := &glue.GetSchemaVersionInput{
		SchemaVersionId: aws.String(schemaID),
	}
	result, err := m.client.GetSchemaVersion(ctx, input)
	if err != nil {
		if strings.Contains(err.Error(), "EntityNotFoundException") {
			return false, "", nil
		}
		return false, "", errors.Trace(err)
	}
	return true, *result.SchemaDefinition, nil
}

// This is the header of the glue message, ref:
// https://github.com/awslabs/aws-glue-schema-registry/blob/
// master/common/src/main/java/com/amazonaws/services/
// schemaregistry/utils/AWSSchemaRegistryConstants.java
const (
	headerVersionByte      = uint8(3) // 3 is fixed for the glue message
	compressionDefaultByte = uint8(0) // 0  no compression
)

func (m *glueSchemaManager) getMsgHeader(schemaID string) ([]byte, error) {
	header := []byte{}
	header = append(header, headerVersionByte)
	header = append(header, compressionDefaultByte)
	uuid, err := uuid.ParseBytes([]byte(schemaID))
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrEncodeFailed, err)
	}
	header = append(header, uuid[:]...)
	return header, nil
}

func getGlueSchemaIDFromHeader(header []byte) (string, error) {
	if len(header) < 18 {
		return "", cerror.ErrDecodeFailed.GenWithStackByArgs("header is too short")
	}
	uuid := uuid.UUID(header[2:18])
	return uuid.String(), nil
}
