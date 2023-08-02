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
	"testing"

	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func newClueSchemaManagerForTest(t *testing.T, ctx context.Context, cfg *config.GlueSchemaRegistryConfig) *glueSchemaManager {
	m, err := NewGlueSchemaManager(ctx, cfg)
	require.NoError(t, err)
	m.(*glueSchemaManager).client = newMockGlueClientImpl()
	return m.(*glueSchemaManager)
}

func TestGlueSchemaManager_Register(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GlueSchemaRegistryConfig{
		Region: "us-west-2",
	}
	m := newClueSchemaManagerForTest(t, ctx, cfg)

	schemaName := "test_schema"
	schemaDefinition := `{"type": "record", "name": "test_schema", "fields": [{"name": "field1", "type": "string"}]}`
	schemaID, err := m.Register(ctx, schemaName, schemaDefinition)
	require.NoError(t, err)
	require.NotEmpty(t, schemaID.gID)

	// Register the same schema again
	schemaID2, err := m.Register(ctx, schemaName, schemaDefinition)
	require.NoError(t, err)
	require.Equal(t, schemaID.gID, schemaID2.gID)

	// Register a different schema
	schemaDefinition2 := `{"type": "record", "name": "test_schema2", "fields": [{"name": "field1", "type": "string"}]}`
	schemaID3, err := m.Register(ctx, schemaName, schemaDefinition2)
	require.NoError(t, err)
	require.NotEqual(t, schemaID.gID, schemaID3.gID)
}

func TestGlueSchemaManager_Lookup(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GlueSchemaRegistryConfig{
		Region: "us-west-2",
	}
	m := newClueSchemaManagerForTest(t, ctx, cfg)

	schemaName := "test_schema"
	schemaDefinition := `{"type": "record", "name": "test_schema", "fields": [{"name": "field1", "type": "string"}]}`
	schemaID, err := m.Register(ctx, schemaName, schemaDefinition)
	require.NoError(t, err)

	codec, err := m.Lookup(ctx, schemaName, schemaID)
	require.NoError(t, err)
	require.NotNil(t, codec)
	require.Equal(t, schemaDefinition, codec.Schema())
}

func TestGlueSchemaManager_GetCachedOrRegister(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GlueSchemaRegistryConfig{
		Region: "us-west-2",
	}
	m := newClueSchemaManagerForTest(t, ctx, cfg)

	schemaName := "test_schema"
	schemaDefinition := `{"type": "record", "name": "test_schema", "fields": [{"name": "field1", "type": "string"}]}`
	codec, _, err := m.GetCachedOrRegister(ctx, schemaName, 1, func() (string, error) {
		return schemaDefinition, nil
	})
	require.NoError(t, err)
	require.NotNil(t, codec)
	require.Equal(t, schemaDefinition, codec.Schema())

	// Get the same schema again
	codec2, _, err := m.GetCachedOrRegister(ctx, schemaName, 1, func() (string, error) {
		return schemaDefinition, nil
	})
	require.NoError(t, err)
	require.Equal(t, codec, codec2)
}

func TestGlueSchemaManager_RegistryType(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GlueSchemaRegistryConfig{
		Region: "us-west-2",
	}
	m := newClueSchemaManagerForTest(t, ctx, cfg)

	registryType := m.RegistryType()
	require.Equal(t, common.SchemaRegistryTypeGlue, registryType)
}

func TestGlueSchemaManager_getMsgHeader(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GlueSchemaRegistryConfig{
		Region: "us-west-2",
	}
	m := newClueSchemaManagerForTest(t, ctx, cfg)

	schemaName := "test_schema"
	schemaDefinition := `{"type": "record", "name": "test_schema", "fields": [{"name": "field1", "type": "string"}]}`
	schemaID, err := m.Register(ctx, schemaName, schemaDefinition)
	require.NoError(t, err)

	header, err := m.getMsgHeader(schemaID.gID)
	require.NoError(t, err)
	require.Equal(t, header[0], header_version_byte)
	require.Equal(t, header[1], compression_default_byte)
	sid, err := getGlueSchemaIDFromHeader(header)
	require.NoError(t, err)
	require.Equal(t, schemaID.gID, sid)
}
