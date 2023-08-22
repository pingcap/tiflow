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

	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func newClueSchemaManagerForTest() *glueSchemaManager {
	res := &glueSchemaManager{
		registryName: "test_registry",
		client:       newMockGlueClientImpl(),
		cache:        make(map[string]*schemaCacheEntry),
		registryType: common.SchemaRegistryTypeGlue,
	}
	return res
}

func TestGlueSchemaManager_Register(t *testing.T) {
	ctx := context.Background()
	m := newClueSchemaManagerForTest()

	schemaName := "test_schema"
	schemaDefinition := `{"type": "record", "name": "test_schema", "fields": [{"name": "field1", "type": "string"}]}`
	schemaID, err := m.Register(ctx, schemaName, schemaDefinition)
	require.NoError(t, err)
	require.NotEmpty(t, schemaID.glueSchemaID)

	// Register the same schema again
	schemaID2, err := m.Register(ctx, schemaName, schemaDefinition)
	require.NoError(t, err)
	require.Equal(t, schemaID.glueSchemaID, schemaID2.glueSchemaID)

	// Register a different schema
	schemaDefinition2 := `{"type": "record", "name": "test_schema2", "fields": [{"name": "field1", "type": "string"}]}`
	schemaID3, err := m.Register(ctx, schemaName, schemaDefinition2)
	require.NoError(t, err)
	require.NotEqual(t, schemaID.glueSchemaID, schemaID3.glueSchemaID)
}

func TestGlueSchemaManager_Lookup(t *testing.T) {
	ctx := context.Background()
	m := newClueSchemaManagerForTest()

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
	m := newClueSchemaManagerForTest()

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
	m := newClueSchemaManagerForTest()

	registryType := m.RegistryType()
	require.Equal(t, common.SchemaRegistryTypeGlue, registryType)
}

func TestGlueSchemaManager_getMsgHeader(t *testing.T) {
	ctx := context.Background()
	m := newClueSchemaManagerForTest()

	schemaName := "test_schema"
	schemaDefinition := `{"type": "record", "name": "test_schema", "fields": [{"name": "field1", "type": "string"}]}`
	schemaID, err := m.Register(ctx, schemaName, schemaDefinition)
	require.NoError(t, err)

	header, err := m.getMsgHeader(schemaID.glueSchemaID)
	require.NoError(t, err)
	require.Equal(t, header[0], headerVersionByte)
	require.Equal(t, header[1], compressionDefaultByte)
	sid, err := getGlueSchemaIDFromHeader(header)
	require.NoError(t, err)
	require.Equal(t, schemaID.glueSchemaID, sid)
}
