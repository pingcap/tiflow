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

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
)

// glueClient is a partial interface of glue client, used to mock glue client in unit test
type glueClient interface {
	GetRegistry(ctx context.Context,
		params *glue.GetRegistryInput,
		optFns ...func(*glue.Options)) (*glue.GetRegistryOutput, error)
	CreateSchema(ctx context.Context,
		params *glue.CreateSchemaInput,
		optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error)
	RegisterSchemaVersion(ctx context.Context,
		params *glue.RegisterSchemaVersionInput,
		optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error)
	GetSchemaVersion(ctx context.Context,
		params *glue.GetSchemaVersionInput,
		optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error)
}

type mockGlueClientImpl struct {
	createSchemaInput           map[string]*glue.CreateSchemaInput          // schemaName -> schema
	registerSchemaVersionsInput map[string]*glue.RegisterSchemaVersionInput // schemaName -> schema
}

func newMockGlueClientImpl() *mockGlueClientImpl {
	return &mockGlueClientImpl{
		createSchemaInput:           make(map[string]*glue.CreateSchemaInput),
		registerSchemaVersionsInput: make(map[string]*glue.RegisterSchemaVersionInput),
	}
}

func (m *mockGlueClientImpl) CreateSchema(
	ctx context.Context,
	params *glue.CreateSchemaInput,
	optFns ...func(*glue.Options),
) (*glue.CreateSchemaOutput, error) {
	id := uuid.New()
	sid := id.String()
	// here use Description to store schemaVersionID for test purpose
	params.Description = &sid
	m.createSchemaInput[*params.SchemaName] = params
	m.registerSchemaVersionsInput[sid] = &glue.RegisterSchemaVersionInput{
		SchemaId: &types.SchemaId{
			SchemaName: params.SchemaName,
			// here use schemaArn as schemaVersionId for test purpose
			SchemaArn: &sid,
		},
		SchemaDefinition: params.SchemaDefinition,
	}
	return &glue.CreateSchemaOutput{
		SchemaVersionId: &sid,
	}, nil
}

func (m *mockGlueClientImpl) RegisterSchemaVersion(
	ctx context.Context,
	params *glue.RegisterSchemaVersionInput,
	optFns ...func(*glue.Options),
) (*glue.RegisterSchemaVersionOutput, error) {
	old, ok := m.createSchemaInput[*params.SchemaId.SchemaName]
	if !ok {
		return nil, errors.New("EntityNotFoundException")
	}

	// if schema definition is the same, don't create new schema version
	if *old.SchemaDefinition == *params.SchemaDefinition {
		return &glue.RegisterSchemaVersionOutput{
			SchemaVersionId: old.Description,
		}, nil
	}

	m.createSchemaInput[*params.SchemaId.SchemaName] = &glue.CreateSchemaInput{
		SchemaDefinition: params.SchemaDefinition,
	}
	id := uuid.New()
	sid := id.String()
	params.SchemaId.SchemaArn = &sid
	m.registerSchemaVersionsInput[*params.SchemaId.SchemaArn] = params
	return &glue.RegisterSchemaVersionOutput{
		SchemaVersionId: params.SchemaId.SchemaArn,
	}, nil
}

func (m *mockGlueClientImpl) GetSchemaVersion(
	ctx context.Context,
	params *glue.GetSchemaVersionInput,
	optFns ...func(*glue.Options),
) (*glue.GetSchemaVersionOutput, error) {
	if params.SchemaVersionId == nil {
		res, ok := m.createSchemaInput[*params.SchemaId.SchemaName]
		if !ok {
			return nil, errors.New("EntityNotFoundException")
		}
		return &glue.GetSchemaVersionOutput{SchemaDefinition: res.SchemaDefinition}, nil
	}

	res, ok := m.registerSchemaVersionsInput[*params.SchemaVersionId]
	if !ok {
		return nil, errors.New("EntityNotFoundException")
	}
	return &glue.GetSchemaVersionOutput{SchemaDefinition: res.SchemaDefinition}, nil
}

func (m *mockGlueClientImpl) GetRegistry(
	ctx context.Context,
	params *glue.GetRegistryInput,
	optFns ...func(*glue.Options),
) (*glue.GetRegistryOutput, error) {
	return nil, nil
}
