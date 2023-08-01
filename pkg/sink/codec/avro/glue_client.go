package avro

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/glue"
)

// glueClient is a partial interface of glue client, used to mock glue client in unit test
type glueClient interface {
	CreateSchema(ctx context.Context, params *glue.CreateSchemaInput, optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error)
	RegisterSchemaVersion(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error)
	GetSchemaVersion(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error)
}

type mockGlueClientImpl struct {
	createSchemasOutput          map[string]*glue.CreateSchemaOutput          // schemaName -> schema
	registerSchemaVersionsOutput map[string]*glue.RegisterSchemaVersionOutput // schemaName -> schema
	getSchemaVersionsOutput      map[string]*glue.GetSchemaVersionOutput      // schemaVersionID -> schema
}

func newMockGlueClientImpl() *mockGlueClientImpl {
	return &mockGlueClientImpl{
		createSchemasOutput:          make(map[string]*glue.CreateSchemaOutput),
		registerSchemaVersionsOutput: make(map[string]*glue.RegisterSchemaVersionOutput),
		getSchemaVersionsOutput:      make(map[string]*glue.GetSchemaVersionOutput),
	}
}

func (m *mockGlueClientImpl) CreateSchema(
	ctx context.Context,
	params *glue.CreateSchemaInput,
	optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
	return m.createSchemasOutput[*params.SchemaName], nil
}

func (m *mockGlueClientImpl) RegisterSchemaVersion(
	ctx context.Context,
	params *glue.RegisterSchemaVersionInput,
	optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
	return m.registerSchemaVersionsOutput[*params.SchemaId.SchemaName], nil
}

func (m *mockGlueClientImpl) GetSchemaVersion(
	ctx context.Context,
	params *glue.GetSchemaVersionInput,
	optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
	return m.getSchemaVersionsOutput[*params.SchemaVersionId], nil
}
