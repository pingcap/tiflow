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

package bucket

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

const (
	envS3Endpoint        = "ENGINE_S3_ENDPOINT"
	envS3AccessKeyID     = "ENGINE_S3_ACCESS_KEY"
	envS3SecretAccessKey = "ENGINE_S3_SECRET_KEY"

	// UtBucketName is the bucket name for UT
	UtBucketName = "engine-ut"
	// MockExecutorID is the executor ID for mock executor
	MockExecutorID = "executor-1"
)

// GetS3OptionsForUT returns the s3 options for unit test.
func GetS3OptionsForUT() (*brStorage.S3BackendOptions, error) {
	endpoint := os.Getenv(envS3Endpoint)
	if len(endpoint) == 0 {
		return nil, errors.Errorf("empty endpoint in env %s", envS3Endpoint)
	}

	accessKeyID := os.Getenv(envS3AccessKeyID)
	if len(accessKeyID) == 0 {
		return nil, errors.Errorf("empty access key ID in env %s", envS3AccessKeyID)
	}

	secretAccessKey := os.Getenv(envS3SecretAccessKey)
	if len(secretAccessKey) == 0 {
		return nil, errors.Errorf("empty secret access key in env %s", envS3SecretAccessKey)
	}

	return &brStorage.S3BackendOptions{
		Endpoint:        endpoint,
		AccessKey:       accessKeyID,
		SecretAccessKey: secretAccessKey,
		Provider:        "minio",
		ForcePathStyle:  true,
	}, nil
}

type mockBucketCreator struct {
	baseDir string
	bucket  string
}

func newMockBucketCreator(tempDir string, bucket string) *mockBucketCreator {
	return &mockBucketCreator{
		baseDir: tempDir,
		bucket:  bucket,
	}
}

func (f *mockBucketCreator) newBucketForScope(
	ctx context.Context, scope internal.ResourceScope,
) (brStorage.ExternalStorage, error) {
	uri := fmt.Sprintf("%s/%s", f.baseURI(), scope.BuildResPath())
	return f.newBucketFromURI(ctx, uri)
}

func (f *mockBucketCreator) newBucketFromURI(
	ctx context.Context,
	uri string,
) (brStorage.ExternalStorage, error) {
	return brStorage.NewLocalStorage(uri)
}

func (f *mockBucketCreator) baseURI() string {
	return fmt.Sprintf("%s/%s", f.baseDir, f.bucket)
}

func (f *mockBucketCreator) resourceType() resModel.ResourceType {
	return resModel.ResourceTypeS3
}

func (f *mockBucketCreator) assertFileExists(t *testing.T, uri string) {
	require.FileExists(t, filepath.Join(f.baseDir, uri))
}

func (f *mockBucketCreator) assertFileNotExist(t *testing.T, uri string) {
	require.NoFileExists(t, filepath.Join(f.baseDir, uri))
}

// NewFileManagerForUT returns a file manager for UT.
func NewFileManagerForUT(tempDir string, executorID resModel.ExecutorID) (*FileManager, *mockBucketCreator) {
	creator := newMockBucketCreator(tempDir, UtBucketName)
	return NewFileManager(
		executorID,
		creator,
	), creator
}

// NewFileManagerForUTFromSharedStorageFactory returns a file manager for UT.
func NewFileManagerForUTFromSharedStorageFactory(
	executorID model.ExecutorID, creator *mockBucketCreator,
) *FileManager {
	return NewFileManager(executorID, creator)
}
