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

package s3

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/stretchr/testify/require"
)

const (
	utBucketName   = "engine-ut"
	mockExecutorID = "executor-1"
)

type mockExternalStorageFactory struct {
	t       *testing.T
	tempDir string
}

func newMockExternalStorageFactory(t *testing.T) *mockExternalStorageFactory {
	return &mockExternalStorageFactory{
		t:       t,
		tempDir: t.TempDir(),
	}
}

func (f *mockExternalStorageFactory) newS3ExternalStorageForScope(
	ctx context.Context,
	bucket BucketName,
	scope internal.ResourceScope,
) (brStorage.ExternalStorage, error) {
	uri := f.scopeURI(bucket, scope)
	return f.newS3ExternalStorageFromURI(ctx, uri)
}

func (f *mockExternalStorageFactory) newS3ExternalStorageFromURI(
	ctx context.Context,
	uri string,
) (brStorage.ExternalStorage, error) {
	ls, err := brStorage.NewLocalStorage(uri)
	require.NoError(f.t, err)
	return ls, nil
}

func (f *mockExternalStorageFactory) scopeURI(
	bucket BucketName,
	scope internal.ResourceScope,
) string {
	return fmt.Sprintf("%s/%s/%s/%s",
		f.tempDir, bucket, scope.Executor, scope.WorkerID)
}

func (f *mockExternalStorageFactory) assertFileExists(uri string) {
	require.FileExists(f.t, filepath.Join(f.tempDir, uri))
}

func (f *mockExternalStorageFactory) assertFileNotExist(uri string) {
	require.NoFileExists(f.t, filepath.Join(f.tempDir, uri))
}

func newFileManagerForUT(t *testing.T) (*FileManager, *mockExternalStorageFactory) {
	factory := newMockExternalStorageFactory(t)
	return NewFileManagerWithFactory(
		mockExecutorID,
		NewConstantBucketSelector(utBucketName),
		factory,
	), factory
}

func TestFileManagerCreateResource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fm, factory := newFileManagerForUT(t)

	desc, err := fm.CreateResource(ctx, internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: mockExecutorID,
			WorkerID: "worker-1",
		},
		Name: "resource-1",
	})
	require.NoError(t, err)
	factory.assertFileExists(filepath.Join(
		utBucketName, mockExecutorID, "worker-1", "resource-1", placeholderFileName))

	storage, err := desc.ExternalStorage(ctx)
	require.NoError(t, err)
	err = storage.WriteFile(ctx, "file-1", []byte("dummydummy"))
	require.NoError(t, err)
	factory.assertFileExists(filepath.Join(
		utBucketName, mockExecutorID, "worker-1", "resource-1", "file-1"))
}
