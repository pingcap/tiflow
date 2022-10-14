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
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/stretchr/testify/require"
)

type mockExternalStorageFactory struct {
	baseDir string
	bucket  string
}

func newMockExternalStorageFactory(tempDir string, bucket string) *mockExternalStorageFactory {
	return &mockExternalStorageFactory{
		baseDir: tempDir,
		bucket:  bucket,
	}
}

func (f *mockExternalStorageFactory) newS3ExternalStorageForScope(
	ctx context.Context, scope internal.ResourceScope,
) (brStorage.ExternalStorage, error) {
	uri := fmt.Sprintf("%s/%s", f.baseURI(), scope.BuildResPath())
	return f.newS3ExternalStorageFromURI(ctx, uri)
}

func (f *mockExternalStorageFactory) newS3ExternalStorageFromURI(
	ctx context.Context,
	uri string,
) (brStorage.ExternalStorage, error) {
	return brStorage.NewLocalStorage(uri)
}

func (f *mockExternalStorageFactory) baseURI() string {
	return fmt.Sprintf("%s/%s", f.baseDir, f.bucket)
}

func (f *mockExternalStorageFactory) assertFileExists(t *testing.T, uri string) {
	require.FileExists(t, filepath.Join(f.baseDir, uri))
}

func (f *mockExternalStorageFactory) assertFileNotExist(t *testing.T, uri string) {
	require.NoFileExists(t, filepath.Join(f.baseDir, uri))
}

func newFileManagerForUT(t *testing.T) (*FileManager, *mockExternalStorageFactory) {
	factory := newMockExternalStorageFactory(t.TempDir(), UtBucketName)
	return NewFileManager(
		MockExecutorID,
		factory,
	), factory
}

func newFileManagerForUTFromSharedStorageFactory(
	executorID model.ExecutorID, factory *mockExternalStorageFactory,
) *FileManager {
	return NewFileManager(executorID, factory)
}

func TestFileManagerCreateAndRemoveResource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fm, factory := newFileManagerForUT(t)

	ident := internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: "worker-1",
		},
		Name: "resource-1",
	}
	desc, err := fm.CreateResource(ctx, ident)
	require.NoError(t, err)
	factory.assertFileExists(t, filepath.Join(
		UtBucketName, MockExecutorID, "worker-1", "resource-1", placeholderFileName))

	storage, err := desc.ExternalStorage(ctx)
	require.NoError(t, err)
	err = storage.WriteFile(ctx, "file-1", []byte("dummydummy"))
	require.NoError(t, err)
	factory.assertFileExists(t, filepath.Join(
		UtBucketName, MockExecutorID, "worker-1", "resource-1", "file-1"))

	err = fm.RemoveResource(ctx, ident)
	require.NoError(t, err)

	factory.assertFileNotExist(t, filepath.Join(
		UtBucketName, MockExecutorID, "worker-1", "resource-1", placeholderFileName))
	factory.assertFileNotExist(t, filepath.Join(
		UtBucketName, MockExecutorID, "worker-1", "resource-1", "file-1"))
}

func TestFileManagerCreateDuplicate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fm, _ := newFileManagerForUT(t)

	ident := internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: "worker-1",
		},
		Name: "resource-1",
	}
	_, err := fm.CreateResource(ctx, ident)
	require.NoError(t, err)

	_, err = fm.CreateResource(ctx, ident)
	require.ErrorContains(t, err, "resource already exists")
}

func TestFileManagerSetAndGetPersisted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fm, _ := newFileManagerForUT(t)

	ident := internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: "worker-1",
		},
		Name: "resource-1",
	}
	desc, err := fm.CreateResource(ctx, ident)
	require.NoError(t, err)

	storage, err := desc.ExternalStorage(ctx)
	require.NoError(t, err)
	err = storage.WriteFile(ctx, "file-1", []byte("dummydummy"))
	require.NoError(t, err)

	err = fm.SetPersisted(ctx, ident)
	require.NoError(t, err)

	desc, err = fm.GetPersistedResource(ctx, ident)
	require.NoError(t, err)
	storage, err = desc.ExternalStorage(ctx)
	require.NoError(t, err)
	ok, err := storage.FileExists(ctx, "file-1")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestFileManagerDoublePersisted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fm, _ := newFileManagerForUT(t)

	ident := internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: "worker-1",
		},
		Name: "resource-1",
	}
	desc, err := fm.CreateResource(ctx, ident)
	require.NoError(t, err)

	storage, err := desc.ExternalStorage(ctx)
	require.NoError(t, err)
	err = storage.WriteFile(ctx, "file-1", []byte("dummydummy"))
	require.NoError(t, err)

	err = fm.SetPersisted(ctx, ident)
	require.NoError(t, err)

	// Double persistence is allowed to maintain idempotency.
	err = fm.SetPersisted(ctx, ident)
	require.NoError(t, err)
}

func TestFileManagerRemoveTemporaryResources(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fm, factory := newFileManagerForUT(t)

	ident1 := internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: "executor-1",
			WorkerID: "worker-1",
		},
		Name: "resource-1",
	}
	_, err := fm.CreateResource(ctx, ident1)
	require.NoError(t, err)

	err = fm.SetPersisted(ctx, ident1)
	require.NoError(t, err)

	factory.assertFileExists(t, filepath.Join(
		UtBucketName, MockExecutorID, "worker-1", "resource-1", ".keep"))

	ident2 := internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: "executor-1",
			WorkerID: "worker-1",
		},
		Name: "resource-2",
	}
	_, err = fm.CreateResource(ctx, ident2)
	require.NoError(t, err)

	factory.assertFileExists(t, filepath.Join(
		UtBucketName, MockExecutorID, "worker-1", "resource-2", ".keep"))

	err = fm.RemoveTemporaryFiles(ctx, internal.ResourceScope{
		Executor: MockExecutorID,
		WorkerID: "worker-1",
	})
	require.NoError(t, err)

	factory.assertFileNotExist(t, filepath.Join(
		UtBucketName, MockExecutorID, "worker-1", "resource-2", ".keep"))
}

func TestFileManagerShareResourceAcrossExecutors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	factory := newMockExternalStorageFactory(t.TempDir(), UtBucketName)
	fm1 := newFileManagerForUTFromSharedStorageFactory("executor-1", factory)
	fm2 := newFileManagerForUTFromSharedStorageFactory("executor-2", factory)

	ident := internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: "executor-1",
			WorkerID: "worker-1",
		},
		Name: "resource-1",
	}
	desc, err := fm1.CreateResource(ctx, ident)
	require.NoError(t, err)

	storage, err := desc.ExternalStorage(ctx)
	require.NoError(t, err)

	err = storage.WriteFile(ctx, "file-1", []byte("test-content"))
	require.NoError(t, err)

	// TODO: Open the test here after using the contents of the placeholder
	// to indicate the persistent state.
	// _, err = fm2.GetPersistedResource(ctx, ident)
	// require.ErrorContains(t, err, "ResourceFilesNotFoundError")

	err = fm1.SetPersisted(ctx, ident)
	require.NoError(t, err)

	desc, err = fm2.GetPersistedResource(ctx, ident)
	require.NoError(t, err)

	storage, err = desc.ExternalStorage(ctx)
	require.NoError(t, err)

	bytes, err := storage.ReadFile(ctx, "file-1")
	require.NoError(t, err)
	require.Equal(t, []byte("test-content"), bytes)

	err = fm1.RemoveResource(ctx, ident)
	require.NoError(t, err)

	_, err = fm2.GetPersistedResource(ctx, ident)
	require.ErrorContains(t, err, "ResourceFilesNotFoundError")
}
