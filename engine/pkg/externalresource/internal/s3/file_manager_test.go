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
	"path/filepath"
	"testing"

	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestFileManagerCreateAndRemoveResource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fm, factory := NewFileManagerForUT(t.TempDir(), MockExecutorID)

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
	fm, _ := NewFileManagerForUT(t.TempDir(), MockExecutorID)

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
	fm, _ := NewFileManagerForUT(t.TempDir(), MockExecutorID)

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
	fm, _ := NewFileManagerForUT(t.TempDir(), MockExecutorID)

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
	fm, factory := NewFileManagerForUT(t.TempDir(), MockExecutorID)

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
	fm1 := NewFileManagerForUTFromSharedStorageFactory("executor-1", factory)
	fm2 := NewFileManagerForUTFromSharedStorageFactory("executor-2", factory)

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
	// require.True(t, errors.Is(err, errors.ErrResourceFilesNotFound))

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
	require.True(t, errors.Is(err, errors.ErrResourceFilesNotFound))
}

func TestFileManagerCleanOrRecreatePersistedResource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	factory := newMockExternalStorageFactory(t.TempDir(), UtBucketName)
	fm1 := NewFileManagerForUTFromSharedStorageFactory("executor-1", factory)
	fm2 := NewFileManagerForUTFromSharedStorageFactory("executor-2", factory)

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
	err = fm1.SetPersisted(ctx, ident)
	require.NoError(t, err)

	// clean from creator
	err = storage.WriteFile(ctx, "file-1", []byte("test-content"))
	require.NoError(t, err)
	_, err = fm1.CleanOrRecreatePersistedResource(ctx, ident)
	require.NoError(t, err)
	ok, err := storage.FileExists(ctx, placeholderFileName)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = storage.FileExists(ctx, "file-1")
	require.NoError(t, err)
	require.False(t, ok)

	err = storage.WriteFile(ctx, "file-1", []byte("test-content"))
	require.NoError(t, err)
	ok, err = storage.FileExists(ctx, "file-1")
	require.NoError(t, err)
	require.True(t, ok)

	// clean from other node
	_, err = fm2.CleanOrRecreatePersistedResource(ctx, ident)
	require.NoError(t, err)
	ok, err = storage.FileExists(ctx, placeholderFileName)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = storage.FileExists(ctx, "file-1")
	require.NoError(t, err)
	require.False(t, ok)

	// clean non-existent resources from other nodes
	err = fm1.RemoveResource(ctx, ident)
	require.NoError(t, err)
	ok, err = storage.FileExists(ctx, placeholderFileName)
	require.NoError(t, err)
	require.False(t, ok)
	_, err = fm2.CleanOrRecreatePersistedResource(ctx, ident)
	require.NoError(t, err)
	ok, err = storage.FileExists(ctx, placeholderFileName)
	require.NoError(t, err)
	require.True(t, ok)
}
