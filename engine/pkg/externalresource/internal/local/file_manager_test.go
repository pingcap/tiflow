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

package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/stretchr/testify/require"
)

func newResourceIdentForTesting(executor, workerID, resourceName string) internal.ResourceIdent {
	return internal.ResourceIdent{
		Name: resourceName,
		ResourceScope: internal.ResourceScope{
			ProjectInfo: tenant.NewProjectInfo("fakeTenant", "fakeProject"),
			Executor:    resModel.ExecutorID(executor),
			WorkerID:    workerID,
		},
	}
}

func TestFileManagerBasics(t *testing.T) {
	t.Parallel()

	executorID := "executor-1"
	dir := t.TempDir()
	fm := NewLocalFileManager(model.ExecutorID(executorID), resModel.LocalFileConfig{BaseDir: dir})

	// In this test, we create resource-1 and resource-2, and only
	// resource-1 will be marked as persisted.
	//
	// Then we test that resource-2 can be correctly cleaned up as
	// temporary files, while resource-1 can be cleaned up as a persisted
	// resource.

	ctx := context.Background()
	// Creates resource-1
	res, err := fm.CreateResource(ctx,
		newResourceIdentForTesting(executorID, "worker-1", "resource-1"))
	require.NoError(t, err)
	res1, ok := res.(*resourceDescriptor)
	require.True(t, ok)
	require.Equal(t, &resourceDescriptor{
		BasePath: dir,
		Ident: internal.ResourceIdent{
			Name: "resource-1",
			ResourceScope: internal.ResourceScope{
				ProjectInfo: tenant.NewProjectInfo("fakeTenant", "fakeProject"),
				WorkerID:    "worker-1",
				Executor:    model.ExecutorID(executorID),
			},
		},
	}, res1)

	storage, err := newBrStorageForLocalFile(res1.AbsolutePath())
	require.NoError(t, err)
	fwriter, err := storage.Create(context.Background(), "1.txt", nil)
	require.NoError(t, err)
	err = fwriter.Close(context.Background())
	require.NoError(t, err)
	require.FileExists(t, res1.AbsolutePath()+"/1.txt")

	fm.SetPersisted(ctx, newResourceIdentForTesting(executorID, "worker-1", "resource-1"))

	// Creates resource-2
	res, err = fm.CreateResource(ctx, newResourceIdentForTesting(executorID, "worker-1", "resource-2"))
	require.NoError(t, err)
	res2, ok := res.(*resourceDescriptor)
	require.True(t, ok)
	require.Equal(t, &resourceDescriptor{
		BasePath: dir,
		Ident: internal.ResourceIdent{
			Name: "resource-2",
			ResourceScope: internal.ResourceScope{
				ProjectInfo: tenant.NewProjectInfo("fakeTenant", "fakeProject"),
				WorkerID:    "worker-1",
				Executor:    model.ExecutorID(executorID),
			},
		},
	}, res2)

	storage, err = newBrStorageForLocalFile(res2.AbsolutePath())
	require.NoError(t, err)
	fwriter, err = storage.Create(context.Background(), "1.txt", nil)
	require.NoError(t, err)
	err = fwriter.Close(context.Background())
	require.NoError(t, err)
	require.FileExists(t, res2.AbsolutePath()+"/1.txt")

	// Clean up temporary files
	err = fm.RemoveTemporaryFiles(ctx, internal.ResourceScope{
		Executor: model.ExecutorID(executorID), WorkerID: "worker-1",
	})
	require.NoError(t, err)

	require.NoDirExists(t, res2.AbsolutePath())
	require.DirExists(t, res1.AbsolutePath())

	// Clean up persisted resource
	err = fm.RemoveResource(ctx, newResourceIdentForTesting(executorID, "worker-1", "resource-1"))
	require.NoError(t, err)
	require.NoDirExists(t, res1.AbsolutePath())

	// Test repeated removals
	err = fm.RemoveResource(ctx, newResourceIdentForTesting(executorID, "worker-1", "resource-1"))
	require.Error(t, err)
	require.Regexp(t, ".*ErrResourceDoesNotExist.*", err)
}

func TestFileManagerManyWorkers(t *testing.T) {
	t.Parallel()

	const numWorkers = 10

	dir := t.TempDir()
	fm := NewLocalFileManager("", resModel.LocalFileConfig{BaseDir: dir})
	ctx := context.Background()

	for i := 0; i < numWorkers; i++ {
		// For each worker, first create a persisted resource
		res, err := fm.CreateResource(ctx, newResourceIdentForTesting("",
			fmt.Sprintf("worker-%d", i),
			fmt.Sprintf("resource-%d-1", i)))
		require.NoError(t, err)
		res1, ok := res.(*resourceDescriptor)
		require.True(t, ok)

		storage, err := newBrStorageForLocalFile(res1.AbsolutePath())
		require.NoError(t, err)
		fwriter, err := storage.Create(context.Background(), "1.txt", nil)
		require.NoError(t, err)
		err = fwriter.Close(context.Background())
		require.NoError(t, err)
		require.FileExists(t, res1.AbsolutePath()+"/1.txt")

		fm.SetPersisted(ctx, newResourceIdentForTesting("",
			fmt.Sprintf("worker-%d", i),
			fmt.Sprintf("resource-%d-1", i)))

		// Then create a temporary resource
		res, err = fm.CreateResource(ctx, newResourceIdentForTesting("",
			fmt.Sprintf("worker-%d", i),
			fmt.Sprintf("resource-%d-2", i)))
		require.NoError(t, err)
		res2, ok := res.(*resourceDescriptor)
		require.True(t, ok)

		storage, err = newBrStorageForLocalFile(res2.AbsolutePath())
		require.NoError(t, err)
		fwriter, err = storage.Create(context.Background(), "1.txt", nil)
		require.NoError(t, err)
		err = fwriter.Close(context.Background())
		require.NoError(t, err)
		require.FileExists(t, res2.AbsolutePath()+"/1.txt")
	}

	// Garbage collects about half the workers' temporary files.
	for i := 0; i < numWorkers/2; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		err := fm.RemoveTemporaryFiles(ctx, internal.ResourceScope{WorkerID: workerID})
		require.NoError(t, err)
	}

	for i := 0; i < numWorkers; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		resourceID1 := fmt.Sprintf("resource-%d-1", i)
		require.DirExists(t, filepath.Join(dir, workerID, ResourceNameToFilePathName(resourceID1)))

		resourceID2 := fmt.Sprintf("resource-%d-2", i)
		if i < numWorkers/2 {
			require.NoDirExists(t, filepath.Join(dir, workerID, ResourceNameToFilePathName(resourceID2)))
		} else {
			require.DirExists(t, filepath.Join(dir, workerID, ResourceNameToFilePathName(resourceID2)))
		}
	}
}

func TestCleanUpTemporaryFilesNotFound(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fm := NewLocalFileManager("", resModel.LocalFileConfig{BaseDir: dir})

	// Note that worker-1 does not have any resource.
	err := fm.RemoveTemporaryFiles(context.Background(),
		internal.ResourceScope{WorkerID: "worker-1"})
	// We expect NoError because it is normal for a worker
	// to never create any resource.
	require.NoError(t, err)
}

func TestCreateAndGetResource(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fm := NewLocalFileManager("", resModel.LocalFileConfig{BaseDir: dir})
	ctx := context.Background()
	ident := newResourceIdentForTesting("", "worker-1", "resource-1")
	_, err := fm.GetPersistedResource(ctx, ident)
	require.Error(t, err)
	require.Regexp(t, ".*ErrResourceDoesNotExist.*", err)

	_, err = fm.CreateResource(ctx, ident)
	require.NoError(t, err)

	_, err = fm.GetPersistedResource(ctx, ident)
	require.Error(t, err)
	require.Regexp(t, ".*ErrResourceDoesNotExist.*", err)

	fm.SetPersisted(ctx, ident)
	_, err = fm.GetPersistedResource(ctx, ident)
	require.NoError(t, err)

	err = fm.RemoveResource(ctx, ident)
	require.NoError(t, err)

	_, err = fm.GetPersistedResource(ctx, ident)
	require.Error(t, err)
	require.Regexp(t, ".*ErrResourceDoesNotExist.*", err)
}

func TestResourceNamesWithSlash(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fm := NewLocalFileManager("", resModel.LocalFileConfig{BaseDir: dir})

	ctx := context.Background()
	_, err := fm.CreateResource(ctx, newResourceIdentForTesting("", "worker-1", "a"))
	require.NoError(t, err)

	_, err = fm.CreateResource(ctx, newResourceIdentForTesting("", "worker-1", "a/b"))
	require.NoError(t, err)

	_, err = fm.CreateResource(ctx, newResourceIdentForTesting("", "worker-1", "a/b/c"))
	require.NoError(t, err)

	fm.SetPersisted(ctx, newResourceIdentForTesting("", "worker-1", "a/b/c"))
	_, err = fm.GetPersistedResource(ctx, newResourceIdentForTesting("", "worker-1", "a/b/c"))
	require.NoError(t, err)

	err = fm.RemoveTemporaryFiles(ctx, internal.ResourceScope{WorkerID: "worker-1"})
	require.NoError(t, err)

	_, err = fm.GetPersistedResource(ctx, newResourceIdentForTesting("", "worker-1", "a/b/c"))
	require.NoError(t, err)
}

func TestPreCheckConfig(t *testing.T) {
	t.Parallel()

	// Happy path
	dir := t.TempDir()
	err := PreCheckConfig(resModel.LocalFileConfig{BaseDir: dir})
	require.NoError(t, err)

	// Directory does not exist but can be created.
	baseDir := filepath.Join(dir, "not-exist")
	err = PreCheckConfig(resModel.LocalFileConfig{BaseDir: baseDir})
	require.NoError(t, err)

	// Directory exists but not writable
	baseDir = filepath.Join(dir, "not-writable")
	require.NoError(t, os.MkdirAll(baseDir, 0o400))
	err = PreCheckConfig(resModel.LocalFileConfig{BaseDir: baseDir})
	require.Error(t, err)
	require.Regexp(t, ".*ErrLocalFileDirNotWritable.*", err)
}
