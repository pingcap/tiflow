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
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

const (
	caseTimeout = 10 * time.Second
)

func newFileManagerForTest(t *testing.T) *FileManager {
	// Remove the following comments if running tests locally.
	// os.Setenv("ENGINE_S3_ENDPOINT", "http://127.0.0.1:9000/")
	// os.Setenv("ENGINE_S3_ACCESS_KEY", "engine")
	// os.Setenv("ENGINE_S3_SECRET_KEY", "engineSecret")
	options, err := GetS3OptionsForUT()
	if err != nil {
		t.Skipf("server not configured for s3 integration: %s", err.Error())
	}

	pathPrefix := fmt.Sprintf("%s-%s", t.Name(), time.Now().Format("20060102-150405"))
	config := resModel.S3Config{
		S3BackendOptions: *options,
		Bucket:           UtBucketName,
		Prefix:           pathPrefix,
	}
	err = PreCheckConfig(config)
	require.NoError(t, err)
	return NewFileManagerWithConfig(MockExecutorID, config)
}

func TestIntegrationS3FileManagerBasics(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()

	fm := newFileManagerForTest(t)
	ident := internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: "worker-1",
		},
		Name: "resource-1",
	}

	desc, err := fm.CreateResource(ctx, internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: "worker-1",
		},
		Name: "resource-1",
	})
	require.NoError(t, err)

	storage, err := desc.ExternalStorage(ctx)
	require.NoError(t, err)

	err = storage.WriteFile(ctx, "file-1", []byte("content-1"))
	require.NoError(t, err)

	err = fm.SetPersisted(ctx, ident)
	require.NoError(t, err)

	desc, err = fm.GetPersistedResource(ctx, ident)
	require.NoError(t, err)

	storage, err = desc.ExternalStorage(ctx)
	require.NoError(t, err)

	bytes, err := storage.ReadFile(ctx, "file-1")
	require.NoError(t, err)
	require.Equal(t, []byte("content-1"), bytes)

	err = fm.RemoveResource(ctx, ident)
	require.NoError(t, err)
}

func TestIntegrationS3FileManagerRemoveTemporaryResources(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()

	fm := newFileManagerForTest(t)
	workers := []string{"worker-1", "worker-2", "worker-3"}
	for _, worker := range workers {
		scope := internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: worker,
		}

		for i := 0; i < numTemporaryResources; i++ {
			_, err := fm.CreateResource(ctx, internal.ResourceIdent{
				ResourceScope: scope,
				Name:          fmt.Sprintf("temp-resource-%d", i),
			})
			require.NoError(t, err)
		}

		for i := 0; i < numPersistedResources; i++ {
			ident := internal.ResourceIdent{
				ResourceScope: scope,
				Name:          fmt.Sprintf("persisted-resource-%d", i),
			}
			_, err := fm.CreateResource(ctx, ident)
			require.NoError(t, err)

			err = fm.SetPersisted(ctx, ident)
			require.NoError(t, err)
		}
	}

	checkWorker := func(worker string, removed bool) {
		scope := internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: worker,
		}
		for i := 0; i < numPersistedResources; i++ {
			ident := internal.ResourceIdent{
				ResourceScope: scope,
				Name:          fmt.Sprintf("persisted-resource-%d", i),
			}
			_, err := fm.GetPersistedResource(ctx, ident)
			require.NoError(t, err)
		}

		for i := 0; i < numTemporaryResources; i++ {
			_, err := fm.GetPersistedResource(ctx, internal.ResourceIdent{
				ResourceScope: scope,
				Name:          fmt.Sprintf("temp-resource-%d", i),
			})
			if removed {
				require.True(t, errors.Is(err, errors.ErrResourceFilesNotFound))
			} else {
				require.NoError(t, err)
			}
		}
	}

	// remove worker-1
	removeWorker0 := func() {
		err := fm.RemoveTemporaryFiles(ctx, internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: workers[0],
		})
		require.NoError(t, err)
		checkWorker(workers[0], true)
		checkWorker(workers[1], false)
		checkWorker(workers[2], false)
	}
	removeWorker0()
	// test for idempotency
	removeWorker0()

	// remove executor
	removeExecutor := func() {
		err := fm.RemoveTemporaryFiles(ctx, internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: "",
		})
		require.NoError(t, err)
		checkWorker(workers[0], true)
		checkWorker(workers[1], true)
		checkWorker(workers[2], true)
	}
	removeExecutor()
	// test for idempotency
	removeExecutor()
}
