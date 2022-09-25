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

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/s3"
	"github.com/stretchr/testify/require"
)

const (
	utBucketName   = "engine-ut"
	mockExecutorID = "executor-1"

	caseTimeout = 10 * time.Second
)

func newFileManagerForTest(t *testing.T) *s3.FileManager {
	options, err := s3.GetS3OptionsForUT()
	if err != nil {
		t.Skipf("server not configured for s3 integration: %s", err.Error())
	}

	pathPrefix := fmt.Sprintf("%s-%s", t.Name(), time.Now().Format("20060102-150405"))
	factory := s3.NewExternalStorageFactoryWithPrefix(pathPrefix, options)
	return s3.NewFileManager(
		mockExecutorID,
		s3.NewConstantBucketSelector(utBucketName),
		factory,
	)
}

func TestIntegrationS3FileManagerBasics(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()

	fm := newFileManagerForTest(t)
	ident := internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: mockExecutorID,
			WorkerID: "worker-1",
		},
		Name: "resource-1",
	}

	desc, err := fm.CreateResource(ctx, internal.ResourceIdent{
		ResourceScope: internal.ResourceScope{
			Executor: mockExecutorID,
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

func TestIntegrationS3FileManagerTemporaryResources(t *testing.T) {
	t.Parallel()

	const (
		numTemporaryResources = 10
		numPersistedResources = 10
	)

	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()

	fm := newFileManagerForTest(t)
	scope := internal.ResourceScope{
		Executor: mockExecutorID,
		WorkerID: "worker-1",
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

	err := fm.RemoveTemporaryFiles(ctx, scope)
	require.NoError(t, err)

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
		require.ErrorContains(t, err, "ResourceFilesNotFoundError")
	}
}
