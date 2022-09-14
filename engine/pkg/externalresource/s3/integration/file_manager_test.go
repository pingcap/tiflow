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
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/s3"
	"github.com/stretchr/testify/require"
)

const (
	utBucketName   = "engine-ut"
	mockExecutorID = "executor-1"

	caseTimeout = 10 * time.Second
)

func newFileManagerForTest(t *testing.T) *s3.FileManager {
	options, err := getS3OptionsForUT()
	require.NoError(t, err)

	pathPrefix := fmt.Sprintf("%d", rand.Int())
	factory := s3.NewExternalStorageFactoryWithPrefix(pathPrefix)
	return s3.NewFileManagerWithFactory(
		mockExecutorID,
		s3.NewConstantBucketSelector(utBucketName),
		factory,
		options,
	)
}

func TestFileManagerBasics(t *testing.T) {
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

func TestFileManagerTemporaryResources(t *testing.T) {
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
