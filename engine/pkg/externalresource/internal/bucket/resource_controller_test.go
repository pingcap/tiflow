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
	"testing"

	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

const (
	numTemporaryResources = 10
	numPersistedResources = 10
)

func TestS3ResourceController(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()

	temproraryResNames := make([]resModel.ResourceName, numTemporaryResources)
	for i := 0; i < numTemporaryResources; i++ {
		resID := fmt.Sprintf("/s3/temporary-resource-%d", i)
		_, resName, err := resModel.ParseResourceID(resID)
		require.NoError(t, err)
		temproraryResNames[i] = resName
	}

	persistedResNames := make([]resModel.ResourceName, numPersistedResources)
	persistedResMetas := []*resModel.ResourceMeta{}
	for i := 0; i < numPersistedResources; i++ {
		resID := fmt.Sprintf("/s3/persisted-resource-%d", i)
		_, resName, err := resModel.ParseResourceID(resID)
		require.NoError(t, err)
		persistedResNames[i] = resName
	}

	fm, factory := NewFileManagerForUT(t.TempDir(), MockExecutorID)
	workers := []string{"worker-1", "worker-2", "worker-3"}
	// generate mock data
	for _, worker := range workers {
		scope := internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: worker,
		}

		for _, persistedResName := range persistedResNames {
			ident := internal.ResourceIdent{
				ResourceScope: scope,
				Name:          persistedResName,
			}
			_, err := fm.CreateResource(ctx, ident)
			require.NoError(t, err)

			err = fm.SetPersisted(ctx, ident)
			require.NoError(t, err)

			persistedResMetas = append(persistedResMetas, &resModel.ResourceMeta{
				ID:       resModel.BuildResourceID(resModel.ResourceTypeS3, persistedResName),
				Executor: MockExecutorID,
				Worker:   worker,
			})
		}

		for _, tempResName := range temproraryResNames {
			_, err := fm.CreateResource(ctx, internal.ResourceIdent{
				ResourceScope: scope,
				Name:          tempResName,
			})
			require.NoError(t, err)
		}
	}

	checkWorker := func(worker string, removed bool) {
		scope := internal.ResourceScope{
			Executor: MockExecutorID,
			WorkerID: worker,
		}
		for _, persistedResName := range persistedResNames {
			ident := internal.ResourceIdent{
				ResourceScope: scope,
				Name:          persistedResName,
			}
			_, err := fm.GetPersistedResource(ctx, ident)
			require.NoError(t, err)
		}

		for _, tempResName := range temproraryResNames {
			_, err := fm.GetPersistedResource(ctx, internal.ResourceIdent{
				ResourceScope: scope,
				Name:          tempResName,
			})
			if removed {
				require.True(t, errors.Is(err, errors.ErrResourceFilesNotFound))
			} else {
				require.NoError(t, err)
			}
		}
	}
	checkWorker(workers[0], false)
	checkWorker(workers[1], false)
	checkWorker(workers[2], false)

	// test GCExecutor
	fm1 := NewFileManagerForUTFromSharedStorageFactory("leader-controller", factory)
	controller := &resourceController{fm: fm1}
	gcExecutor := func() {
		err := controller.GCExecutor(ctx, persistedResMetas, MockExecutorID)
		require.NoError(t, err)
		checkWorker(workers[0], true)
		checkWorker(workers[1], true)
		checkWorker(workers[2], true)
	}
	gcExecutor()
	// test for idempotency
	gcExecutor()

	// test GCSingleResource
	for _, res := range persistedResMetas {
		_, resName, err := resModel.ParseResourceID(res.ID)
		require.NoError(t, err)
		ident := internal.ResourceIdent{
			ResourceScope: internal.ResourceScope{
				Executor: MockExecutorID,
				WorkerID: res.Worker,
			},
			Name: resName,
		}
		_, err = fm.GetPersistedResource(ctx, ident)
		require.NoError(t, err)

		require.NoError(t, controller.GCSingleResource(ctx, res))
		_, err = fm.GetPersistedResource(ctx, ident)
		require.True(t, errors.Is(err, errors.ErrResourceFilesNotFound))

		// test for idempotency
		require.NoError(t, controller.GCSingleResource(ctx, res))
		_, err = fm.GetPersistedResource(ctx, ident)
		require.True(t, errors.Is(err, errors.ErrResourceFilesNotFound))
	}
}
