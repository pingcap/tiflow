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

	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
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

	fm, factory := newFileManagerForUT(t)
	workers := []string{"worker-1", "worker-2", "worker-3"}
	persistedResources := []*resModel.ResourceMeta{}
	// generate mock data
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
			name := fmt.Sprintf("persisted-resource-%d", i)
			ident := internal.ResourceIdent{
				ResourceScope: scope,
				Name:          name,
			}
			_, err := fm.CreateResource(ctx, ident)
			require.NoError(t, err)

			err = fm.SetPersisted(ctx, ident)
			require.NoError(t, err)

			persistedResources = append(persistedResources, &resModel.ResourceMeta{
				ID:       "/s3/" + name,
				Executor: ident.Executor,
				Worker:   worker,
			})
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
				require.ErrorContains(t, err, "ResourceFilesNotFoundError")
			} else {
				require.NoError(t, err)
			}
		}
	}
	checkWorker(workers[0], false)
	checkWorker(workers[1], false)
	checkWorker(workers[2], false)

	// test GCExecutor
	fm1 := newFileManagerForUTFromSharedStorageFactory("leader-controller", factory)
	controller := &resourceController{fm: fm1}
	gcExecutor := func() {
		err := controller.GCExecutor(ctx, persistedResources, MockExecutorID)
		require.NoError(t, err)
		checkWorker(workers[0], true)
		checkWorker(workers[1], true)
		checkWorker(workers[2], true)
	}
	gcExecutor()
	// test for idempotency
	gcExecutor()

	// test GCSingleResource
	for _, res := range persistedResources {
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
		require.ErrorContains(t, err, "ResourceFilesNotFoundError")

		// test for idempotency
		require.NoError(t, controller.GCSingleResource(ctx, res))
		_, err = fm.GetPersistedResource(ctx, ident)
		require.ErrorContains(t, err, "ResourceFilesNotFoundError")
	}
}
