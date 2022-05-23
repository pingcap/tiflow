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

package resourcemeta

import (
	"context"
	"fmt"
	"testing"
	"time"

	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/stretchr/testify/require"
)

func newAccessorWithMockKV() *MetadataAccessor {
	cli, _ := pkgOrm.NewMockClient()
	return NewMetadataAccessor(cli)
}

func TestAccessorBasics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	acc := newAccessorWithMockKV()
	_, found, err := acc.GetResource(ctx, "resource-1")
	require.NoError(t, err)
	require.False(t, found)

	ok, err := acc.CreateResource(ctx, &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-1",
		Executor: "executor-1",
		Deleted:  false,
	})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = acc.CreateResource(ctx, &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-1",
		Executor: "executor-1",
		Deleted:  false,
	})
	require.NoError(t, err)
	require.False(t, ok)

	resc, found, err := acc.GetResource(ctx, "resource-1")
	require.NoError(t, err)
	require.True(t, found)
	checkResourceMetaEqual(t, &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-1",
		Executor: "executor-1",
		Deleted:  false,
	}, resc)

	ok, err = acc.UpdateResource(ctx, &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-2",
		Executor: "executor-1",
		Deleted:  false,
	})
	require.NoError(t, err)
	require.True(t, ok)

	resc, found, err = acc.GetResource(ctx, "resource-1")
	require.NoError(t, err)
	require.True(t, found)
	checkResourceMetaEqual(t, &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-2",
		Executor: "executor-1",
		Deleted:  false,
	}, resc)

	ok, err = acc.UpdateResource(ctx, &resModel.ResourceMeta{
		ID:       "resource-2",
		Job:      "job-1",
		Worker:   "worker-2",
		Executor: "executor-1",
		Deleted:  false,
	})
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = acc.DeleteResource(ctx, "resource-1")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestMetadataAccessorGetResourcesForExecutor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	acc := newAccessorWithMockKV()

	for i := 0; i < 1000; i++ {
		executor := "executor-1"
		if i >= 500 {
			executor = "executor-2"
		}

		ok, err := acc.CreateResource(ctx, &resModel.ResourceMeta{
			ID:       fmt.Sprintf("resource-%d", i),
			Job:      "job-1",
			Worker:   fmt.Sprintf("worker-%d", i),
			Executor: resModel.ExecutorID(executor),
			Deleted:  false,
		})
		require.NoError(t, err)
		require.True(t, ok)
	}

	results, err := acc.GetResourcesForExecutor(ctx, "executor-1")
	require.NoError(t, err)
	require.Len(t, results, 500)
}

func checkResourceMetaEqual(t *testing.T, expect, actual *resModel.ResourceMeta) {
	require.Equal(t, expect.ID, actual.ID)
	require.Equal(t, expect.Job, actual.Job)
	require.Equal(t, expect.Worker, actual.Worker)
	require.Equal(t, expect.Executor, actual.Executor)
	require.Equal(t, expect.Deleted, actual.Deleted)
}
