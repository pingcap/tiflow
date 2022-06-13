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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
)

func TestLocalFileTriggeredByJobRemoval(t *testing.T) {
	ctx := context.Background()

	cluster := newMockGCCluster()
	cluster.Start(t)

	baseDir := t.TempDir()
	cluster.AddBroker("executor-1", baseDir)
	brk := cluster.MustGetBroker(t, "executor-1")

	cluster.jobInfo.SetJobStatus("job-1", frameModel.MasterStatusInit)

	handle, err := brk.OpenStorage(
		context.Background(),
		"worker-1",
		"job-1",
		"/local/resource-1")
	require.NoError(t, err)

	_, err = handle.BrExternalStorage().Create(context.Background(), "1.txt")
	require.NoError(t, err)
	err = handle.Persist(ctx)
	require.NoError(t, err)

	// Assert meta exists for `/local/resource-1`
	resMeta, err := cluster.meta.GetResourceByID(ctx, "/local/resource-1")
	require.NoError(t, err)
	require.Equal(t, model.ExecutorID("executor-1"), resMeta.Executor)
	broker.AssertLocalFileExists(t, baseDir, "worker-1", "resource-1", "1.txt")

	// Triggers GC by removing the job
	cluster.jobInfo.RemoveJob("job-1")
	require.Eventually(t, func() bool {
		_, err := cluster.meta.GetResourceByID(ctx, "/local/resource-1")
		return err != nil && pkgOrm.IsNotFoundError(err)
	}, 1*time.Second, 5*time.Millisecond)
	broker.AssertNoLocalFileExists(t, baseDir, "worker-1", "resource-1", "1.txt")

	cluster.Stop()
}

func TestLocalFileRecordRemovedTriggeredByExecutorOffline(t *testing.T) {
	ctx := context.Background()

	cluster := newMockGCCluster()
	cluster.Start(t)

	baseDir := t.TempDir()
	cluster.AddBroker("executor-1", baseDir)
	brk := cluster.MustGetBroker(t, "executor-1")

	cluster.jobInfo.SetJobStatus("job-1", frameModel.MasterStatusInit)

	handle, err := brk.OpenStorage(
		context.Background(),
		"worker-1",
		"job-1",
		"/local/resource-1")
	require.NoError(t, err)
	_, err = handle.BrExternalStorage().Create(context.Background(), "1.txt")
	require.NoError(t, err)
	err = handle.Persist(ctx)
	require.NoError(t, err)

	cluster.executorInfo.RemoveExecutor("executor-1")
	require.Eventually(t, func() bool {
		_, err := cluster.meta.GetResourceByID(ctx, "/local/resource-1")
		return err != nil && pkgOrm.IsNotFoundError(err)
	}, 1*time.Second, 5*time.Millisecond)
}

func TestCleanUpStaleResourcesOnStartUp(t *testing.T) {
	ctx := context.Background()

	cluster := newMockGCCluster()

	baseDir := t.TempDir()

	// We do not start the cluster until we have initialized
	// the mock cluster to the desired initial state.
	cluster.AddBroker("executor-1", baseDir)
	brk := cluster.MustGetBroker(t, "executor-1")

	// Putting "non-existent-job" here is acceptable because resource
	// creation does not check job existence.
	handle, err := brk.OpenStorage(
		context.Background(),
		"worker-1",
		"non-existent-job",
		"/local/resource-1")
	require.NoError(t, err)
	_, err = handle.BrExternalStorage().Create(context.Background(), "1.txt")
	require.NoError(t, err)
	err = handle.Persist(ctx)
	require.NoError(t, err)
	broker.AssertLocalFileExists(t, baseDir, "worker-1", "resource-1", "1.txt")

	// Asserts that the job exists.
	_, err = cluster.meta.GetResourceByID(ctx, "/local/resource-1")
	require.NoError(t, err)

	cluster.jobInfo.SetJobStatus("job-1", frameModel.MasterStatusInit)
	err = cluster.meta.CreateResource(ctx, &resModel.ResourceMeta{
		ID:        "/local/resource-2",
		Job:       "job-1",
		Worker:    "worker-1",
		Executor:  "non-existent-executor",
		GCPending: false,
	})
	require.NoError(t, err)

	cluster.Start(t)

	require.Eventually(t, func() bool {
		_, err := cluster.meta.GetResourceByID(ctx, "/local/resource-1")
		return err != nil && pkgOrm.IsNotFoundError(err)
	}, 1*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		_, err := cluster.meta.GetResourceByID(ctx, "/local/resource-2")
		return err != nil && pkgOrm.IsNotFoundError(err)
	}, 1*time.Second, 5*time.Millisecond)
	broker.AssertNoLocalFileExists(t, baseDir, "worker-1", "resource-1", "1.txt")

	cluster.Stop()
}
