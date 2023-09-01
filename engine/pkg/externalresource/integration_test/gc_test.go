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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/local"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLocalFileTriggeredByJobRemoval(t *testing.T) {
	fakeProjectInfo := tenant.NewProjectInfo("fakeTenant", "fakeProject")
	ctx := context.Background()

	cluster, mockFeatureChecker := newMockGCCluster(t)
	mockFeatureChecker.EXPECT().Available(gomock.Any()).Return(true).AnyTimes()
	cluster.Start(t)

	baseDir := t.TempDir()
	cluster.AddBroker("executor-1", baseDir)
	brk := cluster.MustGetBroker(t, "executor-1")

	cluster.jobInfo.SetJobStatus("job-1", frameModel.MasterStateInit)

	resID := "/local/resource-1"
	_, resName, err := resModel.ParseResourceID(resID)
	require.NoError(t, err)
	handle, err := brk.OpenStorage(
		context.Background(),
		fakeProjectInfo,
		"worker-1",
		"job-1",
		resID)
	require.NoError(t, err)

	_, err = handle.BrExternalStorage().Create(context.Background(), "1.txt", nil)
	require.NoError(t, err)
	err = handle.Persist(ctx)
	require.NoError(t, err)

	// Assert meta exists for `/local/resource-1`
	resMeta, err := cluster.meta.GetResourceByID(ctx, pkgOrm.ResourceKey{JobID: "job-1", ID: resID})
	require.NoError(t, err)
	require.Equal(t, model.ExecutorID("executor-1"), resMeta.Executor)
	local.AssertLocalFileExists(t, baseDir, "worker-1", resName, "1.txt")

	// Triggers GC by removing the job
	cluster.jobInfo.RemoveJob("job-1")
	require.Eventually(t, func() bool {
		_, err := cluster.meta.GetResourceByID(ctx, pkgOrm.ResourceKey{JobID: "job-1", ID: resID})
		log.Warn("GetResourceByID", zap.Error(err))
		return err != nil && pkgOrm.IsNotFoundError(err)
	}, 1*time.Second, 5*time.Millisecond)
	local.AssertNoLocalFileExists(t, baseDir, "worker-1", resName, "1.txt")

	cluster.Stop()
}

func TestLocalFileRecordRemovedTriggeredByExecutorOffline(t *testing.T) {
	fakeProjectInfo := tenant.NewProjectInfo("fakeTenant", "fakeProject")
	ctx := context.Background()

	cluster, mockFeatureChecker := newMockGCCluster(t)
	mockFeatureChecker.EXPECT().Available(gomock.Any()).Return(true).AnyTimes()
	cluster.Start(t)

	baseDir := t.TempDir()
	cluster.AddBroker("executor-1", baseDir)
	brk := cluster.MustGetBroker(t, "executor-1")

	cluster.jobInfo.SetJobStatus("job-1", frameModel.MasterStateInit)

	handle, err := brk.OpenStorage(
		context.Background(),
		fakeProjectInfo,
		"worker-1",
		"job-1",
		"/local/resource-1")
	require.NoError(t, err)
	_, err = handle.BrExternalStorage().Create(context.Background(), "1.txt", nil)
	require.NoError(t, err)
	err = handle.Persist(ctx)
	require.NoError(t, err)

	cluster.executorInfo.RemoveExecutor("executor-1")
	require.Eventually(t, func() bool {
		_, err := cluster.meta.GetResourceByID(ctx, pkgOrm.ResourceKey{JobID: "job-1", ID: "/local/resource-1"})
		return err != nil && pkgOrm.IsNotFoundError(err)
	}, 1*time.Second, 5*time.Millisecond)
}

func TestCleanUpStaleResourcesOnStartUp(t *testing.T) {
	ctx := context.Background()

	cluster, mockFeatureChecker := newMockGCCluster(t)
	mockFeatureChecker.EXPECT().Available(gomock.Any()).Return(true).AnyTimes()

	baseDir := t.TempDir()

	// We do not start the cluster until we have initialized
	// the mock cluster to the desired initial state.
	cluster.AddBroker("executor-1", baseDir)
	_ = cluster.MustGetBroker(t, "executor-1")

	cluster.jobInfo.SetJobStatus("job-1", frameModel.MasterStateInit)
	err := cluster.meta.CreateResource(ctx, &resModel.ResourceMeta{
		ID:        "/local/resource-2",
		Job:       "job-1",
		Worker:    "worker-1",
		Executor:  "non-existent-executor",
		GCPending: false,
	})
	require.NoError(t, err)

	cluster.Start(t)

	require.Eventually(t, func() bool {
		_, err := cluster.meta.GetResourceByID(ctx, pkgOrm.ResourceKey{JobID: "job-1", ID: "/local/resource-1"})
		return err != nil && pkgOrm.IsNotFoundError(err)
	}, 1*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		_, err := cluster.meta.GetResourceByID(ctx, pkgOrm.ResourceKey{JobID: "job-1", ID: "/local/resource-2"})
		return err != nil && pkgOrm.IsNotFoundError(err)
	}, 1*time.Second, 5*time.Millisecond)
	local.AssertNoLocalFileExists(t, baseDir, "worker-1", "resource-1", "1.txt")

	cluster.Stop()
}
