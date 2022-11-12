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

package broker

import (
	"context"
	"testing"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/local"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newResourceIdentForTesting(executor, workerID, resourceName string) internal.ResourceIdent {
	return internal.ResourceIdent{
		Name: resModel.EncodeResourceName(resourceName),
		ResourceScope: internal.ResourceScope{
			ProjectInfo: tenant.NewProjectInfo("fakeTenant", "fakeProject"),
			Executor:    resModel.ExecutorID(executor),
			WorkerID:    workerID,
		},
	}
}

func TestStorageHandlePersistAndDiscard(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	executor := resModel.ExecutorID("executor-1")
	ident := newResourceIdentForTesting(string(executor), "worker-1", "test-resource")
	fm := local.NewLocalFileManager(executor, resModel.LocalFileConfig{BaseDir: dir})
	cli := manager.NewMockClient()

	ctx := context.Background()
	desc, err := fm.CreateResource(ctx, ident)
	require.NoError(t, err)

	handle, err := newResourceHandle(
		"job-1",
		executor,
		fm, desc, false, cli)
	require.NoError(t, err)

	cli.On("CreateResource", mock.Anything, &pb.CreateResourceRequest{
		ProjectInfo: &pb.ProjectInfo{
			TenantId:  desc.ResourceIdent().TenantID(),
			ProjectId: desc.ResourceIdent().ProjectID(),
		},
		ResourceId:      "/local/test-resource",
		CreatorExecutor: string(executor),
		JobId:           "job-1",
		CreatorWorkerId: "worker-1",
	}).Return(nil).Once()
	err = handle.Persist(context.Background())
	require.NoError(t, err)
	cli.AssertExpectations(t)
	cli.ExpectedCalls = nil

	err = handle.Persist(context.Background())
	require.NoError(t, err)

	desc, err = fm.GetPersistedResource(ctx, ident)
	require.NoError(t, err)
	require.NotNil(t, desc)

	cli.On("RemoveResource", mock.Anything, &pb.RemoveResourceRequest{
		ResourceKey: &pb.ResourceKey{
			JobId:      "job-1",
			ResourceId: "/local/test-resource",
		},
	}).Return(nil).Once()
	err = handle.Discard(context.Background())
	require.NoError(t, err)
	cli.AssertExpectations(t)
	cli.ExpectedCalls = nil

	_, err = fm.GetPersistedResource(ctx, ident)
	require.Error(t, err)
	require.Regexp(t, ".*ErrResourceDoesNotExist.*", err)

	// Discarding twice should fail.
	err = handle.Discard(context.Background())
	require.Error(t, err)
	require.Regexp(t, ".*ErrInvalidResourceHandle.*", err)

	// Persisting after discarding should fail.
	err = handle.Persist(context.Background())
	require.Error(t, err)
	require.Regexp(t, ".*ErrInvalidResourceHandle.*", err)
}

func TestStorageHandleDiscardTemporaryResource(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fm := local.NewLocalFileManager("", resModel.LocalFileConfig{BaseDir: dir})
	cli := manager.NewMockClient()

	ctx := context.Background()
	desc, err := fm.CreateResource(ctx, newResourceIdentForTesting("", "worker-1", "test-resource"))
	require.NoError(t, err)

	handle, err := newResourceHandle(
		"job-1",
		"executor-1",
		fm, desc, false, cli)
	require.NoError(t, err)

	err = handle.Discard(context.Background())
	require.NoError(t, err)
	cli.AssertNotCalled(t, "RemoveResource")
	cli.ExpectedCalls = nil

	_, err = fm.GetPersistedResource(ctx, newResourceIdentForTesting("", "worker-1", "test-resource"))
	require.Error(t, err)
	require.Regexp(t, ".*ErrResourceDoesNotExist.*", err)

	err = handle.Discard(context.Background())
	require.Error(t, err)
	require.Regexp(t, ".*ErrInvalidResourceHandle.*", err)
}
