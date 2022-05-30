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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/storagecfg"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
)

func TestStorageHandlePersistAndDiscard(t *testing.T) {
	dir := t.TempDir()
	fm := NewLocalFileManager(storagecfg.LocalFileConfig{BaseDir: dir})
	mockManagerClient := &manager.MockClient{}
	cli := rpcutil.NewFailoverRPCClientsForTest[pb.ResourceManagerClient](mockManagerClient)

	desc, err := fm.CreateResource("worker-1", "test-resource")
	require.NoError(t, err)

	handle, err := newLocalResourceHandle(
		"/local/test-resource",
		"job-1",
		"executor-1",
		fm, desc, cli)
	require.NoError(t, err)

	mockManagerClient.On("CreateResource", mock.Anything, &pb.CreateResourceRequest{
		ResourceId:      "/local/test-resource",
		CreatorExecutor: "executor-1",
		JobId:           "job-1",
		CreatorWorkerId: "worker-1",
	}, []grpc.CallOption(nil)).Return(&pb.CreateResourceResponse{}, nil).Once()
	err = handle.Persist(context.Background())
	require.NoError(t, err)
	mockManagerClient.AssertExpectations(t)
	mockManagerClient.ExpectedCalls = nil

	desc, err = fm.GetPersistedResource("worker-1", "test-resource")
	require.NoError(t, err)
	require.NotNil(t, desc)

	mockManagerClient.On("RemoveResource", mock.Anything, &pb.RemoveResourceRequest{
		ResourceId: "/local/test-resource",
	}, []grpc.CallOption(nil)).Return(&pb.RemoveResourceResponse{}, nil).Once()
	err = handle.Discard(context.Background())
	require.NoError(t, err)
	mockManagerClient.AssertExpectations(t)
	mockManagerClient.ExpectedCalls = nil

	_, err = fm.GetPersistedResource("worker-1", "test-resource")
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
	dir := t.TempDir()
	fm := NewLocalFileManager(storagecfg.LocalFileConfig{BaseDir: dir})
	mockManagerClient := &manager.MockClient{}
	cli := rpcutil.NewFailoverRPCClientsForTest[pb.ResourceManagerClient](mockManagerClient)

	desc, err := fm.CreateResource("worker-1", "test-resource")
	require.NoError(t, err)

	handle, err := newLocalResourceHandle(
		"/local/test-resource",
		"job-1",
		"executor-1",
		fm, desc, cli)
	require.NoError(t, err)

	err = handle.Discard(context.Background())
	require.NoError(t, err)
	mockManagerClient.AssertNotCalled(t, "RemoveResource")
	mockManagerClient.ExpectedCalls = nil

	_, err = fm.GetPersistedResource("worker-1", "test-resource")
	require.Error(t, err)
	require.Regexp(t, ".*ErrResourceDoesNotExist.*", err)

	err = handle.Discard(context.Background())
	require.Error(t, err)
	require.Regexp(t, ".*ErrInvalidResourceHandle.*", err)
}
