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
	"path/filepath"
	"testing"

	"github.com/gogo/status"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/tiflow/engine/pb"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/storagecfg"
)

func newBroker(t *testing.T) (*Impl, *rpcutil.FailoverRPCClients[pb.ResourceManagerClient], string) {
	tmpDir := t.TempDir()
	client := manager.NewWrappedMockClient()
	broker := NewBroker(&storagecfg.Config{Local: &storagecfg.LocalFileConfig{BaseDir: tmpDir}},
		"executor-1",
		client)
	return broker, client, tmpDir
}

func TestBrokerOpenNewStorage(t *testing.T) {
	brk, client, dir := newBroker(t)

	st, err := status.New(codes.Internal, "resource manager error").WithDetails(&pb.ResourceError{
		ErrorCode: pb.ResourceErrorCode_ResourceNotFound,
	})
	require.NoError(t, err)

	innerClient := client.GetLeaderClient().(*manager.MockClient)
	innerClient.On("QueryResource", mock.Anything, &pb.QueryResourceRequest{ResourceId: "/local/test-1"}, mock.Anything).
		Return((*pb.QueryResourceResponse)(nil), st.Err())
	hdl, err := brk.OpenStorage(context.Background(), "worker-1", "job-1", "/local/test-1")
	require.NoError(t, err)
	require.Equal(t, "/local/test-1", hdl.ID())

	innerClient.AssertExpectations(t)
	innerClient.ExpectedCalls = nil

	f, err := hdl.BrExternalStorage().Create(context.Background(), "1.txt")
	require.NoError(t, err)

	err = f.Close(context.Background())
	require.NoError(t, err)

	innerClient.On("CreateResource", mock.Anything, &pb.CreateResourceRequest{
		ResourceId:      "/local/test-1",
		CreatorExecutor: "executor-1",
		JobId:           "job-1",
		CreatorWorkerId: "worker-1",
	}, mock.Anything).Return(&pb.CreateResourceResponse{}, nil)

	err = hdl.Persist(context.Background())
	require.NoError(t, err)

	innerClient.AssertExpectations(t)

	fileName := filepath.Join(dir, "worker-1", "test-1", "1.txt")
	require.FileExists(t, fileName)
}

func TestBrokerOpenExistingStorage(t *testing.T) {
	brk, client, dir := newBroker(t)

	innerClient := client.GetLeaderClient().(*manager.MockClient)
	innerClient.On("QueryResource", mock.Anything, &pb.QueryResourceRequest{ResourceId: "/local/test-2"}, mock.Anything).
		Return(&pb.QueryResourceResponse{
			CreatorExecutor: "executor-1",
			JobId:           "job-1",
			CreatorWorkerId: "worker-2",
		}, nil)
	hdl, err := brk.OpenStorage(context.Background(), "worker-1", "job-1", "/local/test-2")
	require.NoError(t, err)
	require.Equal(t, "/local/test-2", hdl.ID())

	innerClient.AssertExpectations(t)

	f, err := hdl.BrExternalStorage().Create(context.Background(), "1.txt")
	require.NoError(t, err)

	err = f.Close(context.Background())
	require.NoError(t, err)

	fileName := filepath.Join(dir, "worker-2", "test-2", "1.txt")
	require.FileExists(t, fileName)
}
