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
	"os"
	"path/filepath"
	"testing"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/local"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newBroker(t *testing.T) (*DefaultBroker, *manager.MockClient, string) {
	tmpDir := t.TempDir()
	cli := manager.NewMockClient()
	broker, err := NewBroker(&resModel.Config{Local: resModel.LocalFileConfig{BaseDir: tmpDir}},
		"executor-1",
		cli)
	require.NoError(t, err)
	return broker, cli, tmpDir
}

func TestBrokerOpenNewStorage(t *testing.T) {
	t.Parallel()
	fakeProjectInfo := tenant.NewProjectInfo("fakeTenant", "fakeProject")
	brk, cli, dir := newBroker(t)
	defer brk.Close()

	cli.On("QueryResource", mock.Anything,
		&pb.QueryResourceRequest{ResourceKey: &pb.ResourceKey{JobId: "job-1", ResourceId: "/local/test-1"}}, mock.Anything).
		Return((*pb.QueryResourceResponse)(nil), status.Error(codes.NotFound, "resource manager error"))
	hdl, err := brk.OpenStorage(context.Background(), fakeProjectInfo, "worker-1", "job-1", "/local/test-1")
	require.NoError(t, err)
	require.Equal(t, "/local/test-1", hdl.ID())

	cli.AssertExpectations(t)
	cli.ExpectedCalls = nil

	f, err := hdl.BrExternalStorage().Create(context.Background(), "1.txt")
	require.NoError(t, err)

	err = f.Close(context.Background())
	require.NoError(t, err)

	cli.On("CreateResource", mock.Anything, &pb.CreateResourceRequest{
		ProjectInfo:     &pb.ProjectInfo{TenantId: fakeProjectInfo.TenantID(), ProjectId: fakeProjectInfo.ProjectID()},
		ResourceId:      "/local/test-1",
		CreatorExecutor: "executor-1",
		JobId:           "job-1",
		CreatorWorkerId: "worker-1",
	}, mock.Anything).Return(nil)

	err = hdl.Persist(context.Background())
	require.NoError(t, err)

	cli.AssertExpectations(t)

	local.AssertLocalFileExists(t, dir, "worker-1", "test-1", "1.txt")
}

func TestBrokerOpenExistingStorage(t *testing.T) {
	t.Parallel()
	fakeProjectInfo := tenant.NewProjectInfo("fakeTenant", "fakeProject")
	brk, cli, dir := newBroker(t)
	defer brk.Close()

	cli.On("QueryResource", mock.Anything,
		&pb.QueryResourceRequest{ResourceKey: &pb.ResourceKey{JobId: "job-1", ResourceId: "/local/test-2"}}, mock.Anything).
		Return((*pb.QueryResourceResponse)(nil), status.Error(codes.NotFound, "resource manager error")).Once()
	cli.On("CreateResource", mock.Anything, &pb.CreateResourceRequest{
		ProjectInfo:     &pb.ProjectInfo{TenantId: fakeProjectInfo.TenantID(), ProjectId: fakeProjectInfo.ProjectID()},
		ResourceId:      "/local/test-2",
		CreatorExecutor: "executor-1",
		JobId:           "job-1",
		CreatorWorkerId: "worker-2",
	}, mock.Anything).Return(nil)

	hdl, err := brk.OpenStorage(
		context.Background(),
		fakeProjectInfo,
		"worker-2",
		"job-1",
		"/local/test-2")
	require.NoError(t, err)

	err = hdl.Persist(context.Background())
	require.NoError(t, err)

	cli.On("QueryResource", mock.Anything,
		&pb.QueryResourceRequest{ResourceKey: &pb.ResourceKey{JobId: "job-1", ResourceId: "/local/test-2"}}, mock.Anything).
		Return(&pb.QueryResourceResponse{
			CreatorExecutor: "executor-1",
			JobId:           "job-1",
			CreatorWorkerId: "worker-2",
		}, nil)

	hdl, err = brk.OpenStorage(context.Background(), fakeProjectInfo, "worker-1", "job-1", "/local/test-2")
	require.NoError(t, err)
	require.Equal(t, "/local/test-2", hdl.ID())

	cli.AssertExpectations(t)

	f, err := hdl.BrExternalStorage().Create(context.Background(), "1.txt")
	require.NoError(t, err)

	err = f.Close(context.Background())
	require.NoError(t, err)

	local.AssertLocalFileExists(t, dir, "worker-2", "test-2", "1.txt")
}

func TestBrokerRemoveResource(t *testing.T) {
	t.Parallel()
	brk, _, dir := newBroker(t)
	defer brk.Close()

	resPath := filepath.Join(dir, "worker-1", local.ResourceNameToFilePathName("resource-1"))
	err := os.MkdirAll(resPath, 0o700)
	require.NoError(t, err)

	// Wrong creatorID would yield NotFound
	_, err = brk.RemoveResource(context.Background(), &pb.RemoveLocalResourceRequest{
		ResourceId: "/local/resource-1",
		CreatorId:  "worker-2", // wrong creatorID
	})
	require.Error(t, err)
	code := status.Convert(err).Code()
	require.Equal(t, codes.NotFound, code)

	// Wrong file type would yield InvalidArgument
	_, err = brk.RemoveResource(context.Background(), &pb.RemoveLocalResourceRequest{
		ResourceId: "/s3/resource-1",
		CreatorId:  "worker-2", // wrong creatorID
	})
	require.Error(t, err)
	code = status.Convert(err).Code()
	require.Equal(t, codes.InvalidArgument, code)

	_, err = brk.RemoveResource(context.Background(), &pb.RemoveLocalResourceRequest{
		ResourceId: "/wrongType/resource-1",
		CreatorId:  "worker-2", // wrong creatorID
	})
	require.Error(t, err)
	code = status.Convert(err).Code()
	require.Equal(t, codes.InvalidArgument, code)

	// The response is ignored because it is an empty PB message.
	_, err = brk.RemoveResource(context.Background(), &pb.RemoveLocalResourceRequest{
		ResourceId: "/local/resource-1",
		CreatorId:  "worker-1",
	})
	require.NoError(t, err)
	require.NoDirExists(t, resPath)

	// Repeated calls should fail with NotFound
	_, err = brk.RemoveResource(context.Background(), &pb.RemoveLocalResourceRequest{
		ResourceId: "/local/resource-1",
		CreatorId:  "worker-1",
	})
	require.Error(t, err)
	code = status.Convert(err).Code()
	require.Equal(t, codes.NotFound, code)

	// Unexpected resource type
	_, err = brk.RemoveResource(context.Background(), &pb.RemoveLocalResourceRequest{
		ResourceId: "/s3/resource-1",
		CreatorId:  "worker-1",
	})
	require.Error(t, err)
	code = status.Convert(err).Code()
	require.Equal(t, codes.InvalidArgument, code)

	// Unparsable ResourceID
	_, err = brk.RemoveResource(context.Background(), &pb.RemoveLocalResourceRequest{
		ResourceId: "#@$!@#!$",
		CreatorId:  "worker-1",
	})
	require.Error(t, err)
	code = status.Convert(err).Code()
	require.Equal(t, codes.InvalidArgument, code)

	// Empty CreatorID
	_, err = brk.RemoveResource(context.Background(), &pb.RemoveLocalResourceRequest{
		ResourceId: "/local/resource-1",
		CreatorId:  "",
	})
	require.Error(t, err)
	code = status.Convert(err).Code()
	require.Equal(t, codes.InvalidArgument, code)

	// Empty ResourceID
	_, err = brk.RemoveResource(context.Background(), &pb.RemoveLocalResourceRequest{
		ResourceId: "",
		CreatorId:  "worker-1",
	})
	require.Error(t, err)
	code = status.Convert(err).Code()
	require.Equal(t, codes.InvalidArgument, code)
}
