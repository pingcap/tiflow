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

package client

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/enginepb/mock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestCreateResource(t *testing.T) {
	t.Parallel()

	mockCli := mock.NewMockResourceManagerClient(gomock.NewController(t))
	resourceCli := NewResourceManagerClient(mockCli)

	req := &enginepb.CreateResourceRequest{
		ResourceId:      "/local/resource-1",
		CreatorExecutor: "executor-1",
		JobId:           "job-1",
		CreatorWorkerId: "worker-1",
	}
	mockCli.EXPECT().CreateResource(gomock.Any(), gomock.Eq(req)).
		Return(&enginepb.CreateResourceResponse{}, nil).Times(1)
	require.NoError(t, resourceCli.CreateResource(context.Background(), req))

	mockCli.EXPECT().CreateResource(gomock.Any(), gomock.Eq(req)).
		Return(nil, errors.ErrInvalidArgument.GenWithStackByArgs("resource-id")).Times(1)
	require.True(t, errors.Is(resourceCli.CreateResource(context.Background(), req), errors.ErrInvalidArgument))

	mockCli.EXPECT().CreateResource(gomock.Any(), gomock.Eq(req)).
		Return(nil, errors.ErrResourceAlreadyExists.GenWithStackByArgs(req.ResourceId)).Times(1)
	mockCli.EXPECT().QueryResource(gomock.Any(), gomock.Eq(&enginepb.QueryResourceRequest{
		ResourceKey: &enginepb.ResourceKey{
			JobId:      "job-1",
			ResourceId: "/local/resource-1",
		},
	})).Return(&enginepb.QueryResourceResponse{
		CreatorExecutor: "executor-1",
		JobId:           "job-1",
		CreatorWorkerId: "worker-1",
	}, nil).Times(1)
	require.NoError(t, resourceCli.CreateResource(context.Background(), req))

	mockCli.EXPECT().CreateResource(gomock.Any(), gomock.Eq(req)).
		Return(nil, errors.ErrResourceAlreadyExists.GenWithStackByArgs(req.ResourceId)).Times(1)
	mockCli.EXPECT().QueryResource(gomock.Any(), gomock.Eq(&enginepb.QueryResourceRequest{
		ResourceKey: &enginepb.ResourceKey{
			JobId:      "job-1",
			ResourceId: "/local/resource-1",
		},
	})).Return(&enginepb.QueryResourceResponse{
		CreatorExecutor: "executor-1",
		JobId:           "job-1",
		CreatorWorkerId: "worker-2",
	}, nil).Times(1)
	require.ErrorContains(t, resourceCli.CreateResource(context.Background(), req), "already exists")
}

func TestRemoveResource(t *testing.T) {
	t.Parallel()

	mockCli := mock.NewMockResourceManagerClient(gomock.NewController(t))
	resourceCli := NewResourceManagerClient(mockCli)

	req := &enginepb.RemoveResourceRequest{ResourceKey: &enginepb.ResourceKey{
		JobId:      "job-1",
		ResourceId: "/local/resource-1",
	}}
	mockCli.EXPECT().RemoveResource(gomock.Any(), gomock.Eq(req)).
		Return(&enginepb.RemoveResourceResponse{}, nil).Times(1)
	require.NoError(t, resourceCli.RemoveResource(context.Background(), req))

	mockCli.EXPECT().RemoveResource(gomock.Any(), gomock.Eq(req)).
		Return(nil, errors.ErrResourceDoesNotExist.GenWithStackByArgs("/local/resource-1")).Times(1)
	require.NoError(t, resourceCli.RemoveResource(context.Background(), req))
}
