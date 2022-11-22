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
	pbMock "github.com/pingcap/tiflow/engine/enginepb/mock"
	"github.com/stretchr/testify/require"
)

func TestBrokerServiceClientNormal(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := pbMock.NewMockBrokerServiceClient(ctrl)
	serviceCli := NewBrokerServiceClient(client)

	client.EXPECT().RemoveResource(gomock.Any(), gomock.Eq(&enginepb.RemoveLocalResourceRequest{
		ResourceId: "/local/resource-1",
		WorkerId:   "worker-1",
	})).Return(&enginepb.RemoveLocalResourceResponse{}, nil).Times(1)

	err := serviceCli.RemoveResource(context.Background(), "worker-1", "/local/resource-1")
	require.NoError(t, err)
}
