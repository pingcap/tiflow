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

package resourcetypes

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/client"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
)

func TestRemoveFileOnExecutor(t *testing.T) {
	clientManager := client.NewClientManager()
	mockCli := &client.MockExecutorClient{}
	err := clientManager.AddExecutorClient("executor-1", mockCli)
	require.NoError(t, err)

	resourceTp := NewLocalFileResourceType(clientManager)
	gcHandler := resourceTp.GCHandler()

	resMeta := &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-1",
		Executor: "executor-1",
	}

	mockCli.On("RemoveLocalResource", mock.Anything, &pb.RemoveLocalResourceRequest{
		ResourceId: "resource-1",
		CreatorId:  "worker-1",
	}).Return(nil)
	err = gcHandler(context.Background(), resMeta)
	require.NoError(t, err)
}
