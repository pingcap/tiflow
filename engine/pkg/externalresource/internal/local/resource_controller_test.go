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

package local

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/engine/pkg/client"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/stretchr/testify/require"
)

func TestRemoveFileOnExecutor(t *testing.T) {
	clientGroup := client.NewMockExecutorGroup()
	mockCli := client.NewMockExecutorClient(gomock.NewController(t))
	clientGroup.AddClient("executor-1", mockCli)

	resourceTp := NewFileResourceController(clientGroup)
	gcHandler := resourceTp.GCSingleResource

	resMeta := &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-1",
		Executor: "executor-1",
	}

	mockCli.EXPECT().
		RemoveResource(gomock.Any(), "worker-1", "resource-1").
		Return(nil).Times(1)

	err := gcHandler(context.Background(), resMeta)
	require.NoError(t, err)
}
