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

package master

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/client"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/label"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type mockEpochGen struct {
	pkgOrm.Client

	nextEpoch int64
}

func (g *mockEpochGen) GenEpoch(ctx context.Context) (int64, error) {
	return g.nextEpoch, nil
}

type workerCreatorTestHelper struct {
	ExecutorGroup      *client.MockExecutorGroup
	ServerMasterClient *client.MockServerMasterClient
	FrameMetaClient    *mockEpochGen

	Creator *WorkerCreator
}

func newWorkerCreatorTestHelper(
	t *testing.T,
	jobID frameModel.MasterID,
	hooks *WorkerCreationHooks,
	inheritedSelectors []*label.Selector,
) *workerCreatorTestHelper {
	ret := &workerCreatorTestHelper{
		ExecutorGroup:      client.NewMockExecutorGroup(),
		ServerMasterClient: client.NewMockServerMasterClient(gomock.NewController(t)),
		FrameMetaClient:    &mockEpochGen{nextEpoch: 1},
	}

	ret.Creator = NewWorkerCreatorBuilder().
		WithMasterID(jobID).
		WithLogger(log.L()).
		WithInheritedSelectors(inheritedSelectors...).
		WithExecutorGroup(ret.ExecutorGroup).
		WithFrameMetaClient(ret.FrameMetaClient).
		WithServerMasterClient(ret.ServerMasterClient).
		WithHooks(hooks).Build()
	return ret
}

func TestCreateWorkerNormal(t *testing.T) {
	t.Parallel()

	inheritedSelectors := []*label.Selector{
		{
			Key:    "label1",
			Target: "value1",
			Op:     label.OpEq,
		},
		{
			Key:    "label2",
			Target: "value2",
			Op:     label.OpNeq,
		},
	}
	additionalSelectors := []*label.Selector{
		{
			Key:    "label3",
			Target: "value3",
			Op:     label.OpEq,
		},
		{
			Key:    "label4",
			Target: "value4",
			Op:     label.OpNeq,
		},
	}
	var callBackInvoked atomic.Bool
	helper := newWorkerCreatorTestHelper(t, "job-1",
		&WorkerCreationHooks{
			BeforeStartingWorker: func(
				workerID frameModel.WorkerID,
				executorID model.ExecutorID,
				epoch frameModel.Epoch,
			) {
				require.Equal(t, "worker-1", workerID)
				require.Equal(t, "executor-1", executorID)
				require.Equal(t, int64(1), epoch)
				require.False(t, callBackInvoked.Swap(true))
			},
		}, inheritedSelectors)

	// expectedPBSelectors should combine the inherited selectors and
	// the additional selectors.
	expectedPBSelectors := []*pb.Selector{
		{
			Label:  "label1",
			Target: "value1",
			Op:     pb.Selector_Eq,
		},
		{
			Label:  "label2",
			Target: "value2",
			Op:     pb.Selector_Neq,
		},
		{
			Label:  "label3",
			Target: "value3",
			Op:     pb.Selector_Eq,
		},
		{
			Label:  "label4",
			Target: "value4",
			Op:     pb.Selector_Neq,
		},
	}
	helper.ServerMasterClient.EXPECT().ScheduleTask(gomock.Any(),
		&pb.ScheduleTaskRequest{
			TaskId: "worker-1",
			Resources: resModel.ToResourceRequirement(
				"job-1", "/local/resource-1", "/local/resource-2"),
			Selectors: expectedPBSelectors,
		}).Return(
		&pb.ScheduleTaskResponse{
			ExecutorId:   "executor-1",
			ExecutorAddr: "1.1.1.1:1234",
		}, nil).Times(1)
	executorCli := client.NewMockExecutorClient(gomock.NewController(t))
	helper.ExecutorGroup.AddClient("executor-1", executorCli)
	projectInfo := tenant.NewProjectInfo("tenant-1", "project-1")
	executorCli.EXPECT().DispatchTask(gomock.Any(),
		&client.DispatchTaskArgs{
			ProjectInfo:  projectInfo,
			WorkerID:     "worker-1",
			MasterID:     "job-1",
			WorkerType:   int64(1),
			WorkerConfig: []byte("sample-config"),
			WorkerEpoch:  int64(1),
		}, gomock.Any()).
		Return(nil).
		Times(1)

	err := helper.Creator.CreateWorker(
		context.Background(),
		projectInfo,
		frameModel.WorkerType(1),
		"worker-1",
		[]byte("sample-config"),
		CreateWorkerWithSelectors(additionalSelectors...),
		CreateWorkerWithResourceRequirements("/local/resource-1", "/local/resource-2"))
	require.NoError(t, err)
}
