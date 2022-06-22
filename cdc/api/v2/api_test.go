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

package v2

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/stretchr/testify/mock"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

var (
	changeFeedID         = model.DefaultChangeFeedID("test-changeFeed")
	captureID            = "test-capture"
	nonExistChangefeedID = model.DefaultChangeFeedID("non-exist-changefeed")
)

type mockStatusProvider struct {
	mock.Mock
}

func newStatusProvider() *mockStatusProvider {
	statusProvider := &mockStatusProvider{}
	statusProvider.On("GetChangeFeedStatus", mock.Anything, changeFeedID).
		Return(&model.ChangeFeedStatus{CheckpointTs: 1}, nil)

	statusProvider.On("GetChangeFeedStatus", mock.Anything, nonExistChangefeedID).
		Return(new(model.ChangeFeedStatus),
			cerror.ErrChangeFeedNotExists.GenWithStackByArgs(nonExistChangefeedID))

	statusProvider.On("GetAllTaskStatuses", mock.Anything).
		Return(map[model.CaptureID]*model.TaskStatus{captureID: {}}, nil)

	statusProvider.On("GetTaskPositions", mock.Anything).
		Return(map[model.CaptureID]*model.TaskPosition{
			captureID: {Error: &model.RunningError{Message: "test"}},
		}, nil)

	statusProvider.On("GetAllChangeFeedStatuses", mock.Anything).
		Return(map[model.ChangeFeedID]*model.ChangeFeedStatus{
			model.DefaultChangeFeedID(changeFeedID.ID + "1"): {CheckpointTs: 1},
			model.DefaultChangeFeedID(changeFeedID.ID + "2"): {CheckpointTs: 2},
		}, nil)

	statusProvider.On("GetAllChangeFeedInfo", mock.Anything).
		Return(map[model.ChangeFeedID]*model.ChangeFeedInfo{
			model.DefaultChangeFeedID(changeFeedID.ID + "1"): {State: model.StateNormal},
			model.DefaultChangeFeedID(changeFeedID.ID + "2"): {State: model.StateStopped},
		}, nil)

	statusProvider.On("GetAllTaskStatuses", mock.Anything).
		Return(map[model.CaptureID]*model.TaskStatus{captureID: {}}, nil)

	statusProvider.On("GetChangeFeedInfo", mock.Anything).
		Return(&model.ChangeFeedInfo{State: model.StateNormal}, nil)

	statusProvider.On("GetProcessors", mock.Anything).
		Return([]*model.ProcInfoSnap{{CfID: changeFeedID, CaptureID: captureID}}, nil)

	statusProvider.On("GetCaptures", mock.Anything).
		Return([]*model.CaptureInfo{{ID: captureID}}, nil)

	return statusProvider
}

// MockPDClient mocks pd.Client to facilitate unit testing.
type MockPDClient struct {
	pd.Client
}

func (m *MockPDClient) GetTS(_ context.Context) (int64, int64, error) {
	return oracle.GetPhysical(time.Now()), 0, nil
}

// MockPDClient mocks pd.Client to facilitate unit testing.
type MockEtcdClient struct {
	etcd.CDCEtcdClient
}

func (*MockEtcdClient) CreateChangefeedInfo(_ ...interface{}) error {
	return nil
}

type testCase struct {
	url    string
	method string
}

func newRouter(apiV2 OpenAPIV2) *gin.Engine {
	router := gin.New()
	RegisterOpenAPIV2Routes(router, apiV2)
	return router
}
