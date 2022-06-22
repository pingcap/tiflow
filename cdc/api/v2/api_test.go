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
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

var (
	changeFeedID         = model.DefaultChangeFeedID("test-changeFeed")
	captureID            = "test-capture"
	nonExistChangefeedID = model.DefaultChangeFeedID("non-exist-changefeed")
)

//type mockStatusProvider struct {
//	mock.Mock
//}
//
//func newStatusProvider() *mockStatusProvider {
//	statusProvider := &mockStatusProvider{}
//	statusProvider.On("GetChangeFeedStatus", mock.Anything, changeFeedID).
//		Return(&model.ChangeFeedStatus{CheckpointTs: 1}, nil)
//
//	statusProvider.On("GetChangeFeedStatus", mock.Anything, nonExistChangefeedID).
//		Return(new(model.ChangeFeedStatus),
//			cerror.ErrChangeFeedNotExists.GenWithStackByArgs(nonExistChangefeedID))
//
//	statusProvider.On("GetAllTaskStatuses", mock.Anything).
//		Return(map[model.CaptureID]*model.TaskStatus{captureID: {}}, nil)
//
//	statusProvider.On("GetTaskPositions", mock.Anything).
//		Return(map[model.CaptureID]*model.TaskPosition{
//			captureID: {Error: &model.RunningError{Message: "test"}},
//		}, nil)
//
//	statusProvider.On("GetAllChangeFeedStatuses", mock.Anything).
//		Return(map[model.ChangeFeedID]*model.ChangeFeedStatus{
//			model.DefaultChangeFeedID(changeFeedID.ID + "1"): {CheckpointTs: 1},
//			model.DefaultChangeFeedID(changeFeedID.ID + "2"): {CheckpointTs: 2},
//		}, nil)
//
//	statusProvider.On("GetAllChangeFeedInfo", mock.Anything).
//		Return(map[model.ChangeFeedID]*model.ChangeFeedInfo{
//			model.DefaultChangeFeedID(changeFeedID.ID + "1"): {State: model.StateNormal},
//			model.DefaultChangeFeedID(changeFeedID.ID + "2"): {State: model.StateStopped},
//		}, nil)
//
//	statusProvider.On("GetAllTaskStatuses", mock.Anything).
//		Return(map[model.CaptureID]*model.TaskStatus{captureID: {}}, nil)
//
//	statusProvider.On("GetChangeFeedInfo", mock.Anything).
//		Return(&model.ChangeFeedInfo{State: model.StateNormal}, nil)
//
//	statusProvider.On("GetProcessors", mock.Anything).
//		Return([]*model.ProcInfoSnap{{CfID: changeFeedID, CaptureID: captureID}}, nil)
//
//	statusProvider.On("GetCaptures", mock.Anything).
//		Return([]*model.CaptureInfo{{ID: captureID}}, nil)
//
//	return statusProvider
//}

type mockAPIV2Helper struct {
	APIV2Helper

	verifyCreateFunc func(context.Context, *ChangefeedConfig, pd.Client,
		owner.StatusProvider, string, tidbkv.Storage) (*model.ChangeFeedInfo, error)
	verifyUpdateFunc func(context.Context, *ChangefeedConfig, *model.ChangeFeedInfo,
		*model.UpstreamInfo) (*model.ChangeFeedInfo, *model.UpstreamInfo, error)
	getPDClientFunc func(context.Context, []string, *security.Credential) (pd.Client, error)
	//getKvStorageFunc func()
}

func (m mockAPIV2Helper) verifyCreateChangefeedConfig(
	ctx context.Context,
	cfg *ChangefeedConfig,
	pdClient pd.Client,
	statusProvider owner.StatusProvider,
	ensureGCServiceID string,
	kvStorage tidbkv.Storage,
) (*model.ChangeFeedInfo, error) {
	if m.verifyCreateFunc != nil {
		return m.verifyCreateFunc(ctx, cfg, pdClient, statusProvider, ensureGCServiceID, kvStorage)
	}
	return APIV2HelperImpl{}.verifyCreateChangefeedConfig(ctx, cfg, pdClient, statusProvider, ensureGCServiceID, kvStorage)
}

func (m mockAPIV2Helper) verifyUpdateChangefeedConfig(
	ctx context.Context,
	cfg *ChangefeedConfig,
	oldInfo *model.ChangeFeedInfo,
	oldUpInfo *model.UpstreamInfo,
) (*model.ChangeFeedInfo, *model.UpstreamInfo, error) {
	if m.verifyUpdateFunc != nil {
		return m.verifyUpdateFunc(ctx, cfg, oldInfo, oldUpInfo)
	}
	return APIV2HelperImpl{}.verifyUpdateChangefeedConfig(ctx, cfg, oldInfo, oldUpInfo)
}

func (m mockAPIV2Helper) getPDClient(ctx context.Context,
	pdAddrs []string,
	credential *security.Credential,
) (pd.Client, error) {
	if m.verifyUpdateFunc != nil {
		return m.getPDClientFunc(ctx, pdAddrs, credential)
	}
	return APIV2HelperImpl{}.getPDClient(ctx, pdAddrs, credential)
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
