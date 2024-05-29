// Copyright 2021 PingCAP, Inc.
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

package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/capture"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	"github.com/pingcap/tiflow/cdc/scheduler"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	mock_etcd "github.com/pingcap/tiflow/pkg/etcd/mock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	changeFeedID         = model.DefaultChangeFeedID("test-changeFeed")
	captureID            = "test-capture"
	nonExistChangefeedID = model.DefaultChangeFeedID("non-exist-changefeed")
)

type mockStatusProvider struct {
	mock.Mock
}

type testCase struct {
	url    string
	method string
}

func (p *mockStatusProvider) GetAllChangeFeedStatuses(ctx context.Context) (
	map[model.ChangeFeedID]*model.ChangeFeedStatusForAPI, error,
) {
	args := p.Called(ctx)
	return args.Get(0).(map[model.ChangeFeedID]*model.ChangeFeedStatusForAPI), args.Error(1)
}

func (p *mockStatusProvider) GetChangeFeedStatus(ctx context.Context, changefeedID model.ChangeFeedID) (
	*model.ChangeFeedStatusForAPI, error,
) {
	args := p.Called(ctx, changefeedID)
	log.Info("err", zap.Error(args.Error(1)))
	return args.Get(0).(*model.ChangeFeedStatusForAPI), args.Error(1)
}

func (p *mockStatusProvider) GetAllChangeFeedInfo(ctx context.Context) (
	map[model.ChangeFeedID]*model.ChangeFeedInfo, error,
) {
	args := p.Called(ctx)
	return args.Get(0).(map[model.ChangeFeedID]*model.ChangeFeedInfo), args.Error(1)
}

func (p *mockStatusProvider) GetChangeFeedInfo(ctx context.Context, changefeedID model.ChangeFeedID) (
	*model.ChangeFeedInfo, error,
) {
	args := p.Called(ctx)
	return args.Get(0).(*model.ChangeFeedInfo), args.Error(1)
}

func (p *mockStatusProvider) GetAllTaskStatuses(ctx context.Context, changefeedID model.ChangeFeedID) (
	map[model.CaptureID]*model.TaskStatus, error,
) {
	args := p.Called(ctx)
	return args.Get(0).(map[model.CaptureID]*model.TaskStatus), args.Error(1)
}

func (p *mockStatusProvider) GetProcessors(ctx context.Context) ([]*model.ProcInfoSnap, error) {
	args := p.Called(ctx)
	return args.Get(0).([]*model.ProcInfoSnap), args.Error(1)
}

func (p *mockStatusProvider) GetCaptures(ctx context.Context) ([]*model.CaptureInfo, error) {
	args := p.Called(ctx)
	return args.Get(0).([]*model.CaptureInfo), args.Error(1)
}

func (p *mockStatusProvider) GetChangeFeedSyncedStatus(ctx context.Context,
	changefeedID model.ChangeFeedID,
) (*model.ChangeFeedSyncedStatusForAPI, error) {
	args := p.Called(ctx)
	return args.Get(0).(*model.ChangeFeedSyncedStatusForAPI), args.Error(1)
}

func (p *mockStatusProvider) IsHealthy(ctx context.Context) (bool, error) {
	args := p.Called(ctx)
	return args.Get(0).(bool), args.Error(1)
}

func (p *mockStatusProvider) GetAllChangeFeedCheckpointTs(ctx context.Context) (map[model.ChangeFeedID]uint64, error) {
	args := p.Called(ctx)
	return args.Get(0).(map[model.ChangeFeedID]uint64), args.Error(1)
}

func (p *mockStatusProvider) IsChangefeedExists(ctx context.Context, id model.ChangeFeedID) (bool, error) {
	args := p.Called(ctx)
	return args.Get(0).(bool), args.Error(1)
}

func newRouter(c capture.Capture, p owner.StatusProvider) *gin.Engine {
	router := gin.New()
	RegisterOpenAPIRoutes(router, NewOpenAPI4Test(c, p))
	return router
}

func newRouterWithoutStatusProvider(c capture.Capture) *gin.Engine {
	router := gin.New()
	RegisterOpenAPIRoutes(router, NewOpenAPI(c))
	return router
}

func newStatusProvider() *mockStatusProvider {
	statusProvider := &mockStatusProvider{}
	statusProvider.On("GetChangeFeedStatus", mock.Anything, changeFeedID).
		Return(&model.ChangeFeedStatusForAPI{CheckpointTs: 1}, nil)

	statusProvider.On("GetChangeFeedStatus", mock.Anything, nonExistChangefeedID).
		Return(new(model.ChangeFeedStatusForAPI),
			cerror.ErrChangeFeedNotExists.GenWithStackByArgs(nonExistChangefeedID))

	statusProvider.On("GetAllTaskStatuses", mock.Anything).
		Return(map[model.CaptureID]*model.TaskStatus{captureID: {}}, nil)

	statusProvider.On("GetAllChangeFeedCheckpointTs", mock.Anything).
		Return(map[model.ChangeFeedID]*model.ChangeFeedStatusForAPI{
			model.ChangeFeedID4Test("ab", "123"):  {CheckpointTs: 1},
			model.ChangeFeedID4Test("ab", "13"):   {CheckpointTs: 2},
			model.ChangeFeedID4Test("abc", "123"): {CheckpointTs: 1},
			model.ChangeFeedID4Test("def", "456"): {CheckpointTs: 2},
		}, nil)

	statusProvider.On("GetAllChangeFeedInfo", mock.Anything).
		Return(map[model.ChangeFeedID]*model.ChangeFeedInfo{
			model.ChangeFeedID4Test("ab", "123"):  {State: model.StateNormal},
			model.ChangeFeedID4Test("ab", "13"):   {State: model.StateStopped},
			model.ChangeFeedID4Test("abc", "123"): {State: model.StateNormal},
			model.ChangeFeedID4Test("def", "456"): {State: model.StateStopped},
		}, nil)

	statusProvider.On("GetAllTaskStatuses", mock.Anything).
		Return(map[model.CaptureID]*model.TaskStatus{captureID: {}}, nil)

	statusProvider.On("GetChangeFeedInfo", mock.Anything).
		Return(&model.ChangeFeedInfo{
			State:          model.StateNormal,
			CreatorVersion: "v6.5.1",
		}, nil)

	statusProvider.On("GetProcessors", mock.Anything).
		Return([]*model.ProcInfoSnap{{CfID: changeFeedID, CaptureID: captureID}}, nil)

	statusProvider.On("GetCaptures", mock.Anything).
		Return([]*model.CaptureInfo{{ID: captureID}}, nil)

	return statusProvider
}

func TestListChangefeed(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	provider := mock_owner.NewMockStatusProvider(ctrl)
	cp := mock_capture.NewMockCapture(ctrl)
	cp.EXPECT().StatusProvider().Return(provider).AnyTimes()
	mo := mock_owner.NewMockOwner(ctrl)
	cp.EXPECT().GetOwner().Return(mo, nil).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	provider.EXPECT().GetAllChangeFeedCheckpointTs(gomock.Any()).Return(
		map[model.ChangeFeedID]uint64{
			model.ChangeFeedID4Test("ab", "123"):  1,
			model.ChangeFeedID4Test("ab", "13"):   2,
			model.ChangeFeedID4Test("abc", "123"): 1,
			model.ChangeFeedID4Test("def", "456"): 2,
		}, nil).AnyTimes()
	provider.EXPECT().GetAllChangeFeedInfo(gomock.Any()).Return(
		map[model.ChangeFeedID]*model.ChangeFeedInfo{
			model.ChangeFeedID4Test("ab", "123"):  {State: model.StateNormal},
			model.ChangeFeedID4Test("ab", "13"):   {State: model.StateStopped},
			model.ChangeFeedID4Test("abc", "123"): {State: model.StateNormal},
			model.ChangeFeedID4Test("def", "456"): {State: model.StateStopped},
		}, nil).AnyTimes()
	router := newRouterWithoutStatusProvider(cp)

	// test list changefeed succeeded
	api := testCase{url: "/api/v1/changefeeds", method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp []model.ChangefeedCommonInfo
	fmt.Println(w.Body.String())
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, 4, len(resp))
	require.Equal(t, "ab", resp[0].Namespace)
	require.Equal(t, "123", resp[0].ID)
	require.Equal(t, "ab", resp[1].Namespace)
	require.Equal(t, "13", resp[1].ID)
	require.Equal(t, "abc", resp[2].Namespace)
	require.Equal(t, "123", resp[2].ID)
	require.Equal(t, "def", resp[3].Namespace)
	require.Equal(t, "456", resp[3].ID)

	// test list changefeed with specific state
	api = testCase{url: fmt.Sprintf("/api/v1/changefeeds?state=%s", "stopped"), method: "GET"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	resp = []model.ChangefeedCommonInfo{}
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, 2, len(resp))
	require.Equal(t, model.StateStopped, resp[0].FeedState)
	require.Equal(t, model.StateStopped, resp[1].FeedState)
	require.Equal(t, uint64(0x2), resp[0].CheckpointTSO)
	require.Equal(t, uint64(0x2), resp[1].CheckpointTSO)
}

func TestGetChangefeed(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient.EXPECT().GetClusterID().Return("abcd").AnyTimes()
	cp := capture.NewCaptureWithOwner4Test(mo)
	cp.EtcdClient = etcdClient
	capture := mock_capture.NewMockCapture(ctrl)
	capture.EXPECT().GetOwner().Return(mo, nil).AnyTimes()
	capture.EXPECT().IsReady().Return(true).AnyTimes()
	capture.EXPECT().GetOwner().Return(mo, nil).AnyTimes()
	capture.EXPECT().IsReady().Return(true).AnyTimes()
	capture.EXPECT().IsOwner().Return(true).AnyTimes()
	statusProvider := mock_owner.NewMockStatusProvider(ctrl)
	capture.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	router := newRouterWithoutStatusProvider(capture)

	statusProvider.EXPECT().GetChangeFeedInfo(gomock.Any(), gomock.Any()).Return(&model.ChangeFeedInfo{
		State:          model.StateNormal,
		CreatorVersion: "v6.5.1",
	}, nil)
	statusProvider.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).
		Return(&model.ChangeFeedStatusForAPI{CheckpointTs: 1}, nil)
	statusProvider.EXPECT().GetAllTaskStatuses(gomock.Any(), gomock.Any()).
		Return(map[model.CaptureID]*model.TaskStatus{captureID: {}}, nil)
	// test get changefeed succeeded
	api := testCase{url: fmt.Sprintf("/api/v1/changefeeds/%s", changeFeedID.ID), method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp model.ChangefeedDetail
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, model.StateNormal, resp.FeedState)
	require.Equal(t, "v6.5.1", resp.CreatorVersion)

	statusProvider.EXPECT().GetChangeFeedInfo(gomock.Any(), gomock.Any()).Return(&model.ChangeFeedInfo{
		State:          model.StateNormal,
		CreatorVersion: "v6.5.1",
	}, nil)
	statusProvider.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).
		Return(new(model.ChangeFeedStatusForAPI),
			cerror.ErrChangeFeedNotExists.GenWithStackByArgs(nonExistChangefeedID))
	// test get changefeed failed
	api = testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s", nonExistChangefeedID.ID),
		method: "GET",
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestPauseChangefeed(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	capture := mock_capture.NewMockCapture(ctrl)
	capture.EXPECT().GetOwner().Return(mo, nil).AnyTimes()
	capture.EXPECT().IsOwner().Return(true).AnyTimes()
	capture.EXPECT().IsReady().Return(true).AnyTimes()
	statusProvider := mock_owner.NewMockStatusProvider(ctrl)
	capture.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	router := newRouterWithoutStatusProvider(capture)
	statusProvider.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).
		Return(&model.ChangeFeedStatusForAPI{CheckpointTs: 1}, nil).Times(2)

	// test pause changefeed succeeded
	mo.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Do(func(adminJob model.AdminJob, done chan<- error) {
			require.EqualValues(t, changeFeedID, adminJob.CfID)
			require.EqualValues(t, model.AdminStop, adminJob.Type)
			close(done)
		})
	api := testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s/pause", changeFeedID.ID),
		method: "POST",
	}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)

	// test pause changefeed failed from owner side
	mo.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Do(func(adminJob model.AdminJob, done chan<- error) {
			done <- cerror.ErrChangeFeedNotExists.FastGenByArgs(adminJob.CfID)
			close(done)
		})
	api = testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s/pause", changeFeedID.ID),
		method: "POST",
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")

	// test pause changefeed failed
	api = testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s/pause", nonExistChangefeedID.ID),
		method: "POST",
	}
	statusProvider.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).
		Return(new(model.ChangeFeedStatusForAPI),
			cerror.ErrChangeFeedNotExists.GenWithStackByArgs(nonExistChangefeedID))
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestResumeChangefeed(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	cp := mock_capture.NewMockCapture(ctrl)
	cp.EXPECT().GetOwner().Return(mo, nil).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	statusProvider := mock_owner.NewMockStatusProvider(ctrl)
	cp.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	router := newRouterWithoutStatusProvider(cp)
	statusProvider.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).
		Return(&model.ChangeFeedStatusForAPI{CheckpointTs: 1}, nil)

	// test resume changefeed succeeded
	mo.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Do(func(adminJob model.AdminJob, done chan<- error) {
			require.EqualValues(t, changeFeedID, adminJob.CfID)
			require.EqualValues(t, model.AdminResume, adminJob.Type)
			close(done)
		})
	api := testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s/resume", changeFeedID.ID),
		method: "POST",
	}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)

	// test resume changefeed failed from owner side.
	statusProvider.EXPECT().GetChangeFeedStatus(gomock.Any(), changeFeedID).
		Return(&model.ChangeFeedStatusForAPI{CheckpointTs: 1}, nil)
	mo.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Do(func(adminJob model.AdminJob, done chan<- error) {
			done <- cerror.ErrChangeFeedNotExists.FastGenByArgs(adminJob.CfID)
			close(done)
		})
	api = testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s/resume", changeFeedID.ID),
		method: "POST",
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")

	// test resume changefeed failed
	statusProvider.EXPECT().GetChangeFeedStatus(gomock.Any(), nonExistChangefeedID).
		Return(nil, cerror.ErrChangeFeedNotExists)
	api = testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s/resume", nonExistChangefeedID.ID),
		method: "POST",
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestRemoveChangefeed(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)

	capture := mock_capture.NewMockCapture(ctrl)
	provider := mock_owner.NewMockStatusProvider(ctrl)
	capture.EXPECT().GetOwner().Return(mo, nil).AnyTimes()
	capture.EXPECT().IsReady().Return(true).AnyTimes()
	capture.EXPECT().IsOwner().Return(true).AnyTimes()
	capture.EXPECT().StatusProvider().Return(provider).AnyTimes()
	router1 := newRouterWithoutStatusProvider(capture)

	// test remove changefeed succeeded
	mo.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Do(func(adminJob model.AdminJob, done chan<- error) {
			require.EqualValues(t, changeFeedID, adminJob.CfID)
			require.EqualValues(t, model.AdminRemove, adminJob.Type)
			close(done)
		})
	provider.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(true, nil)
	provider.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(false, nil)
	api := testCase{url: fmt.Sprintf("/api/v1/changefeeds/%s", changeFeedID.ID), method: "DELETE"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router1.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)

	router2 := newRouterWithoutStatusProvider(capture)
	provider.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(true, nil)

	// test remove changefeed failed from owner side
	mo.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Do(func(adminJob model.AdminJob, done chan<- error) {
			done <- cerror.ErrChangeFeedNotExists.FastGenByArgs(adminJob.CfID)
			close(done)
		})
	api = testCase{url: fmt.Sprintf("/api/v1/changefeeds/%s", changeFeedID.ID), method: "DELETE"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router2.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")

	// test remove changefeed failed
	provider.EXPECT().IsChangefeedExists(gomock.Any(), nonExistChangefeedID).Return(false, nil)
	api = testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s", nonExistChangefeedID.ID),
		method: "DELETE",
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router2.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestRebalanceTables(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	capture := mock_capture.NewMockCapture(ctrl)
	capture.EXPECT().GetOwner().Return(mo, nil).AnyTimes()
	capture.EXPECT().IsReady().Return(true).AnyTimes()
	capture.EXPECT().IsOwner().Return(true).AnyTimes()
	statusProvider := mock_owner.NewMockStatusProvider(ctrl)
	capture.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	router := newRouter(capture, statusProvider)

	statusProvider.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).
		Return(&model.ChangeFeedStatusForAPI{CheckpointTs: 1}, nil).Times(2)

	// test rebalance table succeeded
	mo.EXPECT().
		RebalanceTables(gomock.Any(), gomock.Any()).
		Do(func(cfID model.ChangeFeedID, done chan<- error) {
			require.EqualValues(t, cfID, changeFeedID)
			close(done)
		})
	api := testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s/tables/rebalance_table", changeFeedID.ID),
		method: "POST",
	}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)

	// test rebalance table failed from owner side.
	mo.EXPECT().
		RebalanceTables(gomock.Any(), gomock.Any()).
		Do(func(cfID model.ChangeFeedID, done chan<- error) {
			done <- cerror.ErrChangeFeedNotExists.FastGenByArgs(cfID)
			close(done)
		})
	api = testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s/tables/rebalance_table", changeFeedID.ID),
		method: "POST",
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")

	statusProvider.EXPECT().GetChangeFeedStatus(gomock.Any(), nonExistChangefeedID).
		Return(nil, cerror.ErrChangeFeedNotExists)
	// test rebalance table failed
	api = testCase{
		url: fmt.Sprintf("/api/v1/changefeeds/%s/tables/rebalance_table",
			nonExistChangefeedID.ID),
		method: "POST",
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestDrainCapture(t *testing.T) {
	t.Parallel()

	statusProvider := newStatusProvider()

	ctrl := gomock.NewController(t)
	owner := mock_owner.NewMockOwner(ctrl)
	capture := capture.NewCaptureWithOwner4Test(owner)
	router := newRouter(capture, statusProvider)

	captureInfo, err := capture.Info()
	require.NoError(t, err)
	data := model.DrainCaptureRequest{
		CaptureID: captureInfo.ID,
	}
	b, err := json.Marshal(&data)
	require.NoError(t, err)

	body := bytes.NewReader(b)
	api := testCase{
		url:    "/api/v1/captures/drain",
		method: "PUT",
	}
	request, err := http.NewRequestWithContext(context.Background(), api.method, api.url, body)
	require.NoError(t, err)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, request)
	// only has one capture
	require.Equal(t, http.StatusBadRequest, w.Code)
	respErr := model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "CDC:ErrSchedulerRequestFailed")
	require.Contains(t, respErr.Error, "scheduler request failed")

	statusProvider.ExpectedCalls = statusProvider.
		ExpectedCalls[:len(statusProvider.ExpectedCalls)-1]

	statusProvider.On("GetCaptures", mock.Anything).
		Return([]*model.CaptureInfo{{ID: captureID}, {ID: captureInfo.ID}}, nil)

	defer func() {
		statusProvider.ExpectedCalls = statusProvider.
			ExpectedCalls[:len(statusProvider.ExpectedCalls)-1]

		statusProvider.On("GetCaptures", mock.Anything).
			Return([]*model.CaptureInfo{{ID: captureID}}, nil)
	}()

	data = model.DrainCaptureRequest{CaptureID: "capture-not-found"}
	b, err = json.Marshal(&data)
	require.NoError(t, err)
	body = bytes.NewReader(b)

	request, err = http.NewRequestWithContext(context.Background(), api.method, api.url, body)
	require.NoError(t, err)

	w = httptest.NewRecorder()
	router.ServeHTTP(w, request)
	require.Equal(t, http.StatusBadRequest, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.NoError(t, err)
	require.Contains(t, respErr.Code, "CDC:ErrCaptureNotExist")
	require.Contains(t, respErr.Error, "capture not exists")

	data = model.DrainCaptureRequest{CaptureID: "capture-for-test"}
	b, err = json.Marshal(&data)
	require.NoError(t, err)
	body = bytes.NewReader(b)
	request, err = http.NewRequestWithContext(context.Background(), api.method, api.url, body)
	require.NoError(t, err)

	w = httptest.NewRecorder()
	router.ServeHTTP(w, request)
	require.Equal(t, http.StatusBadRequest, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.NoError(t, err)
	require.Contains(t, respErr.Code, "CDC:ErrSchedulerRequestFailed")
	require.Contains(t, respErr.Error, "cannot drain the owner")

	data = model.DrainCaptureRequest{CaptureID: captureID}
	b, err = json.Marshal(&data)
	require.NoError(t, err)
	body = bytes.NewReader(b)

	request, err = http.NewRequestWithContext(context.Background(), api.method, api.url, body)
	require.NoError(t, err)

	owner.EXPECT().DrainCapture(gomock.Any(), gomock.Any()).
		Do(func(query *scheduler.Query, done chan<- error) {
			query.Resp = &model.DrainCaptureResp{
				CurrentTableCount: 3,
			}
			done <- cerror.ErrSchedulerRequestFailed.
				GenWithStack("not all captures initialized")
			close(done)
		})
	w = httptest.NewRecorder()
	router.ServeHTTP(w, request)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.NoError(t, err)
	require.Contains(t, respErr.Code, "CDC:ErrSchedulerRequestFailed")
	require.Contains(t, respErr.Error, "not all captures initialized")

	body = bytes.NewReader(b)
	request, err = http.NewRequestWithContext(context.Background(), api.method, api.url, body)
	require.NoError(t, err)

	owner.EXPECT().DrainCapture(gomock.Any(), gomock.Any()).
		Do(func(query *scheduler.Query, done chan<- error) {
			query.Resp = &model.DrainCaptureResp{
				CurrentTableCount: 3,
			}
			close(done)
		})

	w = httptest.NewRecorder()
	router.ServeHTTP(w, request)
	require.Equal(t, http.StatusAccepted, w.Code)

	var resp model.DrainCaptureResp
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Equal(t, 3, resp.CurrentTableCount)
}

func TestMoveTable(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	cp := mock_capture.NewMockCapture(ctrl)
	cp.EXPECT().GetOwner().Return(mo, nil).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	statusProvider := newStatusProvider()
	cp.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	router := newRouter(cp, statusProvider)
	// test move table succeeded
	data := struct {
		CaptureID string `json:"capture_id"`
		TableID   int64  `json:"table_id"`
	}{captureID, 1}
	b, err := json.Marshal(&data)
	require.Nil(t, err)
	body := bytes.NewReader(b)
	mo.EXPECT().
		ScheduleTable(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(
			cfID model.ChangeFeedID, toCapture model.CaptureID,
			tableID model.TableID, done chan<- error,
		) {
			require.EqualValues(t, cfID, changeFeedID)
			require.EqualValues(t, toCapture, data.CaptureID)
			require.EqualValues(t, tableID, data.TableID)
			close(done)
		})
	api := testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s/tables/move_table", changeFeedID.ID),
		method: "POST",
	}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, body)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)

	// test move table failed from owner side.
	data = struct {
		CaptureID string `json:"capture_id"`
		TableID   int64  `json:"table_id"`
	}{captureID, 1}
	b, err = json.Marshal(&data)
	require.Nil(t, err)
	body = bytes.NewReader(b)
	mo.EXPECT().
		ScheduleTable(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(
			cfID model.ChangeFeedID, toCapture model.CaptureID,
			tableID model.TableID, done chan<- error,
		) {
			require.EqualValues(t, cfID, changeFeedID)
			require.EqualValues(t, toCapture, data.CaptureID)
			require.EqualValues(t, tableID, data.TableID)
			done <- cerror.ErrChangeFeedNotExists.FastGenByArgs(cfID)
			close(done)
		})
	api = testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s/tables/move_table", changeFeedID.ID),
		method: "POST",
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, body)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")

	// test move table failed
	api = testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s/tables/move_table", nonExistChangefeedID.ID),
		method: "POST",
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, body)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestResignOwner(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	cp := capture.NewCaptureWithOwner4Test(mo)
	router := newRouter(cp, newStatusProvider())
	// test resign owner succeeded
	mo.EXPECT().AsyncStop()
	api := testCase{url: "/api/v1/owner/resign", method: "POST"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)
}

func TestGetProcessor(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cp := mock_capture.NewMockCapture(ctrl)
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	statusProvider := mock_owner.NewMockStatusProvider(ctrl)
	cp.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	router := newRouter(cp, statusProvider)
	dataProvider := newStatusProvider()
	statusProvider.EXPECT().GetChangeFeedInfo(gomock.Any(), changeFeedID).Return(
		dataProvider.GetChangeFeedInfo(context.Background(), changeFeedID)).AnyTimes()
	statusProvider.EXPECT().GetChangeFeedStatus(gomock.Any(), changeFeedID).Return(
		dataProvider.GetChangeFeedStatus(context.Background(), changeFeedID)).AnyTimes()
	statusProvider.EXPECT().GetProcessors(gomock.Any()).Return(
		dataProvider.GetProcessors(context.Background())).AnyTimes()
	statusProvider.EXPECT().GetAllTaskStatuses(gomock.Any(), gomock.Any()).Return(
		dataProvider.GetAllTaskStatuses(context.Background(), changeFeedID)).AnyTimes()
	// test get processor succeeded
	api := testCase{
		url:    fmt.Sprintf("/api/v1/processors/%s/%s", changeFeedID.ID, captureID),
		method: "GET",
	}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)

	// test get processor fail due to capture ID error
	api = testCase{
		url:    fmt.Sprintf("/api/v1/processors/%s/%s", changeFeedID.ID, "non-exist-capture"),
		method: "GET",
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	httpError := &model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(httpError)
	require.Nil(t, err)
	require.Contains(t, httpError.Error, "capture not exists, non-exist-capture")
}

func TestListProcessor(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cp := mock_capture.NewMockCapture(ctrl)
	statusProvider := mock_owner.NewMockStatusProvider(ctrl)
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	cp.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	router := newRouterWithoutStatusProvider(cp)
	statusProvider.EXPECT().GetProcessors(gomock.Any()).
		Return([]*model.ProcInfoSnap{{CfID: changeFeedID, CaptureID: captureID}}, nil)
	// test list processor succeeded
	api := testCase{url: "/api/v1/processors", method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp []model.ProcessorCommonInfo
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, changeFeedID, model.DefaultChangeFeedID(resp[0].CfID))
}

func TestListCapture(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient.EXPECT().GetClusterID().Return("abcd").AnyTimes()
	cp := mock_capture.NewMockCapture(ctrl)
	statusProvider := mock_owner.NewMockStatusProvider(ctrl)
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	cp.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	cp.EXPECT().GetEtcdClient().Return(etcdClient)
	cp.EXPECT().Info().Return(model.CaptureInfo{ID: captureID}, nil)
	router := newRouterWithoutStatusProvider(cp)
	statusProvider.EXPECT().GetCaptures(gomock.Any()).Return([]*model.CaptureInfo{{ID: captureID}}, nil)
	// test list processor succeeded
	api := testCase{url: "/api/v1/captures", method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp []model.Capture
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, captureID, resp[0].ID)
}

func TestServerStatus(t *testing.T) {
	t.Parallel()
	// capture is owner
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	cp := capture.NewCaptureWithOwner4Test(mo)
	etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient.EXPECT().GetClusterID().Return("abcd").AnyTimes()
	ownerRouter := newRouter(cp, newStatusProvider())
	cp.EtcdClient = etcdClient
	api := testCase{url: "/api/v1/status", method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	ownerRouter.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp model.ServerStatus
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, "capture-for-test", resp.ID)
	require.True(t, resp.IsOwner)
	require.Equal(t, "abcd", resp.ClusterID)

	// capture is not owner
	c := capture.NewCapture4Test(nil)
	c.EtcdClient = etcdClient
	r := gin.New()
	RegisterOpenAPIRoutes(r, NewOpenAPI4Test(c, nil))
	api = testCase{url: "/api/v1/status", method: "GET"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	r.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	resp = model.ServerStatus{}
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.False(t, resp.IsOwner)
	require.Equal(t, "abcd", resp.ClusterID)
}

func TestServerStatusLiveness(t *testing.T) {
	t.Parallel()
	// capture is owner
	ctrl := gomock.NewController(t)
	cp := mock_capture.NewMockCapture(ctrl)
	cp.EXPECT().IsOwner().Return(false).AnyTimes()
	etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient.EXPECT().GetClusterID().Return("abcd").AnyTimes()
	ownerRouter := newRouter(cp, newStatusProvider())
	api := testCase{url: "/api/v1/status", method: "GET"}

	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().Info().DoAndReturn(func() (model.CaptureInfo, error) {
		return model.CaptureInfo{}, nil
	}).AnyTimes()
	cp.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()

	// Alive.
	alive := cp.EXPECT().Liveness().DoAndReturn(func() model.Liveness {
		return model.LivenessCaptureAlive
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	ownerRouter.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp model.ServerStatus
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.EqualValues(t, model.LivenessCaptureAlive, resp.Liveness)

	// Draining the capture.
	cp.EXPECT().Liveness().DoAndReturn(func() model.Liveness {
		return model.LivenessCaptureStopping
	}).After(alive)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	ownerRouter.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.EqualValues(t, model.LivenessCaptureStopping, resp.Liveness)
}

func TestSetLogLevel(t *testing.T) {
	t.Parallel()

	// test set log level succeeded
	data := struct {
		Level string `json:"log_level"`
	}{"warn"}
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())
	api := testCase{url: "/api/v1/log", method: "POST"}
	w := httptest.NewRecorder()
	b, err := json.Marshal(&data)
	require.Nil(t, err)
	body := bytes.NewReader(b)
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, body)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)

	// test set log level failed
	data = struct {
		Level string `json:"log_level"`
	}{"foo"}
	api = testCase{url: "/api/v1/log", method: "POST"}
	w = httptest.NewRecorder()
	b, err = json.Marshal(&data)
	require.Nil(t, err)
	body = bytes.NewReader(b)
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, body)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	httpError := &model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(httpError)
	require.Nil(t, err)
	require.Contains(t, httpError.Error, "fail to change log level: foo")
}

func TestHealth(t *testing.T) {
	t.Parallel()
	// capture is owner
	ctrl := gomock.NewController(t)
	cp := mock_capture.NewMockCapture(ctrl)
	cp.EXPECT().IsOwner().Return(true).AnyTimes()

	api := testCase{url: "/api/v1/health", method: "GET"}
	sp := mock_owner.NewMockStatusProvider(ctrl)
	ownerRouter := newRouter(cp, sp)

	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().Info().DoAndReturn(func() (model.CaptureInfo, error) {
		return model.CaptureInfo{}, nil
	}).AnyTimes()

	// IsHealthy returns error.
	isHealthError := sp.EXPECT().IsHealthy(gomock.Any()).
		Return(false, cerror.ErrOwnerNotFound)
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	ownerRouter.ServeHTTP(w, req)
	require.Equal(t, 500, w.Code)

	// IsHealthy returns false.
	isHealthFalse := sp.EXPECT().IsHealthy(gomock.Any()).
		Return(false, cerror.ErrOwnerNotFound).After(isHealthError)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	ownerRouter.ServeHTTP(w, req)
	require.Equal(t, 500, w.Code)

	// IsHealthy returns true.
	sp.EXPECT().IsHealthy(gomock.Any()).
		Return(true, nil).After(isHealthFalse)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	ownerRouter.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
}

// TODO: finished these test cases after we decouple those APIs from etcdClient.
func TestCreateChangefeed(t *testing.T) {}
func TestUpdateChangefeed(t *testing.T) {}
