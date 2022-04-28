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

package api

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
	"github.com/pingcap/tiflow/cdc/model"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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

func (p *mockStatusProvider) GetAllChangeFeedStatuses(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedStatus, error) {
	args := p.Called(ctx)
	return args.Get(0).(map[model.ChangeFeedID]*model.ChangeFeedStatus), args.Error(1)
}

func (p *mockStatusProvider) GetChangeFeedStatus(ctx context.Context, changefeedID model.ChangeFeedID) (*model.ChangeFeedStatus, error) {
	args := p.Called(ctx, changefeedID)
	log.Info("err", zap.Error(args.Error(1)))
	return args.Get(0).(*model.ChangeFeedStatus), args.Error(1)
}

func (p *mockStatusProvider) GetAllChangeFeedInfo(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedInfo, error) {
	args := p.Called(ctx)
	return args.Get(0).(map[model.ChangeFeedID]*model.ChangeFeedInfo), args.Error(1)
}

func (p *mockStatusProvider) GetChangeFeedInfo(ctx context.Context, changefeedID model.ChangeFeedID) (*model.ChangeFeedInfo, error) {
	args := p.Called(ctx)
	return args.Get(0).(*model.ChangeFeedInfo), args.Error(1)
}

func (p *mockStatusProvider) GetAllTaskStatuses(ctx context.Context, changefeedID model.ChangeFeedID) (map[model.CaptureID]*model.TaskStatus, error) {
	args := p.Called(ctx)
	return args.Get(0).(map[model.CaptureID]*model.TaskStatus), args.Error(1)
}

func (p *mockStatusProvider) GetTaskPositions(ctx context.Context, changefeedID model.ChangeFeedID) (map[model.CaptureID]*model.TaskPosition, error) {
	args := p.Called(ctx)
	return args.Get(0).(map[model.CaptureID]*model.TaskPosition), args.Error(1)
}

func (p *mockStatusProvider) GetProcessors(ctx context.Context) ([]*model.ProcInfoSnap, error) {
	args := p.Called(ctx)
	return args.Get(0).([]*model.ProcInfoSnap), args.Error(1)
}

func (p *mockStatusProvider) GetCaptures(ctx context.Context) ([]*model.CaptureInfo, error) {
	args := p.Called(ctx)
	return args.Get(0).([]*model.CaptureInfo), args.Error(1)
}

func newRouter(c *capture.Capture, p *mockStatusProvider) *gin.Engine {
	router := gin.New()
	RegisterOpenAPIRoutes(router, NewOpenAPI4Test(c, p))
	return router
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

func TestListChangefeed(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())

	// test list changefeed succeeded
	api := testCase{url: "/api/v1/changefeeds", method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp []model.ChangefeedCommonInfo
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, 2, len(resp))

	// test list changefeed with specific state
	api = testCase{url: fmt.Sprintf("/api/v1/changefeeds?state=%s", "stopped"), method: "GET"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	resp = []model.ChangefeedCommonInfo{}
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, 1, len(resp))
	require.Equal(t, model.StateStopped, resp[0].FeedState)
	require.Equal(t, uint64(0x2), resp[0].CheckpointTSO)
}

func TestGetChangefeed(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())

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
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())

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
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())

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
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())

	// test remove changefeed succeeded
	mo.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Do(func(adminJob model.AdminJob, done chan<- error) {
			require.EqualValues(t, changeFeedID, adminJob.CfID)
			require.EqualValues(t, model.AdminRemove, adminJob.Type)
			close(done)
		})
	api := testCase{url: fmt.Sprintf("/api/v1/changefeeds/%s", changeFeedID.ID), method: "DELETE"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)

	// test remove changefeed failed from owner size
	mo.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Do(func(adminJob model.AdminJob, done chan<- error) {
			done <- cerror.ErrChangeFeedNotExists.FastGenByArgs(adminJob.CfID)
			close(done)
		})
	api = testCase{url: fmt.Sprintf("/api/v1/changefeeds/%s", changeFeedID.ID), method: "DELETE"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")

	// test remove changefeed failed
	api = testCase{
		url:    fmt.Sprintf("/api/v1/changefeeds/%s", nonExistChangefeedID.ID),
		method: "DELETE",
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

func TestRebalanceTables(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())

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

func TestMoveTable(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())

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
	cp := capture.NewCapture4Test(mo)
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
	mo := mock_owner.NewMockOwner(ctrl)
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())
	// test get processor succeeded
	api := testCase{
		url:    fmt.Sprintf("/api/v1/processors/%s/%s", changeFeedID.ID, captureID),
		method: "GET",
	}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	processorDetail := &model.ProcessorDetail{}
	err := json.NewDecoder(w.Body).Decode(processorDetail)
	require.Nil(t, err)
	require.Equal(t, "test", processorDetail.Error.Message)

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
	err = json.NewDecoder(w.Body).Decode(httpError)
	require.Nil(t, err)
	require.Contains(t, httpError.Error, "capture not exists, non-exist-capture")
}

func TestListProcessor(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())
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
	mo := mock_owner.NewMockOwner(ctrl)
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())
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
	cp := capture.NewCapture4Test(mo)
	ownerRouter := newRouter(cp, newStatusProvider())
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

	// capture is not owner
	c := capture.NewCapture4Test(nil)
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

// TODO: finished these test cases after we decouple those APIs from etcdClient.
func TestCreateChangefeed(t *testing.T) {}
func TestUpdateChangefeed(t *testing.T) {}
func TestHealth(t *testing.T)           {}
