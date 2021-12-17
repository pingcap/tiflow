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

package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	cerror "github.com/pingcap/ticdc/pkg/errors"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/owner"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	changeFeedID       = "test-changeFeed"
	captureID          = "test-capture"
	nonExistChanfeedID = "non-exist-changfeed"
)

type mockStatusProvider struct {
	mock.Mock
}

func (p *mockStatusProvider) GetAllChangeFeedStatuses(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedStatus, error) {
	args := p.Called(ctx)
	return args.Get(0).(map[model.ChangeFeedID]*model.ChangeFeedStatus), args.Error(1)
}

func (p *mockStatusProvider) GetChangeFeedStatus(ctx context.Context, changefeedID model.ChangeFeedID) (*model.ChangeFeedStatus, error) {
	args := p.Called(ctx, changefeedID)
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

func newRouter(p *mockStatusProvider) *gin.Engine {
	capture := NewCapture4Test()
	capture.owner = &owner.Owner{StatusProvider: p}
	handler := HTTPHandler{capture: capture}
	return NewRouter(handler)
}

func newStatusServer() *mockStatusProvider {
	statusProvider := &mockStatusProvider{}
	statusProvider.On("GetChangeFeedStatus", mock.Anything, changeFeedID).
		Return(&model.ChangeFeedStatus{CheckpointTs: 1}, nil)

	statusProvider.On("GetChangeFeedStatus", mock.Anything, nonExistChanfeedID).
		Return(new(model.ChangeFeedStatus),
			cerror.ErrChangeFeedNotExists.GenWithStackByArgs(nonExistChanfeedID))

	statusProvider.On("GetAllTaskStatuses", mock.Anything).
		Return(map[model.CaptureID]*model.TaskStatus{captureID: {}}, nil)

	statusProvider.On("GetTaskPositions", mock.Anything).
		Return(map[model.CaptureID]*model.TaskPosition{captureID: {Error: &model.RunningError{Message: "test"}}}, nil)

	statusProvider.On("GetAllChangeFeedStatuses", mock.Anything).
		Return(map[model.ChangeFeedID]*model.ChangeFeedStatus{
			changeFeedID + "1": {CheckpointTs: 1},
			changeFeedID + "2": {CheckpointTs: 2},
		}, nil)

	statusProvider.On("GetAllChangeFeedInfo", mock.Anything).
		Return(map[model.ChangeFeedID]*model.ChangeFeedInfo{
			changeFeedID + "1": {State: model.StateNormal},
			changeFeedID + "2": {State: model.StateStopped},
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
	router := newRouter(newStatusServer())

	// test list changefeed succeeded
	api := openAPI{url: fmt.Sprint("/api/v1/changefeeds"), method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp []model.ChangefeedCommonInfo
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, 2, len(resp))

	// test list changefeed with specific state
	api = openAPI{url: fmt.Sprintf("/api/v1/changefeeds?state=%s", "stopped"), method: "GET"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(api.method, api.url, nil)
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
	router := newRouter(newStatusServer())

	// test get changefeed succeeded
	api := openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s", changeFeedID), method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp model.ChangefeedDetail
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, model.StateNormal, resp.FeedState)

	// test get changefeed failed
	api = openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s", nonExistChanfeedID), method: "GET"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestPauseChangefeed(t *testing.T) {
	t.Parallel()
	router := newRouter(newStatusServer())
	// test pause changefeed succeeded
	api := openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s/pause", changeFeedID), method: "POST"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)

	// test pause changefeed failed
	api = openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s/pause", nonExistChanfeedID), method: "POST"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestResumeChangefeed(t *testing.T) {
	t.Parallel()
	router := newRouter(newStatusServer())
	// test resume changefeed succeeded
	api := openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s/resume", changeFeedID), method: "POST"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)

	// test resume changefeed failed
	api = openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s/resume", nonExistChanfeedID), method: "POST"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestRemoveChangefeed(t *testing.T) {
	t.Parallel()
	router := newRouter(newStatusServer())
	// test remove changefeed succeeded
	api := openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s", changeFeedID), method: "DELETE"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)

	// test remove changefeed failed
	api = openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s", nonExistChanfeedID), method: "DELETE"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestRebalanceTable(t *testing.T) {
	t.Parallel()
	router := newRouter(newStatusServer())
	// test rebalance table succeeded
	api := openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s/tables/rebalance_table", changeFeedID), method: "POST"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)

	// test rebalance table failed
	api = openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s/tables/rebalance_table", nonExistChanfeedID), method: "POST"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestMoveTable(t *testing.T) {
	t.Parallel()

	data := struct {
		CaptureID string `json:"capture_id"`
		TableID   int64  `json:"table_id"`
	}{captureID, 1}
	b, err := json.Marshal(&data)
	require.Nil(t, err)
	body := bytes.NewReader(b)

	router := newRouter(newStatusServer())
	// test move table succeeded
	api := openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s/tables/move_table", changeFeedID), method: "POST"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, body)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)

	// test move table failed
	api = openAPI{url: fmt.Sprintf("/api/v1/changefeeds/%s/tables/move_table", nonExistChanfeedID), method: "POST"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(api.method, api.url, body)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	respErr := model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Error, "changefeed not exists")
}

func TestResignOwner(t *testing.T) {
	t.Parallel()
	router := newRouter(newStatusServer())
	// test resign owner succeeded
	api := openAPI{url: fmt.Sprint("/api/v1/owner/resign"), method: "POST"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 202, w.Code)
}

func TestGetProcessor(t *testing.T) {
	t.Parallel()
	router := newRouter(newStatusServer())
	// test get processor succeeded
	api := openAPI{url: fmt.Sprintf("/api/v1/processors/%s/%s", changeFeedID, captureID), method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	processorDetail := &model.ProcessorDetail{}
	err := json.NewDecoder(w.Body).Decode(processorDetail)
	require.Nil(t, err)
	require.Equal(t, "test", processorDetail.Error.Message)

	// test get processor fail due to capture ID error
	api = openAPI{url: fmt.Sprintf("/api/v1/processors/%s/%s", changeFeedID, "non-exist-capture"), method: "GET"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	httpError := &model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(httpError)
	require.Nil(t, err)
	require.Contains(t, httpError.Error, "capture not exists, key: non-exist-capture")
}

func TestListProcessor(t *testing.T) {
	t.Parallel()
	router := newRouter(newStatusServer())
	// test list processor succeeded
	api := openAPI{url: fmt.Sprint("/api/v1/processors"), method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp []model.ProcessorCommonInfo
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, changeFeedID, resp[0].CfID)
}

func TestListCapture(t *testing.T) {
	t.Parallel()
	router := newRouter(newStatusServer())
	// test list processor succeeded
	api := openAPI{url: fmt.Sprint("/api/v1/captures"), method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp []model.Capture
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, captureID, resp[0].ID)
}

func TestServerStatus(t *testing.T) {
	t.Parallel()
	// cpature is owner
	ownerRouter := newRouter(newStatusServer())
	api := openAPI{url: fmt.Sprint("/api/v1/status"), method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	ownerRouter.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var resp model.ServerStatus
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, "capture-for-test", resp.ID)
	require.True(t, resp.IsOwner)

	// capture is not owner
	capture := NewCapture4Test()
	router := NewRouter(NewHTTPHandler(capture))
	api = openAPI{url: fmt.Sprint("/api/v1/status"), method: "GET"}
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
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
	router := newRouter(newStatusServer())
	api := openAPI{url: fmt.Sprint("/api/v1/log"), method: "POST"}
	w := httptest.NewRecorder()
	b, err := json.Marshal(&data)
	require.Nil(t, err)
	body := bytes.NewReader(b)
	req, _ := http.NewRequest(api.method, api.url, body)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)

	// test set log level failed
	data = struct {
		Level string `json:"log_level"`
	}{"foo"}
	api = openAPI{url: fmt.Sprint("/api/v1/log"), method: "POST"}
	w = httptest.NewRecorder()
	b, err = json.Marshal(&data)
	require.Nil(t, err)
	body = bytes.NewReader(b)
	req, _ = http.NewRequest(api.method, api.url, body)
	router.ServeHTTP(w, req)
	require.Equal(t, 400, w.Code)
	httpError := &model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(httpError)
	require.Nil(t, err)
	require.Contains(t, httpError.Error, "fail to change log level: foo")
}

// TODO: finished these two test case after we decouple these two API from etcdClient.
func TestCreateChangefeed(t *testing.T) {}
func TestUpdateChangefeed(t *testing.T) {}
func TestHealth(t *testing.T)           {}
