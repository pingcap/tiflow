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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/stretchr/testify/mock"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/cdc/owner"
	"github.com/stretchr/testify/require"
)

const (
	changeFeedID = "test-changeFeed"
	captureID    = "test-capture"
)

type mockStatusProvider struct {
	mock.Mock
}

func (p *mockStatusProvider) GetAllChangeFeedStatuses(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedStatus, error) {
	args := p.Called(ctx)
	return args.Get(0).(map[model.ChangeFeedID]*model.ChangeFeedStatus), args.Error(1)
}

func (p *mockStatusProvider) GetChangeFeedStatus(ctx context.Context, changefeedID model.ChangeFeedID) (*model.ChangeFeedStatus, error) {
	args := p.Called(ctx)
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

func TestGetProcessor(t *testing.T) {
	statusProvider := &mockStatusProvider{}
	statusProvider.On("GetAllTaskStatuses", mock.Anything).
		Return(map[model.CaptureID]*model.TaskStatus{captureID: {}}, nil)
	statusProvider.On("GetTaskPositions", mock.Anything).
		Return(map[model.CaptureID]*model.TaskPosition{
			captureID: {Error: &model.RunningError{Message: "test"}},
		}, nil)

	router := newRouter(statusProvider)

	api := openAPI{url: fmt.Sprintf("/api/v1/processors/%s/%s", changeFeedID, captureID), method: "GET"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(api.method, api.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	processorDetail := &model.ProcessorDetail{}
	err := json.NewDecoder(w.Body).Decode(processorDetail)
	require.Nil(t, err)
	require.Equal(t, "test", processorDetail.Error.Message)

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
