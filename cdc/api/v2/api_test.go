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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	//cerror "github.com/pingcap/tiflow/pkg/errors"
	//"github.com/pingcap/tiflow/pkg/upstream"
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

type testCase struct {
	url    string
	method string
}

func newRouter(c *capture.Capture, p *mockStatusProvider) *gin.Engine {
	router := gin.New()
	RegisterOpenAPIV2Routes(router, NewOpenAPIV2ForTest(c, p))
	return router
}

func newStatusProvider() *mockStatusProvider {
	statusProvider := &mockStatusProvider{}

	return statusProvider
}

func TestGetTso(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	cp := capture.NewCapture4Test(mo)
	router := newRouter(cp, newStatusProvider())

	// test get tso succeeded
	api := testCase{url: "/api/v2/tso", method: "GET"}
	w := httptest.NewRecorder()
	req, err := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
	require.Nil(t, err)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
}
