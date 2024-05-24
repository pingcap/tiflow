// Copyright 2023 PingCAP, Inc.
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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestHealth(t *testing.T) {
	t.Parallel()
	health := testCase{url: "/api/v2/health", method: "GET"}
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)

	statusProvider := mock_owner.NewMockStatusProvider(gomock.NewController(t))
	cp.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()

	statusProvider.EXPECT().IsHealthy(gomock.Any()).Return(false,
		cerror.ErrUnknown.FastGenByArgs())
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), health.method,
		health.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrUnknown")

	statusProvider.EXPECT().IsHealthy(gomock.Any()).Return(false, nil)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), health.method,
		health.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrClusterIsUnhealthy")

	statusProvider.EXPECT().IsHealthy(gomock.Any()).Return(true, nil)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), health.method,
		health.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "{}", w.Body.String())
}
