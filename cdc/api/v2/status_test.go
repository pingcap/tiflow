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
	mock_etcd "github.com/pingcap/tiflow/pkg/etcd/mock"
	"github.com/stretchr/testify/require"
)

func TestGetStatus(t *testing.T) {
	status := testCase{url: "/api/v2/status", method: "GET"}
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)
	etcdClient := mock_etcd.NewMockCDCEtcdClient(gomock.NewController(t))

	etcdClient.EXPECT().
		GetClusterID().Return("1234").AnyTimes()
	cp.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsController().Return(true).AnyTimes()
	cp.EXPECT().Liveness().Return(model.LivenessCaptureStopping).AnyTimes()
	cp.EXPECT().Info().Return(model.CaptureInfo{
		ID: "capture-id",
	}, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), status.method, status.url, nil)
	router.ServeHTTP(w, req)
	resp := model.ServerStatus{}
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, "1234", resp.ClusterID)
	require.Equal(t, model.LivenessCaptureStopping, resp.Liveness)
	require.True(t, resp.IsOwner)
	require.Equal(t, "capture-id", resp.ID)
	require.Equal(t, http.StatusOK, w.Code)
}
