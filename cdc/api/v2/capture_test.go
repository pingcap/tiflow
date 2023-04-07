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
	"github.com/pingcap/tiflow/pkg/errors"
	mock_etcd "github.com/pingcap/tiflow/pkg/etcd/mock"
	"github.com/stretchr/testify/require"
)

func TestListCaptures(t *testing.T) {
	t.Parallel()

	// case 1: get captures failed
	{
		ctrl := gomock.NewController(t)
		cp := mock_capture.NewMockCapture(ctrl)
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		cp.EXPECT().IsOwner().Return(true).AnyTimes()
		statusProvider := mock_owner.NewMockStatusProvider(ctrl)
		cp.EXPECT().StatusProvider().Return(statusProvider)
		statusProvider.EXPECT().GetCaptures(gomock.Any()).Return(nil, errors.New("fake"))

		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			"GET",
			"/api/v2/captures",
			nil,
		)
		router.ServeHTTP(w, req)
		respErr := model.HTTPError{}
		err := json.NewDecoder(w.Body).Decode(&respErr)
		require.Nil(t, err)
		require.Contains(t, respErr.Error, "fake")
		require.Equal(t, http.StatusInternalServerError, w.Code)
	}

	// case 2: capture Info failed.
	{
		ctrl := gomock.NewController(t)
		cp := mock_capture.NewMockCapture(ctrl)
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		cp.EXPECT().IsOwner().Return(true).AnyTimes()
		statusProvider := mock_owner.NewMockStatusProvider(ctrl)
		cp.EXPECT().StatusProvider().Return(statusProvider)
		statusProvider.EXPECT().GetCaptures(gomock.Any()).Return([]*model.CaptureInfo{}, nil)
		cp.EXPECT().Info().Return(model.CaptureInfo{}, errors.New("fake info"))

		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			"GET",
			"/api/v2/captures",
			nil,
		)
		router.ServeHTTP(w, req)
		respErr := model.HTTPError{}
		err := json.NewDecoder(w.Body).Decode(&respErr)
		require.Nil(t, err)
		require.Contains(t, respErr.Error, "fake info")
		require.Equal(t, http.StatusInternalServerError, w.Code)
	}

	// case 3: success
	{
		ctrl := gomock.NewController(t)
		cp := mock_capture.NewMockCapture(ctrl)
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		cp.EXPECT().IsOwner().Return(true).AnyTimes()
		statusProvider := mock_owner.NewMockStatusProvider(ctrl)
		cp.EXPECT().StatusProvider().Return(statusProvider)
		statusProvider.EXPECT().GetCaptures(gomock.Any()).Return([]*model.CaptureInfo{
			{
				ID:            "owner-id",
				AdvertiseAddr: "add1",
			},
			{
				ID:            "capture-id",
				AdvertiseAddr: "add2",
			},
		}, nil)
		cp.EXPECT().Info().Return(model.CaptureInfo{
			ID: "owner-id",
		}, nil)
		etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
		etcdClient.EXPECT().GetClusterID().AnyTimes().Return("cdc-cluster-id")
		cp.EXPECT().GetEtcdClient().AnyTimes().Return(etcdClient)

		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			"GET",
			"/api/v2/captures",
			nil,
		)
		router.ServeHTTP(w, req)
		respCaptures := &ListResponse[Capture]{}
		err := json.NewDecoder(w.Body).Decode(&respCaptures)
		require.Nil(t, err)
		require.Equal(t, 2, respCaptures.Total)
		require.Equal(t, 2, len(respCaptures.Items))
		require.Equal(t, http.StatusOK, w.Code)
		for _, item := range respCaptures.Items {
			if item.ID == "owner-id" {
				require.True(t, item.IsOwner)
				require.Equal(t, "add1", item.AdvertiseAddr)
				require.Equal(t, "cdc-cluster-id", item.ClusterID)
			} else {
				require.False(t, item.IsOwner)
				require.Equal(t, "add2", item.AdvertiseAddr)
				require.Equal(t, "cdc-cluster-id", item.ClusterID)
			}
		}
	}
}
