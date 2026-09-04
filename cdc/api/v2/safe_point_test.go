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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type mockPDClient4SafePoint struct {
	pd.Client
	err error
}

var (
	startTs       uint64 = 451560023174938623
	ttl           int64  = 86400
	serviceID     string = "user-defined"
	expiredAt     int64  = 1722651185
	mockSafePoint        = SafePoint{
		ListServiceGCSafepoint: ListServiceGCSafepoint{
			ServiceGCSafepoints: []*ServiceSafePoint{
				{
					ServiceID: serviceID,
					ExpiredAt: expiredAt,
					SafePoint: startTs,
				},
			},
		},
	}
)

func (m *mockPDClient4SafePoint) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, TTL int64, safePoint uint64) (uint64, error) {
	return startTs, m.err
}

func TestQuerySafePoint(t *testing.T) {
	safepoint := testCase{url: "/api/v2/safepoint", method: "GET"}

	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	helpers.EXPECT().getPDSafepoint(cp).Return(mockSafePoint, nil).AnyTimes()

	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(),
		safepoint.method, safepoint.url, nil)
	router.ServeHTTP(w, req)
	resp := SafePoint{}
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestSetSafePoint(t *testing.T) {
	t.Parallel()

	mockPDClient := &mockPDClient4SafePoint{}
	mockManager := upstream.NewManager4Test(mockPDClient)

	safepoint := testCase{url: "/api/v2/safepoint", method: "POST"}
	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().GetUpstreamManager().Return(mockManager, nil).AnyTimes()
	helpers.EXPECT().getPDSafepoint([]string{}).Return(mockSafePoint, nil).AnyTimes()

	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)

	// case 1: json format mismatches with the spec.
	errConfig := struct {
		StartTs         uint64 `json:"start-ts"`
		TTL             int64  `json:"ttl"` // should bigger than zero
		ServiceIDSuffix string `json:"service-id-suffix"`
	}{
		StartTs: startTs,
		TTL:     -1,
	}
	bodyErr, err := json.Marshal(&errConfig)
	require.Nil(t, err)
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(),
		safepoint.method, safepoint.url, bytes.NewReader(bodyErr))
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	respErr := model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrAPIInvalidParam")

	// case 2: get safepoint failed
	mockPDClient.err = cerrors.ErrAPIGetPDClientFailed
	sfConfig := SafePointConfig{
		TTL: ttl,
	}
	body, err := json.Marshal(&sfConfig)
	require.Nil(t, err)

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(),
		safepoint.method, safepoint.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrInternalServerError")

	// case3: success
	w = httptest.NewRecorder()
	sfConfig = SafePointConfig{
		StartTs: startTs,
		TTL:     ttl,
	}
	req, _ = http.NewRequestWithContext(context.Background(),
		safepoint.method, safepoint.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	resp := SafePoint{}
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestDeleteSafePoint(t *testing.T) {
	t.Parallel()

	mockPDClient := &mockPDClient4SafePoint{}
	mockManager := upstream.NewManager4Test(mockPDClient)

	safepoint := testCase{url: "/api/v2/safepoint", method: "DELETE"}

	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().GetUpstreamManager().Return(mockManager, nil).AnyTimes()
	helpers.EXPECT().getPDSafepoint(cp).Return(mockSafePoint, nil).AnyTimes()

	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)

	// case 1: json format mismatches with the spec.
	errConfig := struct {
		StartTs         uint64 `json:"start-ts"`
		TTL             string `json:"ttl"` // shoule be int64
		ServiceIDSuffix string `json:"service-id-suffix"`
	}{
		StartTs: startTs,
		TTL:     "ttl",
	}
	bodyErr, err := json.Marshal(&errConfig)
	require.Nil(t, err)
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(),
		safepoint.method, safepoint.url, bytes.NewReader(bodyErr))
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	respErr := model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrAPIInvalidParam")

	// case 2: get safepoint failed
	mockPDClient.err = cerrors.ErrAPIGetPDClientFailed
	sfConfig := SafePointConfig{}
	body, err := json.Marshal(&sfConfig)
	require.Nil(t, err)

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(),
		safepoint.method, safepoint.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrInternalServerError")

	// case3: success
	w = httptest.NewRecorder()
	sfConfig = SafePointConfig{
		StartTs: startTs,
		TTL:     ttl,
	}
	req, _ = http.NewRequestWithContext(context.Background(),
		safepoint.method, safepoint.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	resp := SafePoint{}
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, http.StatusOK, w.Code)
}
