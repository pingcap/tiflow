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
	"time"

	"github.com/golang/mock/gomock"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

type mockPDClient4Tso struct {
	pd.Client
	err error
}

func (m *mockPDClient4Tso) GetTS(context.Context) (int64, int64, error) {
	return oracle.GetPhysical(time.Now()), 0, m.err
}

func TestQueryTso(t *testing.T) {
	t.Parallel()

	mockPDClient := &mockPDClient4Tso{}
	mockManager := upstream.NewManager4Test(mockPDClient)

	queryTso := testCase{url: "/api/v2/tso", method: "POST"}

	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().GetUpstreamManager().Return(mockManager, nil).AnyTimes()

	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)

	// case 1: json format mismatch
	errUpConfig := struct {
		ID string `json:"id"` // UpStream ID should be uint64
	}{ID: "wrong-type"}

	body, err := json.Marshal(&errUpConfig)
	require.Nil(t, err)
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(),
		queryTso.method, queryTso.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	respErr := model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrAPIInvalidParam")

	// case 2: get tso failed
	mockPDClient.err = cerrors.ErrAPIGetPDClientFailed
	upConfig := struct {
		ID uint64 `json:"id"`
	}{ID: 0}
	body, err = json.Marshal(&upConfig)
	require.Nil(t, err)

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(),
		queryTso.method, queryTso.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrInternalServerError")

	// case3: success
	mockPDClient.err = nil
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(),
		queryTso.method, queryTso.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	resp := Tso{}
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, http.StatusOK, w.Code)
}
