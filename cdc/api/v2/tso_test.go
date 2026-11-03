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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/cdc/api/authorization"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
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

func TestViewerTSOScope(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	cfg := config.GetDefaultServerConfig()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"reader"}
	cfg.Security.ClientAuthorization = &security.ClientAuthorization{Bindings: []security.ClientRoleBinding{
		{User: "reader", Role: security.RoleViewer, Namespace: "team-a", Changefeed: "orders"},
	}}
	config.StoreGlobalServerConfig(cfg)
	for _, tc := range []struct {
		name, body string
		owned      bool
		status     int
	}{
		{"default upstream", `{}`, true, http.StatusOK},
		{"explicit default", `{"id":0}`, true, http.StatusOK},
		{"foreign upstream", `{"id":7}`, true, http.StatusForbidden},
		{"foreign resource", `{}`, false, http.StatusForbidden},
		{"custom PD", `{"pd_addrs":["http://untrusted:2379"]}`, false, http.StatusForbidden},
		{"custom TLS", `{"ca_path":"/etc/private.pem"}`, false, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cp := mock_capture.NewMockCapture(ctrl)
			cp.EXPECT().IsReady().Return(true).AnyTimes()
			cp.EXPECT().IsOwner().Return(true)
			if tc.name != "custom PD" && tc.name != "custom TLS" {
				provider := mock_owner.NewMockStatusProvider(ctrl)
				cp.EXPECT().StatusProvider().Return(provider)
				id := model.ChangeFeedID{Namespace: "team-a", ID: "orders"}
				if !tc.owned {
					id.Namespace = "team-b"
				}
				provider.EXPECT().GetAllChangeFeedInfo(gomock.Any()).Return(map[model.ChangeFeedID]*model.ChangeFeedInfo{
					id:                                   {UpstreamID: 0},
					{Namespace: "team-b", ID: "foreign"}: {UpstreamID: 7},
				}, nil)
			}
			if tc.status == http.StatusOK {
				cp.EXPECT().GetUpstreamManager().Return(upstream.NewManager4Test(&mockPDClient4Tso{}), nil)
			}
			// No PD client construction is permitted for viewer-supplied endpoints.
			router := newRouter(NewOpenAPIV2ForTest(cp, NewMockAPIV2Helpers(ctrl)))
			req := httptest.NewRequest(http.MethodPost, "/api/v2/tso", bytes.NewBufferString(tc.body))
			req = req.WithContext(authorization.WithPrincipal(req.Context(), authorization.Principal{Username: "reader"}))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, tc.status, w.Code, w.Body.String())
		})
	}
}

func TestViewerTSOForwarding(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	cfg := config.GetDefaultServerConfig()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"reader"}
	cfg.Security.ClientAuthorization = &security.ClientAuthorization{Bindings: []security.ClientRoleBinding{
		{User: "reader", Role: security.RoleViewer, Namespace: "team-a", Changefeed: "orders"},
	}}
	config.StoreGlobalServerConfig(cfg)
	type forwardedRequest struct{ body, auth string }
	forwarded := make(chan forwardedRequest, 1)
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		forwarded <- forwardedRequest{body: string(body), auth: r.Header.Get("Authorization")}
		_, _ = w.Write([]byte(`{"timestamp":123,"logic_time":0}`))
	}))
	defer owner.Close()
	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(false)
	cp.EXPECT().Info().Return(model.CaptureInfo{ID: "follower"}, nil)
	cp.EXPECT().GetOwnerCaptureInfo(gomock.Any()).Return(&model.CaptureInfo{
		ID: "owner", AdvertiseAddr: strings.TrimPrefix(owner.URL, "http://"),
	}, nil)
	router := newRouter(NewOpenAPIV2(cp))
	req := httptest.NewRequest(http.MethodPost, "/api/v2/tso", bytes.NewBufferString(`{"id":42}`))
	req.SetBasicAuth("reader", "secret")
	req = req.WithContext(authorization.WithPrincipal(req.Context(), authorization.Principal{Username: "reader"}))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	got := <-forwarded
	require.JSONEq(t, `{"id":42}`, got.body)
	require.Equal(t, req.Header.Get("Authorization"), got.auth)
}
