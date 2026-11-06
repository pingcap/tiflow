// Copyright 2026 PingCAP, Inc.
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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/cdc/api/authorization"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestScopedCollectionsAndCreation(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	cfg := config.GetDefaultServerConfig()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"reader", "writer"}
	cfg.Security.ClientAuthorization = &security.ClientAuthorization{Bindings: []security.ClientRoleBinding{
		{User: "reader", Role: security.RoleViewer, Namespace: "default", Changefeed: "orders"},
		{User: "writer", Role: security.RoleOperator, Namespace: "team-a", Changefeed: "orders"},
	}}
	config.StoreGlobalServerConfig(cfg)
	ctrl := gomock.NewController(t)
	cp := mock_capture.NewMockCapture(ctrl)
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	provider := mock_owner.NewMockStatusProvider(ctrl)
	cp.EXPECT().StatusProvider().Return(provider).AnyTimes()
	allowed := model.DefaultChangeFeedID("orders")
	other := model.DefaultChangeFeedID("other")
	foreign := model.ChangeFeedID{Namespace: "team-a", ID: "orders"}
	provider.EXPECT().GetAllChangeFeedCheckpointTs(gomock.Any()).Return(map[model.ChangeFeedID]uint64{}, nil)
	provider.EXPECT().GetAllChangeFeedInfo(gomock.Any()).Return(map[model.ChangeFeedID]*model.ChangeFeedInfo{
		allowed: {ID: allowed.ID, Namespace: allowed.Namespace},
		other:   {ID: other.ID, Namespace: other.Namespace},
		foreign: {ID: foreign.ID, Namespace: foreign.Namespace},
	}, nil)
	provider.EXPECT().GetProcessors(gomock.Any()).Return([]*model.ProcInfoSnap{
		{CfID: allowed, CaptureID: "capture"},
		{CfID: other, CaptureID: "capture"},
		{CfID: foreign, CaptureID: "capture"},
	}, nil)
	router := newRouter(NewOpenAPIV2(cp))
	request := func(user, method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		ctx := authorization.WithPrincipal(req.Context(), authorization.Principal{Username: user, UpstreamID: 42})
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req.WithContext(ctx))
		return w
	}
	for _, path := range []string{"/api/v2/changefeeds", "/api/v2/processors"} {
		w := request("reader", http.MethodGet, path, "")
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var payload any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &payload))
		response := payload.(map[string]any)
		require.EqualValues(t, 1, response["total"])
		items := response["items"].([]any)
		require.Len(t, items, 1)
		require.Contains(t, w.Body.String(), "orders")
		require.NotContains(t, w.Body.String(), "other")
		require.NotContains(t, w.Body.String(), "team-a")
	}
	// Mocks deliberately expose no upstream/owner methods: denied requests must
	// not touch PD, KV, etcd, or enqueue an owner job.
	for _, body := range []string{
		`{"namespace":"team-b","changefeed_id":"orders","sink_uri":"blackhole://"}`,
		`{"namespace":"team-a","changefeed_id":"other","sink_uri":"blackhole://"}`,
		`{"changefeed_id":"orders","sink_uri":"blackhole://"}`,
	} {
		w := request("writer", http.MethodPost, "/api/v2/changefeeds", body)
		require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	}
	w := request("reader", http.MethodDelete, "/api/v2/changefeeds/orders", "")
	require.Equal(t, http.StatusForbidden, w.Code)
}

func TestViewerDoesNotReceiveCredentials(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	cfg := config.GetDefaultServerConfig()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"reader", "writer"}
	cfg.Security.ClientAuthorization = &security.ClientAuthorization{Bindings: []security.ClientRoleBinding{
		{User: "reader", Role: security.RoleViewer, Namespace: "default", Changefeed: "orders"},
		{User: "writer", Role: security.RoleOperator, Namespace: "default", Changefeed: "orders"},
	}}
	config.StoreGlobalServerConfig(cfg)
	secret := "test-config-secret"
	replica := config.GetDefaultReplicaConfig()
	replica.Sink.KafkaConfig = &config.KafkaConfig{SASLPassword: &secret}
	info := &model.ChangeFeedInfo{
		ID: "orders", Namespace: "default", State: model.StateStopped,
		SinkURI: "mysql://user:test-user-secret@localhost/db?token=test-uri-secret", Config: replica,
	}
	ctrl := gomock.NewController(t)
	cp := mock_capture.NewMockCapture(ctrl)
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	provider := mock_owner.NewMockStatusProvider(ctrl)
	cp.EXPECT().StatusProvider().Return(provider).AnyTimes()
	provider.EXPECT().GetChangeFeedInfo(gomock.Any(), model.DefaultChangeFeedID("orders")).Return(info, nil).AnyTimes()
	provider.EXPECT().GetChangeFeedStatus(gomock.Any(), model.DefaultChangeFeedID("orders")).Return(&model.ChangeFeedStatusForAPI{}, nil).AnyTimes()
	router := newRouter(NewOpenAPIV2(cp))
	for _, user := range []string{"reader", "writer"} {
		req := httptest.NewRequest(http.MethodGet, "/api/v2/changefeeds/orders", nil)
		ctx := authorization.WithPrincipal(req.Context(), authorization.Principal{Username: user, UpstreamID: 42})
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req.WithContext(ctx))
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotContains(t, w.Body.String(), "test-user-secret")
		if user == "reader" {
			require.NotContains(t, w.Body.String(), "test-uri-secret")
			require.NotContains(t, w.Body.String(), "test-config-secret")
			require.NotContains(t, w.Body.String(), `"config":`)
		}
	}
	require.Equal(t, secret, *info.Config.Sink.KafkaConfig.SASLPassword)
	require.Contains(t, info.SinkURI, "test-user-secret")
	require.Contains(t, info.SinkURI, "test-uri-secret")
}

func TestMetadataAccessAndCredentialMasking(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	for _, mode := range []string{"role bindings", "authentication only", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config.GetDefaultServerConfig()
			cfg.Security.ClientUserRequired = mode != "legacy"
			cfg.Security.ClientAllowedUser = []string{"reader", "writer", "admin", "unbound"}
			if mode == "role bindings" {
				cfg.Security.ClientAuthorization = &security.ClientAuthorization{Bindings: []security.ClientRoleBinding{
					{User: "reader", Role: security.RoleViewer, Namespace: "default", Changefeed: "orders"},
					{User: "writer", Role: security.RoleOperator, Namespace: "default", Changefeed: "orders"},
					{User: "admin", Role: security.RoleAdmin},
				}}
			}
			config.StoreGlobalServerConfig(cfg)
			for _, user := range []string{"", "reader", "writer", "admin", "unbound"} {
				t.Run("user="+user, func(t *testing.T) {
					status := http.StatusOK
					if mode != "legacy" && user == "" {
						status = http.StatusUnauthorized
					} else if mode == "role bindings" && user != "admin" {
						status = http.StatusForbidden
					}
					ctrl := gomock.NewController(t)
					cp := mock_capture.NewMockCapture(ctrl)
					cp.EXPECT().IsReady().Return(true).AnyTimes()
					uri := "mysql://user:dsn-secret@localhost:3306/?sasl-password=sasl-secret&token=token-secret"
					info := &model.ChangeFeedInfo{
						ID: "orders", Namespace: "default", State: model.StateStopped,
						SinkURI: uri, Config: config.GetDefaultReplicaConfig(),
					}
					if status == http.StatusOK {
						cp.EXPECT().IsOwner().Return(true)
						provider := mock_owner.NewMockStatusProvider(ctrl)
						cp.EXPECT().StatusProvider().Return(provider).Times(2)
						id := model.DefaultChangeFeedID("orders")
						provider.EXPECT().GetChangeFeedInfo(gomock.Any(), id).Return(info, nil)
						provider.EXPECT().GetChangeFeedStatus(gomock.Any(), id).Return(&model.ChangeFeedStatusForAPI{}, nil)
					}
					router := newRouter(NewOpenAPIV2(cp))
					req := httptest.NewRequest(http.MethodGet, "/api/v2/changefeeds/orders/meta_info", nil)
					req.Header.Set("X-TiCDC-Role", "admin")
					req.Header.Set("TiCDC-ForwardFromCapture", "trusted-capture")
					if user != "" {
						req = req.WithContext(authorization.WithPrincipal(req.Context(), authorization.Principal{
							Username: user, UpstreamID: 42,
						}))
					}
					w := httptest.NewRecorder()
					router.ServeHTTP(w, req)
					require.Equal(t, status, w.Code, w.Body.String())
					for _, secret := range []string{"dsn-secret", "sasl-secret", "token-secret"} {
						require.NotContains(t, w.Body.String(), secret)
					}
					if status == http.StatusOK {
						var response ChangeFeedInfo
						require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
						require.Contains(t, response.SinkURI, "xxxxx")
						require.NotNil(t, response.Config)
					}
					require.Equal(t, uri, info.SinkURI, "masking must not mutate stored credentials")
				})
			}
		})
	}
}
