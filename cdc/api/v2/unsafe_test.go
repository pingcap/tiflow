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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/api/authorization"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	mock_etcd "github.com/pingcap/tiflow/pkg/etcd/mock"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var withFuntion = func(context.Context, pd.Client) error { return nil }

func TestCDCMetaData(t *testing.T) {
	enableUnsafeAPITestAuthentication(t)

	metaData := testCase{url: "/api/v2/unsafe/metadata", method: "GET"}

	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)

	etcdClient := mock_etcd.NewMockCDCEtcdClient(gomock.NewController(t))
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()

	// case 1: failed
	etcdClient.EXPECT().GetAllCDCInfo(gomock.Any()).Return(nil, cerror.ErrPDEtcdAPIError).Times(1)

	w := httptest.NewRecorder()
	req := newUnsafeTestRequest(metaData.method, metaData.url, "")
	router.ServeHTTP(w, req)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrPDEtcdAPIError")
	require.Equal(t, http.StatusInternalServerError, w.Code)

	// case 2: success
	kvs := []*mvccpb.KeyValue{{
		Key:   []byte("/tidb/cdc/default/default/changefeed/info/orders"),
		Value: []byte(`{"sink-uri":"mysql://user:metadata-secret@host/db?token=token-secret"}`),
	}}
	etcdClient.EXPECT().GetAllCDCInfo(gomock.Any()).Return(kvs, nil).Times(1)

	w = httptest.NewRecorder()
	req = newUnsafeTestRequest(metaData.method, metaData.url, "")
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var resp []EtcdData
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, len(kvs), len(resp))
	for i, kv := range resp {
		require.Equal(t, string(kvs[i].Key), kv.Key)
		require.NotContains(t, kv.Value, "metadata-secret")
		require.NotContains(t, kv.Value, "token-secret")
	}
	require.Contains(t, string(kvs[0].Value), "metadata-secret")
}

func enableUnsafeAPITestAuthentication(t *testing.T) {
	t.Helper()
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	cfg := config.GetDefaultServerConfig()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"admin"}
	config.StoreGlobalServerConfig(cfg)
}

func newUnsafeTestRequest(method, path, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	return req.WithContext(authorization.WithPrincipal(req.Context(), authorization.Principal{Username: "admin"}))
}

func TestUnsafeAPIsWithoutUserAuthentication(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	for _, mode := range []struct {
		name       string
		credential *security.Credential
		tls, mtls  bool
	}{
		{name: "nil security"},
		{name: "authentication disabled", credential: &security.Credential{}},
		{name: "allow-list without authentication", credential: &security.Credential{ClientAllowedUser: []string{"admin"}}},
		{name: "TLS only", credential: &security.Credential{}, tls: true},
		{name: "mTLS only", credential: &security.Credential{}, tls: true, mtls: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			cfg := config.GetDefaultServerConfig()
			cfg.Security = mode.credential
			config.StoreGlobalServerConfig(cfg)
			ctrl := gomock.NewController(t)
			cp := mock_capture.NewMockCapture(ctrl)
			cp.EXPECT().IsReady().Return(true).Times(3)
			cp.EXPECT().IsOwner().Return(true).Times(3)
			etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
			cp.EXPECT().GetEtcdClient().Return(etcdClient).Times(2)
			etcdClient.EXPECT().GetAllCDCInfo(gomock.Any()).Return([]*mvccpb.KeyValue{{
				Key:   []byte("/tidb/cdc/default/team-a/changefeed/info/orders"),
				Value: []byte(`{"state":"finished","sink-uri":"mysql://user:legacy-secret@host"}`),
			}}, nil).Times(2)
			cp.EXPECT().GetUpstreamManager().Return(upstream.NewManager4Test(&mockPDClient{}), nil)
			etcdClient.EXPECT().GetGCServiceID().Return("test-service")
			server := httptest.NewUnstartedServer(newRouter(NewOpenAPIV2(cp)))
			if mode.tls {
				ca, err := security.NewCA()
				require.NoError(t, err)
				certPEM, keyPEM, err := ca.GenerateCerts("client")
				require.NoError(t, err)
				cert, err := tls.X509KeyPair(certPEM, keyPEM)
				require.NoError(t, err)
				roots := x509.NewCertPool()
				require.True(t, roots.AppendCertsFromPEM(ca.CAPEM))
				server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
				if mode.mtls {
					server.TLS.ClientAuth = tls.RequireAndVerifyClientCert
					server.TLS.ClientCAs = roots
				}
				server.StartTLS()
				clientTLS := server.Client().Transport.(*http.Transport).TLSClientConfig
				clientTLS.RootCAs = roots
				clientTLS.Certificates = []tls.Certificate{cert}
			} else {
				server.Start()
			}
			t.Cleanup(server.Close)
			for _, endpoint := range []struct {
				method, path string
				status       int
			}{
				{http.MethodGet, "/api/v2/unsafe/metadata", http.StatusOK},
				// Invalid input reaches handler validation instead of an auth gate.
				{http.MethodPost, "/api/v2/unsafe/resolve_lock", http.StatusBadRequest},
				{http.MethodDelete, "/api/v2/unsafe/service_gc_safepoint", http.StatusOK},
			} {
				req, err := http.NewRequestWithContext(context.Background(), endpoint.method,
					server.URL+endpoint.path, strings.NewReader(`{}`))
				require.NoError(t, err)
				resp, err := server.Client().Do(req)
				require.NoError(t, err)
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, resp.Body.Close())
				require.NoError(t, err)
				require.Equal(t, endpoint.status, resp.StatusCode, string(body))
				require.NotContains(t, string(body), "legacy-secret")
			}
		})
	}
}

func TestDeleteServiceGcSafePointProtectsChangefeeds(t *testing.T) {
	enableUnsafeAPITestAuthentication(t)
	for _, state := range []model.FeedState{
		model.StateNormal, model.StateStopped, model.StatePending, model.StateWarning,
		model.StateFailed, model.StateFinished, model.StateRemoved, "unknown", "",
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cp := mock_capture.NewMockCapture(ctrl)
			cp.EXPECT().IsReady().Return(true)
			cp.EXPECT().IsOwner().Return(true)
			etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
			cp.EXPECT().GetEtcdClient().Return(etcdClient)
			// Non-default namespaces and upstreams must also be protected.
			id := model.ChangeFeedID{Namespace: "team-a", ID: "orders"}
			value, err := json.Marshal(&model.ChangeFeedInfo{
				ID: id.ID, Namespace: id.Namespace, UpstreamID: 42, State: state,
			})
			require.NoError(t, err)
			etcdClient.EXPECT().GetAllCDCInfo(gomock.Any()).Return([]*mvccpb.KeyValue{{
				Key: []byte("/tidb/cdc/default/team-a/changefeed/info/orders"), Value: value,
			}}, nil)
			helpers := NewMockAPIV2Helpers(ctrl)
			allowed := state == model.StateFinished || state == model.StateRemoved
			deleted := false
			if allowed {
				client := &gc.MockPDClient{UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
					require.Equal(t, "test-service", serviceID)
					require.Zero(t, ttl)
					require.Equal(t, uint64(math.MaxUint64), safePoint)
					deleted = true
					return 0, nil
				}}
				helpers.EXPECT().getPDClient(gomock.Any(), []string{"http://pd:2379"}, gomock.Any()).Return(client, nil)
				etcdClient.EXPECT().GetGCServiceID().Return("test-service")
			}
			router := newRouter(NewOpenAPIV2ForTest(cp, helpers))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, newUnsafeTestRequest(http.MethodDelete, "/api/v2/unsafe/service_gc_safepoint", `{"pd_addrs":["http://pd:2379"]}`))
			if allowed {
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			} else {
				require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
			}
			require.Equal(t, allowed, deleted)
		})
	}
}

func TestDeleteServiceGcSafePointMetadataFailure(t *testing.T) {
	enableUnsafeAPITestAuthentication(t)
	for _, readError := range []error{nil, cerror.ErrPDEtcdAPIError} {
		ctrl := gomock.NewController(t)
		cp := mock_capture.NewMockCapture(ctrl)
		cp.EXPECT().IsReady().Return(true)
		cp.EXPECT().IsOwner().Return(true)
		etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
		cp.EXPECT().GetEtcdClient().Return(etcdClient)
		etcdClient.EXPECT().GetAllCDCInfo(gomock.Any()).Return([]*mvccpb.KeyValue{{
			Key: []byte("/tidb/cdc/default/team-a/changefeed/info/incomplete"),
		}}, readError)
		router := newRouter(NewOpenAPIV2(cp))
		w := httptest.NewRecorder()
		router.ServeHTTP(w, newUnsafeTestRequest(http.MethodDelete, "/api/v2/unsafe/service_gc_safepoint", `{}`))
		if readError == nil {
			require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		} else {
			require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
		}
	}
}

func TestDeleteServiceGcSafePointEmptyCluster(t *testing.T) {
	enableUnsafeAPITestAuthentication(t)
	ctrl := gomock.NewController(t)
	cp := mock_capture.NewMockCapture(ctrl)
	cp.EXPECT().IsReady().Return(true)
	cp.EXPECT().IsOwner().Return(true)
	etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	cp.EXPECT().GetEtcdClient().Return(etcdClient)
	etcdClient.EXPECT().GetAllCDCInfo(gomock.Any()).Return([]*mvccpb.KeyValue{{
		Key: []byte("/tidb/cdc/default/__cdc_meta__/meta/meta-version"), Value: []byte("42"),
	}}, nil)
	etcdClient.EXPECT().GetGCServiceID().Return("empty-cluster")
	deleted := false
	client := &gc.MockPDClient{UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		require.Equal(t, "empty-cluster", serviceID)
		require.Zero(t, ttl)
		require.Equal(t, uint64(math.MaxUint64), safePoint)
		deleted = true
		return 0, nil
	}}
	cp.EXPECT().GetUpstreamManager().Return(upstream.NewManager4Test(client), nil)
	router := newRouter(NewOpenAPIV2(cp))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, newUnsafeTestRequest(http.MethodDelete, "/api/v2/unsafe/service_gc_safepoint", `{}`))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.True(t, deleted)
}

func TestWithUpstreamConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upManager := upstream.NewManager(ctx, upstream.CaptureTopologyCfg{GCServiceID: "abc"})
	upManager.AddUpstream(&model.UpstreamInfo{
		ID:          uint64(1),
		PDEndpoints: "http://127.0.0.1:22379",
	})
	cpCtrl := gomock.NewController(t)
	cp := mock_capture.NewMockCapture(cpCtrl)
	hpCtrl := gomock.NewController(t)
	helpers := NewMockAPIV2Helpers(hpCtrl)

	cp.EXPECT().GetUpstreamManager().Return(upManager, nil).AnyTimes()
	api := NewOpenAPIV2ForTest(cp, helpers)

	// upStream ID > 0 , getUpstream config ok
	upstreamConfig := &UpstreamConfig{
		ID: 1,
	}
	err := api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.Nil(t, err)

	// upStream ID > 0, getUpstream config not ok
	upstreamConfig = &UpstreamConfig{
		ID: 2,
	}
	err = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.NotNil(t, err)

	// upStreamConfig.ID = 0, len(pdAddress) > 0 : failed to getPDClient
	upstreamConfig = &UpstreamConfig{
		PDConfig: PDConfig{
			PDAddrs: []string{"http://127.0.0.1:22379"},
		},
	}
	helpers.EXPECT().
		getPDClient(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&mockPDClient{}, nil)
	err = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.Nil(t, err)

	// upStreamConfig.ID = 0, len(pdAddress) > 0, get PDClient succeed
	upstreamConfig = &UpstreamConfig{
		PDConfig: PDConfig{
			PDAddrs: []string{"http://127.0.0.1:22379"},
		},
	}
	helpers.EXPECT().
		getPDClient(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&mockPDClient{}, errors.New("getPDClient failed"))
	err = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.NotNil(t, err)

	// upStreamConfig.ID = 0, len(pdAddress) > 0, no default upstream
	upstreamConfig = &UpstreamConfig{}
	err = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.NotNil(t, err)

	// success
	upManager = upstream.NewManager4Test(&mockPDClient{})
	cp = mock_capture.NewMockCapture(gomock.NewController(t))
	cp.EXPECT().GetUpstreamManager().Return(upManager, nil).AnyTimes()
	api = NewOpenAPIV2ForTest(cp, helpers)
	upstreamConfig = &UpstreamConfig{}
	err = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.Nil(t, err)
}
