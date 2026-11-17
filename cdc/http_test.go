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

package cdc

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

type authTestCapture struct{ capture.Capture }

func (*authTestCapture) IsReady() bool { return true }
func (*authTestCapture) IsOwner() bool { return true }

func TestManagementRoutesRequireAuthentication(t *testing.T) {
	previous := config.GetGlobalServerConfig()
	cfg := previous.Clone()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"allowed-user"}
	config.StoreGlobalServerConfig(cfg)
	defer config.StoreGlobalServerConfig(previous)

	router := gin.New()
	RegisterRoutes(router, &authTestCapture{}, nil)
	routes := []testCase{
		{url: "/config", method: http.MethodGet},
		{url: "/admin/log", method: http.MethodPost},
		{url: "/capture/owner/admin", method: http.MethodPost},
		{url: "/capture/owner/resign", method: http.MethodPost},
		{url: "/capture/owner/rebalance_trigger", method: http.MethodPost},
		{url: "/capture/owner/move_table", method: http.MethodPost},
		{url: "/capture/owner/changefeed/query", method: http.MethodPost},
		{url: "/api/v1/log", method: http.MethodPost},
		{url: "/api/v2/log", method: http.MethodPost},
		{url: "/api/v1/owner/resign", method: http.MethodPost},
		{url: "/api/v2/owner/resign", method: http.MethodPost},
		{url: "/api/v1/captures/drain", method: http.MethodPut},
		{url: "/api/v2/captures/capture-1/drain", method: http.MethodPost},
	}
	for _, route := range routes {
		w := httptest.NewRecorder()
		req, err := http.NewRequestWithContext(context.Background(), route.method, route.url, nil)
		require.NoError(t, err)
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusUnauthorized, w.Code, route.String())
	}
}

func TestServerConfigResponseRedactsSecrets(t *testing.T) {
	previous := config.GetGlobalServerConfig()
	cfg := previous.Clone()
	cfg.Security.ClientUserRequired = false
	cfg.Security.ClientAllowedUser = []string{"allowed-user-sentinel"}
	cfg.Security.KeyPath = "/private/key-sentinel"
	cfg.Debug.CDCV2.MetaStoreConfig.URI = "mysql://user:meta-password-sentinel@localhost:3306/cdc"
	cfg.DataDir = "/private/data-sentinel"
	config.StoreGlobalServerConfig(cfg)
	defer config.StoreGlobalServerConfig(previous)

	router := gin.New()
	RegisterRoutes(router, &authTestCapture{}, nil)
	w := httptest.NewRecorder()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/config", nil)
	require.NoError(t, err)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	for _, secret := range []string{"allowed-user-sentinel", "key-sentinel", "meta-password-sentinel", "data-sentinel"} {
		require.NotContains(t, w.Body.String(), secret)
	}
	require.Contains(t, cfg.Debug.CDCV2.MetaStoreConfig.URI, "meta-password-sentinel")
}

type testCase struct {
	url    string
	method string
}

func (a *testCase) String() string {
	return fmt.Sprintf("%s:%s", a.method, a.url)
}

func TestPProfPath(t *testing.T) {
	router := gin.New()
	RegisterRoutes(router, capture.NewCapture4Test(nil), nil)

	apis := []*testCase{
		{"/debug/pprof/", http.MethodGet},
		{"/debug/pprof/cmdline", http.MethodGet},
		{"/debug/pprof/symbol", http.MethodGet},
		// these two apis make will make ut slow
		//{"/debug/pprof/profile", http.MethodGet},
		//{"/debug/pprof/trace", http.MethodGet},
		{"/debug/pprof/threadcreate", http.MethodGet},
		{"/debug/pprof/allocs", http.MethodGet},
		{"/debug/pprof/block", http.MethodGet},
		{"/debug/pprof/goroutine?debug=1", http.MethodGet},
		{"/debug/pprof/mutex?debug=1", http.MethodGet},
	}
	for _, api := range apis {
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(context.Background(), api.method, api.url, nil)
		router.ServeHTTP(w, req)
		require.Equal(t, 200, w.Code, api.String())
	}
}

func TestHandleFailpoint(t *testing.T) {
	router := gin.New()
	RegisterRoutes(router, capture.NewCapture4Test(nil), nil)
	fp := "github.com/pingcap/tiflow/cdc/TestHandleFailpoint"
	uri := fmt.Sprintf("/debug/fail/%s", fp)
	body := bytes.NewReader([]byte("return(true)"))
	req, err := http.NewRequestWithContext(context.Background(), "PUT", uri, body)
	require.Nil(t, err)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.True(t, w.Code >= 200 && w.Code <= 300)

	failpointHit := false
	failpoint.Inject("TestHandleFailpoint", func() {
		failpointHit = true
	})
	require.True(t, failpointHit)

	req, err = http.NewRequestWithContext(context.Background(), "DELETE", uri, body)
	require.Nil(t, err)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.True(t, w.Code >= 200 && w.Code <= 300)

	failpointHit = false
	failpoint.Inject("TestHandleFailpoint", func() {
		failpointHit = true
	})
	require.False(t, failpointHit)
}
