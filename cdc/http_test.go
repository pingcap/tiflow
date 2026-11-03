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
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/cdc/api/authorization"
	"github.com/pingcap/tiflow/cdc/api/middleware"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	url    string
	method string
}

func TestAPIAccessCoverage(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	cfg := config.GetDefaultServerConfig()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"admin"}
	config.StoreGlobalServerConfig(cfg)
	router := gin.New()
	// Stop after access control: no real owner, PD, KV or etcd operations may run.
	router.Use(middleware.AccessMiddleware(nil), func(c *gin.Context) { c.AbortWithStatus(http.StatusNoContent) })
	RegisterRoutes(router, nil, nil)
	router.GET("/unclassified", func(c *gin.Context) { t.Fatal("unclassified handler executed") })
	for _, route := range router.Routes() {
		t.Run(route.Method+" "+route.Path, func(t *testing.T) {
			path := strings.NewReplacer(":changefeed_id", "orders", ":capture_id", "capture", ":any", "goroutine", "*any", "example").Replace(route.Path)
			req := httptest.NewRequest(route.Method, path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			public := route.Method == http.MethodGet && (path == "/status" || strings.HasSuffix(path, "/health") || strings.HasSuffix(path, "/status"))
			// A changefeed's status endpoint is a resource, not a public probe.
			public = public && !strings.Contains(path, "/changefeeds/")
			if public {
				require.Equal(t, http.StatusNoContent, w.Code)
			} else {
				require.Equal(t, http.StatusUnauthorized, w.Code)
			}

			// A legacy allow-listed administrator still needs a registered route.
			ctx := authorization.WithPrincipal(req.Context(), authorization.Principal{Username: "admin", UpstreamID: 42})
			w = httptest.NewRecorder()
			router.ServeHTTP(w, req.WithContext(ctx))
			if path == "/unclassified" || strings.HasPrefix(path, "/debug/fail/") || (path == "/metrics" && route.Method != http.MethodGet) {
				require.Equal(t, http.StatusForbidden, w.Code)
			} else {
				require.Equal(t, http.StatusNoContent, w.Code)
			}
		})
	}
}

func TestAPIScopedAccess(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	cfg := config.GetDefaultServerConfig()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"reader", "writer", "admin"}
	cfg.Security.ClientAuthorization = &security.ClientAuthorization{Bindings: []security.ClientRoleBinding{
		{User: "reader", Role: security.RoleViewer, Namespace: "default", Changefeed: "orders"},
		{User: "writer", Role: security.RoleOperator, Namespace: "team-a", Changefeed: "*"},
		{User: "admin", Role: security.RoleAdmin},
	}}
	config.StoreGlobalServerConfig(cfg)
	router := gin.New()
	router.Use(middleware.AccessMiddleware(nil), func(c *gin.Context) { c.AbortWithStatus(http.StatusNoContent) })
	RegisterRoutes(router, nil, nil)
	for _, tc := range []struct {
		user, method, path string
		status             int
	}{
		{"reader", "GET", "/api/v2/changefeeds/orders", 204},
		{"reader", "GET", "/api/v2/changefeeds/other", 403},
		{"reader", "GET", "/api/v2/changefeeds/orders?namespace=team-a", 403},
		{"reader", "DELETE", "/api/v1/changefeeds/orders", 403},
		{"reader", "GET", "/api/v2/changefeeds/orders/meta_info", 403},
		{"reader", "GET", "/config", 403},
		{"reader", "GET", "/metrics", 403},
		{"reader", "POST", "/capture/owner/admin", 403},
		{"writer", "DELETE", "/api/v2/changefeeds/orders?namespace=team-a", 204},
		{"writer", "DELETE", "/api/v2/changefeeds/orders?namespace=team-b", 403},
		{"writer", "DELETE", "/api/v1/changefeeds/orders?namespace=team-a", 403},
		{"writer", "DELETE", "/api/v2/unsafe/service_gc_safepoint", 403},
		{"writer", "POST", "/api/v2/unsafe/resolve_lock", 403},
		{"writer", "POST", "/api/v2/owner/resign", 403},
		{"writer", "POST", "/api/v2/tso", 204},
		{"writer", "POST", "/api/v2/verify_table", 204},
		{"admin", "DELETE", "/api/v2/unsafe/service_gc_safepoint", 204},
		{"admin", "POST", "/capture/owner/admin", 204},
		{"unbound", "GET", "/api/v2/changefeeds", 403},
	} {
		t.Run(tc.user+"/"+tc.method+"/"+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			// These headers must never grant authority, even for forwarded requests.
			req.Header.Set("X-TiCDC-Role", "admin")
			req.Header.Set("TiCDC-ForwardFromCapture", "trusted-capture")
			ctx := authorization.WithPrincipal(req.Context(), authorization.Principal{Username: tc.user, UpstreamID: 42})
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req.WithContext(ctx))
			require.Equal(t, tc.status, w.Code)
		})
	}
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
