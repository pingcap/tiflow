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

package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api/authorization"
	"github.com/pingcap/tiflow/cdc/capture"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type testCaptureInfoProvider struct {
	capture.Capture
	ready bool
}

func (c *testCaptureInfoProvider) IsReady() bool {
	return c.ready
}

func TestCheckServerReadyMiddleware(t *testing.T) {
	capture := &testCaptureInfoProvider{ready: false}
	router := gin.New()
	router.Use(CheckServerReadyMiddleware(capture))
	router.Use(LogMiddleware())
	router.Use(ErrorHandleMiddleware())
	router.GET("/test", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req, err := http.NewRequestWithContext(context.Background(),
		"GET", "/test", nil)
	require.Nil(t, err)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	capture.ready = true
	w = httptest.NewRecorder()
	req, err = http.NewRequestWithContext(context.Background(),
		"GET", "/test", nil)
	require.Nil(t, err)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestLogMiddlewareUsesVerifiedIdentity(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	t.Cleanup(log.ReplaceGlobals(zap.New(core), nil))
	for _, authenticated := range []bool{false, true} {
		router := gin.New()
		router.Use(LogMiddleware())
		router.POST("/test", func(c *gin.Context) {
			if authenticated {
				c.Request = c.Request.WithContext(authorization.WithPrincipal(c.Request.Context(), authorization.Principal{
					Username: "verified-user", UpstreamID: 42,
				}))
			}
			c.Status(http.StatusNoContent)
		})
		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		req.SetBasicAuth("claimed-user", "password-secret")
		logs.TakeAll()
		router.ServeHTTP(httptest.NewRecorder(), req)
		entries := logs.FilterMessage("cdc open api request").All()
		require.Len(t, entries, 1)
		fields := entries[0].ContextMap()
		require.Equal(t, authenticated, fields["authenticated"])
		if authenticated {
			require.Equal(t, "verified-user", fields["username"])
			require.Equal(t, uint64(42), fields["upstreamID"])
		} else {
			require.Empty(t, fields["username"])
			require.Equal(t, uint64(0), fields["upstreamID"])
		}
		require.NotContains(t, fields, "password")
		require.NotContains(t, fields, "Authorization")
	}
}

func TestAuthenticationFailureStatus(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
	}{
		{errors.ErrCredentialNotFound.GenWithStackByArgs("missing credentials"), http.StatusUnauthorized},
		{errors.ErrUnauthorized.GenWithStackByArgs("reader", "not allowed"), http.StatusUnauthorized},
		{&mysql.MySQLError{Number: 1045, Message: "access denied"}, http.StatusUnauthorized},
		{errors.ErrUpstreamNotFound, http.StatusServiceUnavailable},
		{context.DeadlineExceeded, http.StatusServiceUnavailable},
		{errors.New("backend details must not be returned"), http.StatusServiceUnavailable},
	} {
		router := gin.New()
		router.GET("/test", func(c *gin.Context) { abortAuthentication(c, tc.err) })
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.SetBasicAuth("reader", "password-secret")
		router.ServeHTTP(w, req)
		require.Equal(t, tc.status, w.Code)
		require.NotContains(t, w.Body.String(), "password-secret")
		require.NotContains(t, w.Body.String(), "backend details")
	}
}

func TestAuthenticationBackendUnavailable(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	cfg := config.GetDefaultServerConfig()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"reader"}
	config.StoreGlobalServerConfig(cfg)
	for _, useAccess := range []bool{false, true} {
		cp := mock_capture.NewMockCapture(gomock.NewController(t))
		cp.EXPECT().GetUpstreamManager().Return(nil, errors.New("upstream unavailable"))
		router := gin.New()
		if useAccess {
			router.Use(AccessMiddleware(cp))
		} else {
			router.Use(AuthenticateMiddleware(cp))
		}
		router.GET("/api/v2/changefeeds", func(c *gin.Context) { t.Fatal("handler executed without authentication") })
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v2/changefeeds", nil)
		req.SetBasicAuth("reader", "password-secret")
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
		require.Contains(t, w.Body.String(), "ErrServerIsNotReady")
	}
}
