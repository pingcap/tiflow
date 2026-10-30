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

package openapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/check"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var _ = check.Suite(&zapLoggerSuite{})

type zapLoggerSuite struct{}

func TestZapLogger(t *testing.T) {
	check.TestingT(t)
}

func (t *zapLoggerSuite) TestZapLogger(c *check.C) {
	r := gin.New()
	obs, logs := observer.New(zap.DebugLevel)
	logger := zap.New(obs)
	r.Use(ZapLogger(logger))
	r.GET("/something", func(c *gin.Context) {
		c.String(http.StatusOK, "")
	})

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/something", nil)
	r.ServeHTTP(res, req)

	logFields := logs.All()[0].ContextMap()
	c.Assert(logFields["method"], check.Equals, "GET")
	c.Assert(logFields["request"], check.Equals, "GET /something")
	c.Assert(logFields["status"], check.Equals, int64(200))
	c.Assert(logFields["duration"], check.NotNil)
	c.Assert(logFields["host"], check.NotNil)
	c.Assert(logFields["protocol"], check.NotNil)
	c.Assert(logFields["remote_ip"], check.NotNil)
	c.Assert(logFields["user_agent"], check.NotNil)
	c.Assert(logFields["request"], check.NotNil)
	c.Assert(logFields["error"], check.IsNil)
}

func TestAccessLogURLCredentials(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	router := gin.New()
	router.Use(ZapLogger(zap.New(core)))
	router.GET("/test", func(c *gin.Context) {
		require.Equal(t, "private-ak", c.Query("access-key"))
		_ = c.Error(errors.New("storage denied https://host/path?sig=private-signature"))
		c.Status(http.StatusBadRequest)
	})
	req := httptest.NewRequest(http.MethodGet, "/test?access-key=private-ak&unknown-auth-token=private-token&source_name=source-one", nil)
	router.ServeHTTP(httptest.NewRecorder(), req)
	require.Len(t, logs.All(), 1)
	data, err := json.Marshal(logs.All()[0].ContextMap())
	require.NoError(t, err)
	require.NotContains(t, string(data), "private-")
	require.NotContains(t, string(data), "source-one")
	require.Equal(t, "GET /test", logs.All()[0].ContextMap()["request"])
	require.Contains(t, req.RequestURI, "private-ak")
}
