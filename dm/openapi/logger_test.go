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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestZapLogger(t *testing.T) {
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
	require.Equal(t, "GET", logFields["method"])
	require.Equal(t, "GET /something", logFields["request"])
	require.Equal(t, int64(200), logFields["status"])
	require.NotNil(t, logFields["duration"])
	require.NotNil(t, logFields["host"])
	require.NotNil(t, logFields["protocol"])
	require.NotNil(t, logFields["remote_ip"])
	require.NotNil(t, logFields["user_agent"])
	require.NotNil(t, logFields["request"])
	require.Nil(t, logFields["error"])
}
