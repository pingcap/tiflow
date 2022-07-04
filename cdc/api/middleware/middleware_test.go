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
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/stretchr/testify/require"
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
