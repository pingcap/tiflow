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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/failpoint"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	url    string
	method string
}

func (a *testCase) String() string {
	return fmt.Sprintf("%s:%s", a.method, a.url)
}

func TestPProfPath(t *testing.T) {
	router := gin.New()
	RegisterRoutes(router, capture.NewCapture4Test(false), nil)

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
		req, _ := http.NewRequest(api.method, api.url, nil)
		router.ServeHTTP(w, req)
		require.Equal(t, 200, w.Code, api.String())
	}
}

func TestHandleFailpoint(t *testing.T) {
	router := gin.New()
	RegisterRoutes(router, capture.NewCapture4Test(false), nil)
	fp := "github.com/pingcap/tiflow/cdc/TestHandleFailpoint"
	uri := fmt.Sprintf("/debug/fail/%s", fp)
	body := bytes.NewReader([]byte("return(true)"))
	req, err := http.NewRequest("PUT", uri, body)
	require.Nil(t, err)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.True(t, w.Code >= 200 && w.Code <= 300)

	failpointHit := false
	failpoint.Inject("TestHandleFailpoint", func() {
		failpointHit = true
	})
	require.True(t, failpointHit)

	req, err = http.NewRequest("DELETE", uri, body)
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
