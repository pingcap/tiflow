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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"

	"github.com/pingcap/ticdc/cdc/capture"
	"github.com/stretchr/testify/require"
)

func TestPProfPath(t *testing.T) {
	t.Parallel()
	conf := config.GetDefaultServerConfig()
	router := newRouter(capture.NewHTTPHandler(nil), conf)

	apis := []*openAPI{
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

type openAPI struct {
	url    string
	method string
}

func (a *openAPI) String() string {
	return fmt.Sprintf("%s:%s", a.method, a.url)
}
