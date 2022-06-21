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

package servermaster

import (
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestRegisterRoutes(t *testing.T) {
	router := gin.New()
	RegisterRoutes(router, NewOpenAPI(nil))

	routes := router.Routes()
	require.True(t, containsRoute(routes, "GET", "/swagger/*any"))
	require.True(t, containsRouteWithPrefix(routes, "/api/v1"))
	require.True(t, containsRoute(routes, "GET", "/debug/pprof/"))
	require.True(t, containsRoute(routes, "GET", "/debug/pprof/trace"))
	require.True(t, containsRoute(routes, "GET", "/debug/pprof/threadcreate"))
	require.True(t, containsRoute(routes, "GET", "/debug/pprof/cmdline"))
	require.True(t, containsRoute(routes, "GET", "/debug/pprof/profile"))
	require.True(t, containsRoute(routes, "GET", "/debug/pprof/symbol"))
	require.True(t, containsRoute(routes, "GET", "/debug/pprof/:any"))
	require.True(t, containsRouteWithAnyMethod(routes, "/metrics"))
}

func containsRoute(routes gin.RoutesInfo, method, path string) bool {
	for _, route := range routes {
		if route.Method == method && route.Path == path {
			return true
		}
	}
	return false
}

func containsRouteWithAnyMethod(routes gin.RoutesInfo, path string) bool {
	for _, route := range routes {
		if route.Path == path {
			return true
		}
	}
	return false
}

func containsRouteWithPrefix(routes gin.RoutesInfo, prefix string) bool {
	for _, route := range routes {
		if strings.HasPrefix(route.Path, prefix) {
			return true
		}
	}
	return false
}
