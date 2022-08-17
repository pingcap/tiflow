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
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/engine/pkg/openapi"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/pkg/util"
)

// JobAPIPrefix is the prefix of the job API.
const JobAPIPrefix = "/api/v1/jobs/"

// RegisterRoutes create a router for OpenAPI
func RegisterRoutes(router *http.ServeMux, grpcMux *runtime.ServeMux, forwardJobAPI http.HandlerFunc) {
	// Swagger UI
	router.HandleFunc("/swagger", openapi.SwaggerUI)
	router.HandleFunc("/swagger/v1/openapiv2.json", openapi.SwaggerAPIv1)

	// Job API
	router.HandleFunc("/api/v1/", func(w http.ResponseWriter, r *http.Request) {
		if shouldForwardJobAPI(r) {
			forwardJobAPI(w, r)
		} else {
			grpcMux.ServeHTTP(w, r)
		}
	})

	// pprof debug API
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)
	router.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	// Failpoint API
	if util.FailpointBuild {
		// `http.StripPrefix` is needed because `failpoint.HttpHandler` assumes that it handles the prefix `/`.
		router.Handle("/debug/fail/", http.StripPrefix("/debug/fail", &failpoint.HttpHandler{}))
	}

	// Prometheus metrics API
	router.Handle("/metrics", promutil.HTTPHandlerForMetric())
}

func shouldForwardJobAPI(r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, JobAPIPrefix) {
		return false
	}
	apiPath := strings.TrimPrefix(r.URL.Path, JobAPIPrefix)
	fields := strings.SplitN(apiPath, "/", 2)
	if len(fields) != 2 {
		return false
	}
	// pause and cancel are implemented by framework,
	// don't forward them to the job master.
	if fields[1] == "pause" || fields[1] == "cancel" {
		return false
	}
	return true
}
