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

// registerRoutes registers the routes for the HTTP server.
func registerRoutes(router *http.ServeMux, grpcMux *runtime.ServeMux, forwardJobAPI http.HandlerFunc) {
	// Swagger UI
	router.HandleFunc("/swagger", openapi.SwaggerUI)
	router.HandleFunc("/swagger/v1/openapiv2.json", openapi.SwaggerAPIv1)

	// Job API
	// There are two types of job API:
	// 1. The job API implemented by the framework.
	// 2. The job API implemented by the job master.
	// Both of them are registered in the same "/api/v1/jobs/" path.
	// The job API implemented by the job master is registered in the "/api/v1/jobs/{job_id}/".
	// But framework has a special APIs cancel will register in the "/api/v1/jobs/{job_id}/" path too.
	// So we first check whether the request should be forwarded to the job master.
	// If yes, forward the request to the job master. Otherwise, delegate the request to the framework.
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

// shouldForwardJobAPI indicates whether the request should be forwarded to the job master.
func shouldForwardJobAPI(r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, openapi.JobAPIPrefix) {
		return false
	}
	apiPath := strings.TrimPrefix(r.URL.Path, openapi.JobAPIPrefix)
	fields := strings.SplitN(apiPath, "/", 2)
	if len(fields) != 2 {
		return false
	}
	// cancel is implemented by framework,
	// don't forward them to the job master.
	if fields[1] == "cancel" {
		return false
	}
	return true
}
