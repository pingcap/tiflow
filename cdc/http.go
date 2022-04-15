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

package cdc

import (
	"net/http"
	"net/http/pprof"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	// use for OpenAPI online docs
	_ "github.com/pingcap/tiflow/docs/swagger"
)

// RegisterRoutes create a router for OpenAPI
func RegisterRoutes(
	router *gin.Engine,
	capture *capture.Capture,
	registry prometheus.Gatherer,
) {
	// online docs
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// Open API
	api.RegisterOpenAPIRoutes(router, api.NewOpenAPI(capture))

	// Owner API
	api.RegisterOwnerAPIRoutes(router, capture)

	// Status API
	api.RegisterStatusAPIRoutes(router, capture)

	// Log API
	router.POST("/admin/log", gin.WrapF(api.HandleAdminLogLevel))

	// pprof debug API
	pprofGroup := router.Group("/debug/pprof/")
	pprofGroup.GET("", gin.WrapF(pprof.Index))
	pprofGroup.GET("/:any", gin.WrapF(pprof.Index))
	pprofGroup.GET("/cmdline", gin.WrapF(pprof.Cmdline))
	pprofGroup.GET("/profile", gin.WrapF(pprof.Profile))
	pprofGroup.GET("/symbol", gin.WrapF(pprof.Symbol))
	pprofGroup.GET("/trace", gin.WrapF(pprof.Trace))
	pprofGroup.GET("/threadcreate", gin.WrapF(pprof.Handler("threadcreate").ServeHTTP))

	// Failpoint API
	if util.FailpointBuild {
		// `http.StripPrefix` is needed because `failpoint.HttpHandler` assumes that it handles the prefix `/`.
		router.Any("/debug/fail/*any", gin.WrapH(http.StripPrefix("/debug/fail", &failpoint.HttpHandler{})))
	}

	// Promtheus metrics API
	prometheus.DefaultGatherer = registry
	router.Any("/metrics", gin.WrapH(promhttp.Handler()))
}
