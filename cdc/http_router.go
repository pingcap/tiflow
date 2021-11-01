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
	"context"
	"io/ioutil"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/cdc/capture"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	// use for OpenAPI online docs
	_ "github.com/pingcap/ticdc/docs/api"
)

// newRouter create a router for OpenAPI
func newRouter(captureHandler capture.HTTPHandler) *gin.Engine {
	// discard gin log output
	gin.DefaultWriter = ioutil.Discard

	router := gin.New()

	// request will timeout after 10 second
	router.Use(timeoutMiddleware(time.Second * 10))

	// OpenAPI online docs
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// common API
	router.GET("/api/v1/status", captureHandler.ServerStatus)
	router.GET("/api/v1/health", captureHandler.Health)
	router.POST("/api/v1/log", capture.SetLogLevel)

	// changefeed API
	changefeedGroup := router.Group("/api/v1/changefeeds")
	{
		changefeedGroup.GET("", captureHandler.ListChangefeed)
		changefeedGroup.GET("/:changefeed_id", captureHandler.GetChangefeed)
		changefeedGroup.POST("", captureHandler.CreateChangefeed)
		changefeedGroup.PUT("/:changefeed_id", captureHandler.UpdateChangefeed)
		changefeedGroup.POST("/:changefeed_id/pause", captureHandler.PauseChangefeed)
		changefeedGroup.POST("/:changefeed_id/resume", captureHandler.ResumeChangefeed)
		changefeedGroup.DELETE("/:changefeed_id", captureHandler.RemoveChangefeed)
		changefeedGroup.POST("/:changefeed_id/tables/rebalance_table", captureHandler.RebalanceTable)
		changefeedGroup.POST("/:changefeed_id/tables/move_table", captureHandler.MoveTable)
	}

	// owner API
	ownerGroup := router.Group("/api/v1/owner")
	{
		ownerGroup.POST("/resign", captureHandler.ResignOwner)
	}

	// processor API
	processorGroup := router.Group("/api/v1/processors")
	{
		processorGroup.GET("", captureHandler.ListProcessor)
		processorGroup.GET("/:changefeed_id/:capture_id", captureHandler.GetProcessor)
	}

	// capture API
	captureGroup := router.Group("/api/v1/captures")
	{
		captureGroup.GET("", captureHandler.ListCapture)
	}

	// pprof debug API
	pprofGroup := router.Group("/debug/pprof/")
	{
		pprofGroup.GET("", gin.WrapF(pprof.Index))
		pprofGroup.GET("/:any", gin.WrapF(pprof.Index))
		pprofGroup.GET("/cmdline", gin.WrapF(pprof.Cmdline))
		pprofGroup.GET("/profile", gin.WrapF(pprof.Profile))
		pprofGroup.GET("/symbol", gin.WrapF(pprof.Symbol))
		pprofGroup.GET("/trace", gin.WrapF(pprof.Trace))
		pprofGroup.GET("/threadcreate", gin.WrapF(pprof.Handler("threadcreate").ServeHTTP))
	}

	return router
}

// timeoutMiddleware wraps the request context with a timeout
func timeoutMiddleware(timeout time.Duration) func(c *gin.Context) {
	return func(c *gin.Context) {
		// wrap the request context with a timeout
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)

		defer func() {
			// check if context timeout was reached
			if ctx.Err() == context.DeadlineExceeded {

				// write response and abort the request
				c.Writer.WriteHeader(http.StatusGatewayTimeout)
				c.Abort()
			}

			// cancel to clear resources after finished
			cancel()
		}()

		// replace request with context wrapped request
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
