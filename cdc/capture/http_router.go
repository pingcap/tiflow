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

package capture

import (
	"context"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"

	// use for OpenAPI online docs
	_ "github.com/pingcap/tiflow/api"
)

// NewRouter create a router for OpenAPI
func NewRouter(handler HTTPHandler) *gin.Engine {
	router := gin.New()

	router.Use(logMiddleware())
	// request will timeout after 10 second
	router.Use(timeoutMiddleware(time.Second * 10))
	router.Use(errorHandleMiddleware())

	// OpenAPI online docs
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// common API
	router.GET("/api/v1/status", handler.ServerStatus)
	router.GET("/api/v1/health", handler.Health)
	router.POST("/api/v1/log", SetLogLevel)

	// changefeed API
	changefeedGroup := router.Group("/api/v1/changefeeds")
	{
		changefeedGroup.GET("", handler.ListChangefeed)
		changefeedGroup.GET("/:changefeed_id", handler.GetChangefeed)
		changefeedGroup.POST("", handler.CreateChangefeed)
		changefeedGroup.PUT("/:changefeed_id", handler.UpdateChangefeed)
		changefeedGroup.POST("/:changefeed_id/pause", handler.PauseChangefeed)
		changefeedGroup.POST("/:changefeed_id/resume", handler.ResumeChangefeed)
		changefeedGroup.DELETE("/:changefeed_id", handler.RemoveChangefeed)
		changefeedGroup.POST("/:changefeed_id/tables/rebalance_table", handler.RebalanceTable)
		changefeedGroup.POST("/:changefeed_id/tables/move_table", handler.MoveTable)
	}

	// owner API
	ownerGroup := router.Group("/api/v1/owner")
	{
		ownerGroup.POST("/resign", handler.ResignOwner)
	}

	// processor API
	processorGroup := router.Group("/api/v1/processors")
	{
		processorGroup.GET("", handler.ListProcessor)
		processorGroup.GET("/:changefeed_id/:capture_id", handler.GetProcessor)
	}

	// capture API
	captureGroup := router.Group("/api/v1/captures")
	{
		captureGroup.GET("", handler.ListCapture)
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

	if util.FailpointBuild {
		// `http.StripPrefix` is needed because `failpoint.HttpHandler` assumes that it handles the prefix `/`.
		router.Any("/debug/fail/*any", gin.WrapH(http.StripPrefix("/debug/fail", &failpoint.HttpHandler{})))
	}

	prometheus.DefaultGatherer = cdc.Registry
	router.Any("/metrics", gin.WrapH(promhttp.Handler()))

	// old api
	router.GET("/status", gin.WrapF(handler.handleStatus))
	router.GET("/debug/info", gin.WrapF(handler.handleDebugInfo))
	router.POST("/capture/owner/resign", gin.WrapF(handler.handleResignOwner))
	router.POST("/capture/owner/admin", gin.WrapF(handler.handleChangefeedAdmin))
	router.POST("/capture/owner/rebalance_trigger", gin.WrapF(handler.handleRebalanceTrigger))
	router.POST("/capture/owner/move_table", gin.WrapF(handler.handleMoveTable))
	router.POST("/capture/owner/changefeed/query", gin.WrapF(handler.handleChangefeedQuery))
	router.POST("/admin/log", gin.WrapF(handleAdminLogLevel))

	return router
}

// timeoutMiddleware wraps the request context with a timeout
func timeoutMiddleware(timeout time.Duration) gin.HandlerFunc {
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

func logMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		c.Next()

		cost := time.Since(start)

		err := c.Errors.Last()
		var stdErr error
		if err != nil {
			stdErr = err.Err
		}
		// Do not log metrics related requests when there is no error
		if strings.Contains(path, "/metrics") && err == nil {
			return
		}
		log.Info(path,
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()),
			zap.Error(stdErr),
			zap.Duration("cost", cost),
		)
	}
}

func errorHandleMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		// because we will return immediately after an error occurs in http_handler
		// there wil be only one error in c.Errors
		lastError := c.Errors.Last()
		if lastError != nil {
			err := lastError.Err
			// put the error into response
			if IsHTTPBadRequestError(err) {
				c.IndentedJSON(http.StatusBadRequest, model.NewHTTPError(err))
			} else {
				c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
			}
			c.Abort()
			return
		}
	}
}
