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
	"io"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/pingcap/ticdc/pkg/config"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/capture"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"

	// use for OpenAPI online docs
	_ "github.com/pingcap/ticdc/docs/api"
)

// newRouter create a router for OpenAPI
func newRouter(capture2 *capture.Capture, conf *config.ServerConfig) *gin.Engine {
	// discard gin default log output
	gin.DefaultWriter = io.Discard

	router := gin.New()
	if conf.LogHTTP {
		router.Use(logMiddleware())
	}
	// request will timeout after 10 second
	router.Use(timeoutMiddleware(time.Second * 10))
	router.Use(errorHandleMiddleware())

	captureHandler := capture.NewHTTPHandler(capture2)

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
	pprofGroup := router.Group("/debug/pprof")
	{
		pprofGroup.GET("", gin.WrapF(pprof.Index))
		pprofGroup.GET("/:any", gin.WrapF(pprof.Index))
		pprofGroup.GET("/cmdline", gin.WrapF(pprof.Cmdline))
		pprofGroup.GET("/profile", gin.WrapF(pprof.Profile))
		pprofGroup.GET("/symbol", gin.WrapF(pprof.Symbol))
		pprofGroup.GET("/trace", gin.WrapF(pprof.Trace))
	}

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

		var errMessage string
		err := c.Errors.Last()
		if err != nil {
			errMessage = errors.Trace(c.Errors.Last().Err).Error()
		}

		log.Info(path,
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()),
			zap.String("errors", errMessage),
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
			if cerror.IsHTTPBadRequestError(err) {
				c.IndentedJSON(http.StatusBadRequest, model.NewHTTPError(err))
			} else {
				c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
			}
			c.Abort()
			return
		}
	}
}
