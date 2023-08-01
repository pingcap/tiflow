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
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// ClientVersionHeader is the header name of client version
const ClientVersionHeader = "X-client-version"

// LogMiddleware logs the api requests
func LogMiddleware() gin.HandlerFunc {
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
		version := c.Request.Header.Get(ClientVersionHeader)
		log.Info(path,
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()), zap.String("client-version", version),
			zap.Error(stdErr),
			zap.Duration("duration", cost),
		)
	}
}

// ErrorHandleMiddleware puts the error into response
func ErrorHandleMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		// because we will return immediately after an error occurs in http_handler
		// there wil be only one error in c.Errors
		lastError := c.Errors.Last()
		if lastError != nil {
			err := lastError.Err
			// put the error into response
			if api.IsHTTPBadRequestError(err) {
				c.IndentedJSON(http.StatusBadRequest, model.NewHTTPError(err))
			} else {
				c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
			}
			c.Abort()
			return
		}
	}
}

// ForwardToControllerMiddleware forward a request to controller if current server
// is not controller, or handle it locally.
func ForwardToControllerMiddleware(p capture.Capture) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		if !p.IsController() {
			api.ForwardToController(ctx, p)

			// Without calling Abort(), Gin will continue to process the next handler,
			// execute code which should only be run by the owner, and cause a panic.
			// See https://github.com/pingcap/tiflow/issues/5888
			ctx.Abort()
			return
		}
		ctx.Next()
	}
}

// ForwardToChangefeedOwnerMiddleware forward a request to controller if current server
// is not the changefeed owner, or handle it locally.
func ForwardToChangefeedOwnerMiddleware(p capture.Capture,
	changefeedIDFunc func(ctx *gin.Context) model.ChangeFeedID,
) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		changefeedID := changefeedIDFunc(ctx)
		// check if this capture is the changefeed owner
		if handleRequestIfIsChnagefeedOwner(ctx, p, changefeedID) {
			return
		}

		// forward to the controller to find the changefeed owner capture
		if !p.IsController() {
			api.ForwardToController(ctx, p)
			// Without calling Abort(), Gin will continue to process the next handler,
			// execute code which should only be run by the owner, and cause a panic.
			// See https://github.com/pingcap/tiflow/issues/5888
			ctx.Abort()
			return
		}

		controller, err := p.GetController()
		if err != nil {
			_ = ctx.Error(err)
			ctx.Abort()
			return
		}
		// controller check if the changefeed is exists, so we don't need to forward again
		ok, err := controller.IsChangefeedExists(ctx, changefeedID)
		if err != nil {
			_ = ctx.Error(err)
			ctx.Abort()
			return
		}
		if !ok {
			_ = ctx.Error(cerror.ErrChangeFeedNotExists.GenWithStackByArgs(changefeedID))
			ctx.Abort()
			return
		}

		info, err := p.Info()
		if err != nil {
			_ = ctx.Error(err)
			ctx.Abort()
			return
		}
		changefeedCaptureOwner := controller.GetChangefeedOwnerCaptureInfo(changefeedID)
		if changefeedCaptureOwner.ID == info.ID {
			log.Warn("changefeed owner is the same as controller",
				zap.String("captureID", info.ID))
			return
		}
		api.ForwardToCapture(ctx, info.ID, changefeedCaptureOwner.AdvertiseAddr)
		ctx.Abort()
	}
}

func handleRequestIfIsChnagefeedOwner(ctx *gin.Context, p capture.Capture, changefeedID model.ChangeFeedID) bool {
	// currently not only controller capture has the owner, remove this check in the future
	if p.StatusProvider() != nil {
		ok, err := p.StatusProvider().IsChangefeedOwner(ctx, changefeedID)
		if err != nil {
			_ = ctx.Error(err)
			return true
		}
		// this capture is the changefeed owner's capture, handle this request directly
		if ok {
			ctx.Next()
			return true
		}
	}
	return false
}

// CheckServerReadyMiddleware checks if the server is ready
func CheckServerReadyMiddleware(capture capture.Capture) gin.HandlerFunc {
	return func(c *gin.Context) {
		if capture.IsReady() {
			c.Next()
		} else {
			c.IndentedJSON(http.StatusServiceUnavailable,
				model.NewHTTPError(errors.ErrServerIsNotReady))
			c.Abort()
			return
		}
	}
}
