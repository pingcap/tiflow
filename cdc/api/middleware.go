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

package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

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

		log.Info(path,
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()),
			zap.Error(stdErr),
			zap.Duration("duration", cost),
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
