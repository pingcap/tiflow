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

package openapi

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapLogger is a middleware and zap to provide an "access log" like logging for each request.
func ZapLogger(log *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		req := c.Request

		statusCode := c.Writer.Status()
		fields := []zapcore.Field{
			zap.Int("size", c.Writer.Size()),
			zap.String("host", req.Host),
			zap.Int("status", statusCode),
			zap.String("method", req.Method),
			zap.String("protocol", req.Proto),
			zap.String("remote_ip", c.Request.RemoteAddr),
			zap.String("user_agent", req.UserAgent()),
			zap.String("duration", time.Since(start).String()),
			zap.String("request", fmt.Sprintf("%s %s", req.Method, req.RequestURI)),
		}

		switch {
		case statusCode >= 500:
			if err := c.Errors.Last(); err != nil {
				log.With(zap.Error(err)).Error("Server error", fields...)
			} else {
				log.Error("Server error", fields...)
			}
		case statusCode >= 400:
			if err := c.Errors.Last(); err != nil {
				log.With(zap.Error(c.Errors.Last().Err)).Warn("Client error", fields...)
			} else {
				log.Warn("Client error", fields...)
			}
		case statusCode >= 300:
			log.Info("Redirection", fields...)
		default:
			log.Info("Success", fields...)
		}
	}
}
