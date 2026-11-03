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
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/api/authorization"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/upstream"
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
		principal, authenticated := authorization.GetPrincipal(c.Request.Context())
		fields := []zap.Field{
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()), zap.String("client-version", version),
			zap.String("username", principal.Username),
			zap.Bool("authenticated", authenticated),
			zap.Uint64("upstreamID", principal.UpstreamID),
			zap.Error(stdErr),
			zap.Duration("duration", cost),
		}
		if strings.HasPrefix(c.FullPath(), "/capture/owner/") {
			// The handler has parsed the form. Record only operation fields,
			// never credentials, Authorization headers or the complete body.
			fields = append(fields,
				zap.String("namespace", model.DefaultNamespace),
				zap.String("changefeed", c.Request.Form.Get(api.OpVarChangefeedID)),
				zap.String("adminJobType", c.Request.Form.Get(api.OpVarAdminJob)),
				zap.String("targetCapture", c.Request.Form.Get(api.OpVarTargetCaptureID)),
				zap.String("tableID", c.Request.Form.Get(api.OpVarTableID)))
		}
		log.Info("cdc open api request", fields...)
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

// ForwardToOwnerMiddleware forward a request to controller if current server
// is not controller, or handle it locally.
func ForwardToOwnerMiddleware(p capture.Capture) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		if !p.IsOwner() {
			api.ForwardToOwner(ctx, p)

			// Without calling Abort(), Gin will continue to process the next handler,
			// execute code which should only be run by the owner, and cause a panic.
			// See https://github.com/pingcap/tiflow/issues/5888
			ctx.Abort()
			return
		}
		ctx.Next()
	}
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

// AuthenticateMiddleware authenticates the request by query upstream TiDB.
func AuthenticateMiddleware(capture capture.Capture) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		if err := authenticate(ctx, capture); err != nil {
			abortAuthentication(ctx, err)
			return
		}
		ctx.Next()
	}
}

func abortAuthentication(ctx *gin.Context, err error) {
	status := http.StatusUnauthorized
	switch {
	case errors.ErrCredentialNotFound.Equal(err), errors.ErrUnauthorized.Equal(err):
	case errorutil.IsAccessDeniedError(err):
		username, _, _ := ctx.Request.BasicAuth()
		err = errors.ErrUnauthorized.GenWithStackByArgs(username, "invalid upstream credentials")
	default:
		status = http.StatusServiceUnavailable
		// Backend errors belong in server logs, not credential responses.
		err = errors.ErrServerIsNotReady
	}
	ctx.AbortWithStatusJSON(status, model.NewHTTPError(err))
}

func authenticate(ctx *gin.Context, capture capture.Capture) error {
	credential := config.GetGlobalServerConfig().Security
	if credential == nil || (!credential.ClientUserRequired && credential.ClientAuthorization == nil) {
		return nil
	}
	if !credential.ClientUserRequired {
		return errors.ErrUnauthorized.GenWithStackByArgs("", "client authorization requires user authentication")
	}
	if _, ok := authorization.GetPrincipal(ctx.Request.Context()); ok {
		return nil
	}
	username, _, ok := ctx.Request.BasicAuth()
	if !ok {
		return errors.ErrCredentialNotFound.GenWithStackByArgs("please specify the user and password via authorization header")
	}
	up, err := getUpstream(capture)
	if err != nil {
		return err
	}
	if err := verify(ctx, up); err != nil {
		return err
	}
	ctx.Request = ctx.Request.WithContext(authorization.WithPrincipal(ctx.Request.Context(), authorization.Principal{
		Username: username, UpstreamID: up.ID,
	}))
	return nil
}

func getUpstream(capture capture.Capture) (*upstream.Upstream, error) {
	m, err := capture.GetUpstreamManager()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return m.GetDefaultUpstream()
}

func verify(ctx *gin.Context, up *upstream.Upstream) error {
	// get the username and password from the authorization header
	username, password, ok := ctx.Request.BasicAuth()
	if !ok {
		errMsg := "please specify the user and password via authorization header"
		return errors.ErrCredentialNotFound.GenWithStackByArgs(errMsg)
	}

	allowed := false
	serverCfg := config.GetGlobalServerConfig()
	for _, user := range serverCfg.Security.ClientAllowedUser {
		if user == username {
			allowed = true
			break
		}
	}
	if !allowed {
		errMsg := "The user is not allowed."
		if username == "" {
			errMsg = "Empty username is not allowed."
		}
		return errors.ErrUnauthorized.GenWithStackByArgs(username, errMsg)
	}
	verifyCtx, cancel := context.WithTimeout(ctx.Request.Context(), 10*time.Second)
	defer cancel()
	if err := up.VerifyTiDBUser(verifyCtx, username, password); err != nil {
		return errors.Trace(err)
	}
	return nil
}
