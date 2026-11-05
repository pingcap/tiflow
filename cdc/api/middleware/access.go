// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api/authorization"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// Public routes deliberately reveal only health and basic server identity.
var publicRoutes = map[string]bool{
	"GET /status":        true,
	"GET /api/v1/health": true,
	"GET /api/v1/status": true,
	"GET /api/v2/health": true,
	"GET /api/v2/status": true,
}

// Every protected route has an explicit permission. Unknown routes fail closed
// even for administrators. Resource identifiers always come from the same path
// and namespace defaults used by the handlers, never from identity headers.
var routePermissions = map[string]authorization.Action{
	"GET /api/v1/changefeeds":                                        authorization.List,
	"GET /api/v1/changefeeds/:changefeed_id":                         authorization.Read,
	"POST /api/v1/changefeeds":                                       authorization.Create,
	"PUT /api/v1/changefeeds/:changefeed_id":                         authorization.Write,
	"DELETE /api/v1/changefeeds/:changefeed_id":                      authorization.Write,
	"POST /api/v1/changefeeds/:changefeed_id/pause":                  authorization.Write,
	"POST /api/v1/changefeeds/:changefeed_id/resume":                 authorization.Write,
	"POST /api/v1/changefeeds/:changefeed_id/tables/rebalance_table": authorization.Write,
	"POST /api/v1/changefeeds/:changefeed_id/tables/move_table":      authorization.Write,
	"GET /api/v1/processors":                                         authorization.List,
	"GET /api/v1/processors/:changefeed_id/:capture_id":              authorization.Read,
	"GET /api/v1/captures":                                           authorization.Admin,
	"PUT /api/v1/captures/drain":                                     authorization.Admin,
	"POST /api/v1/owner/resign":                                      authorization.Admin,
	"POST /api/v1/log":                                               authorization.Admin,
	"GET /api/v2/changefeeds":                                        authorization.List,
	"GET /api/v2/changefeeds/:changefeed_id":                         authorization.Read,
	"POST /api/v2/changefeeds":                                       authorization.Create,
	"PUT /api/v2/changefeeds/:changefeed_id":                         authorization.Write,
	"DELETE /api/v2/changefeeds/:changefeed_id":                      authorization.Write,
	"POST /api/v2/changefeeds/:changefeed_id/pause":                  authorization.Write,
	"POST /api/v2/changefeeds/:changefeed_id/resume":                 authorization.Write,
	"GET /api/v2/changefeeds/:changefeed_id/status":                  authorization.Read,
	"GET /api/v2/changefeeds/:changefeed_id/synced":                  authorization.Read,
	"GET /api/v2/changefeeds/:changefeed_id/meta_info":               authorization.Admin,
	"GET /api/v2/processors":                                         authorization.List,
	"GET /api/v2/processors/:changefeed_id/:capture_id":              authorization.Read,
	"GET /api/v2/captures":                                           authorization.Admin,
	"POST /api/v2/captures/:capture_id/drain":                        authorization.Admin,
	"POST /api/v2/owner/resign":                                      authorization.Admin,
	"POST /api/v2/log":                                               authorization.Admin,
	"POST /api/v2/verify_table":                                      authorization.InspectUpstream,
	"POST /api/v2/tso":                                               authorization.ReadTSO,
	"GET /api/v2/unsafe/metadata":                                    authorization.Unsafe,
	"POST /api/v2/unsafe/resolve_lock":                               authorization.Unsafe,
	"DELETE /api/v2/unsafe/service_gc_safepoint":                     authorization.Unsafe,
	// The legacy form-based management API is restricted to cluster admins.
	"POST /capture/owner/resign":            authorization.Admin,
	"POST /capture/owner/admin":             authorization.Admin,
	"POST /capture/owner/rebalance_trigger": authorization.Admin,
	"POST /capture/owner/move_table":        authorization.Admin,
	"POST /capture/owner/changefeed/query":  authorization.Admin,
	"POST /admin/log":                       authorization.Admin,
	"GET /config":                           authorization.Admin,
	"GET /debug/info":                       authorization.Admin,
	"GET /debug/pprof/":                     authorization.Admin,
	"GET /debug/pprof/:any":                 authorization.Admin,
	"GET /debug/pprof/cmdline":              authorization.Admin,
	"GET /debug/pprof/profile":              authorization.Admin,
	"GET /debug/pprof/symbol":               authorization.Admin,
	"GET /debug/pprof/trace":                authorization.Admin,
	"GET /debug/pprof/threadcreate":         authorization.Admin,
	"GET /metrics":                          authorization.Metrics,
}

const accessCheckedKey = "ticdc-access-checked"

// AccessMiddleware enforces authentication and route permissions. It is also
// installed on standalone API groups; the marker avoids repeated checks when
// the full server installs it globally to protect future routes as well.
func AccessMiddleware(capture capture.Capture) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetBool(accessCheckedKey) {
			c.Next()
			return
		}
		key := c.Request.Method + " " + c.FullPath()
		credential := config.GetGlobalServerConfig().Security
		// Preserve the legacy authentication switch for all APIs, including unsafe
		// operations and deployments that authenticate clients only through mTLS.
		if credential == nil ||
			(!credential.ClientUserRequired && credential.ClientAuthorization == nil) {
			c.Next()
			return
		}
		if publicRoutes[key] {
			c.Set(accessCheckedKey, true)
			c.Next()
			return
		}
		if err := authenticate(c, capture); err != nil {
			log.Warn("cdc open api authentication failed", zap.String("path", c.Request.URL.Path), zap.Error(err))
			abortAuthentication(c, err)
			return
		}
		action, registered := routePermissions[key]
		resource := model.DefaultChangeFeedID(c.Param("changefeed_id"))
		if strings.HasPrefix(c.FullPath(), "/api/v2/") {
			if namespace := c.Query("namespace"); namespace != "" {
				resource.Namespace = namespace
			}
		}
		if !registered || !RequirePermission(c, action, resource) {
			if !registered {
				denyPermission(c, action, resource)
			}
			return
		}
		c.Set(accessCheckedKey, true)
		c.Next()
	}
}

// RequirePermission checks a body-derived resource before create side effects.
func RequirePermission(c *gin.Context, action authorization.Action, resource model.ChangeFeedID) bool {
	if authorization.Can(c.Request.Context(), action, resource) {
		return true
	}
	denyPermission(c, action, resource)
	return false
}

func denyPermission(c *gin.Context, action authorization.Action, resource model.ChangeFeedID) {
	principal, _ := authorization.GetPrincipal(c.Request.Context())
	err := errors.ErrForbidden.GenWithStackByArgs(principal.Username, string(action))
	log.Warn("cdc open api authorization failed", zap.String("username", principal.Username),
		zap.String("method", c.Request.Method), zap.String("path", c.Request.URL.Path),
		zap.String("namespace", resource.Namespace), zap.String("changefeed", resource.ID),
		zap.String("action", string(action)))
	c.AbortWithStatusJSON(http.StatusForbidden, model.NewHTTPError(err))
}
