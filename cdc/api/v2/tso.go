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

package v2

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/api/authorization"
	"github.com/pingcap/tiflow/cdc/api/middleware"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pd "github.com/tikv/pd/client"
)

// QueryTso request and returns a TSO from PD
func (h *OpenAPIV2) QueryTso(c *gin.Context) {
	ctx := c.Request.Context()
	// Forward before decoding JSON so the owner receives the original body.
	if !authorization.Can(ctx, authorization.InspectUpstream, model.ChangeFeedID{}) && !h.capture.IsOwner() {
		api.ForwardToOwner(c, h.capture)
		c.Abort()
		return
	}

	upstreamConfig := &UpstreamConfig{}
	if err := c.BindJSON(upstreamConfig); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	if !h.authorizeTSO(c, upstreamConfig) {
		return
	}
	resp := &Tso{}
	err := h.withUpstreamConfig(ctx, upstreamConfig,
		func(ctx context.Context, client pd.Client) error {
			timestamp, logicalTime, err := client.GetTS(ctx)
			if err != nil {
				return cerror.WrapError(cerror.ErrInternalServerError, err)
			}
			resp.LogicTime = logicalTime
			resp.Timestamp = timestamp
			return nil
		})
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, resp)
}

func (h *OpenAPIV2) authorizeTSO(c *gin.Context, upstreamConfig *UpstreamConfig) bool {
	ctx := c.Request.Context()
	if authorization.Can(ctx, authorization.InspectUpstream, model.ChangeFeedID{}) {
		return true
	}
	// Viewer access is limited to upstreams of readable changefeeds. It must
	// never construct a client from caller-supplied PD endpoints or TLS files.
	if len(upstreamConfig.PDAddrs) != 0 || upstreamConfig.CAPath != "" ||
		upstreamConfig.CertPath != "" || upstreamConfig.KeyPath != "" || len(upstreamConfig.CertAllowedCN) != 0 {
		return middleware.RequirePermission(c, authorization.InspectUpstream, model.ChangeFeedID{})
	}
	provider := h.capture.StatusProvider()
	if provider == nil {
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, model.NewHTTPError(cerror.ErrOwnerNotFound))
		return false
	}
	infos, err := provider.GetAllChangeFeedInfo(ctx)
	if err != nil {
		_ = c.Error(err)
		return false
	}
	upstreamID := upstreamConfig.ID
	if upstreamID == 0 {
		principal, _ := authorization.GetPrincipal(ctx)
		upstreamID = principal.UpstreamID
	}
	for id, info := range infos {
		if info != nil && info.UpstreamID == upstreamID && authorization.Can(ctx, authorization.Read, id) {
			return true
		}
	}
	return middleware.RequirePermission(c, authorization.InspectUpstream, model.ChangeFeedID{})
}
