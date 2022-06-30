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
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pd "github.com/tikv/pd/client"
)

// QueryTso request and returns a TSO from PD
func (h *OpenAPIV2) QueryTso(c *gin.Context) {
	ctx := c.Request.Context()

	upstreamConfig := &UpstreamConfig{}
	if err := c.BindJSON(upstreamConfig); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
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
