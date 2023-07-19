// Copyright 2023 PingCAP, Inc.
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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/api/middleware"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// @Summary Check the health status of a TiCDC cluster
// @Description Check the health status of a TiCDC cluster
// @Tags common,v2
// @Produce json
// @Success 200 {object} EmptyResponse
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/health [get]
func (h *OpenAPIV2) health(c *gin.Context) {
	if !h.capture.IsController() {
		middleware.ForwardToControllerMiddleware(h.capture)(c)
		return
	}

	ctx := c.Request.Context()
	health, err := h.capture.StatusProvider().IsHealthy(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if !health {
		err = cerror.ErrClusterIsUnhealthy.FastGenByArgs()
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, &EmptyResponse{})
}
