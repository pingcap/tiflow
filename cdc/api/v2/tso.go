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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/upstream"
)

// GetTso request and returns a TSO from PD
func (h *OpenAPIV2) GetTso(c *gin.Context) {
	ctx := c.Request.Context()
	if h.capture.UpstreamManager == nil {
		c.Status(http.StatusServiceUnavailable)
		return
	}
	pdClient := h.capture.UpstreamManager.Get(upstream.DefaultUpstreamID).PDClient
	if pdClient == nil {
		c.Status(http.StatusServiceUnavailable)
		return
	}

	timestamp, logicalTime, err := pdClient.GetTS(ctx)
	if err != nil {
		_ = c.Error(err)
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}

	resp := Tso{timestamp, logicalTime}
	c.IndentedJSON(http.StatusOK, resp)
}
