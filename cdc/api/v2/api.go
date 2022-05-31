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
	"github.com/pingcap/tiflow/cdc/api/middleware"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/pkg/upstream"
)

// OpenAPIV2 provides CDC v2 APIs
type OpenAPIV2 struct {
	capture *capture.Capture
	// use for unit test only
	testStatusProvider owner.StatusProvider
}

// NewOpenAPIV2 creates a new OpenAPIV2.
func NewOpenAPIV2(c *capture.Capture) OpenAPIV2 {
	return OpenAPIV2{capture: c}
}

// NewOpenAPIV2ForTest returns a OpenAPIV2 for test
func NewOpenAPIV2ForTest(c *capture.Capture, p owner.StatusProvider) OpenAPIV2 {
	return OpenAPIV2{capture: c, testStatusProvider: p}
}

func (h *OpenAPIV2) statusProvider() owner.StatusProvider {
	if h.testStatusProvider != nil {
		return h.testStatusProvider
	}
	return h.capture.StatusProvider()
}

// RegisterOpenAPIV2Routes registers routes for OpenAPI
func RegisterOpenAPIV2Routes(router *gin.Engine, api OpenAPIV2) {
	v2 := router.Group("/api/v2")

	v2.Use(middleware.LogMiddleware())
	v2.Use(middleware.ErrorHandleMiddleware())

	// common APIs
	v2.GET("/tso", api.GetTso)
}

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
