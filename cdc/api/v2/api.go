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
	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/api/middleware"
	"github.com/pingcap/tiflow/cdc/capture"
)

// OpenAPIV2 provides CDC v2 APIs
type OpenAPIV2 struct {
	capture *capture.Capture
}

// NewOpenAPI creates a new openAPIs.
func NewOpenAPI(c *capture.Capture) *OpenAPIV2 {
	return &OpenAPIV2{capture: c}
}

// RegisterOpenAPIRoutes registers routes for OpenAPI
func RegisterOpenAPIRoutes(router *gin.Engine, api *OpenAPIV2) {
	v2 := router.Group("/api/v2")

	v2.Use(middleware.LogMiddleware())
	v2.Use(middleware.ErrorHandleMiddleware())

	v2.POST("/verify-table", api.VerifyTable)
}
