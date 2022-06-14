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

// NewOpenAPIV2 creates a new OpenAPIV2.
func NewOpenAPIV2(c *capture.Capture) OpenAPIV2 {
	return OpenAPIV2{capture: c}
}

// RegisterOpenAPIV2Routes registers routes for OpenAPI
func RegisterOpenAPIV2Routes(router *gin.Engine, api OpenAPIV2) {
	v2 := router.Group("/api/v2")

	v2.Use(middleware.CheckServerReadyMiddleware(api.capture))
	v2.Use(middleware.LogMiddleware())
	v2.Use(middleware.ErrorHandleMiddleware())

	// changefeed apis
	changefeedGroup := v2.Group("/changefeeds")
	changefeedGroup.POST("", api.CreateChangefeed)
	changefeedGroup.PUT("/:changefeed_id", api.UpdateChangefeed)
	changefeedGroup.GET("/:changefeed_id/meta_info", api.GetChangeFeedMetaInfo)
	v2.POST("/verify_table", api.VerifyTable)

	// unsafe apis
	v2.GET("/unsafe/metadata", api.CDCMetaData)
	v2.POST("/unsafe/resolve_lock", api.ResolveLock)
	v2.DELETE("/unsafe/service_gc_safepoint", api.DeleteServiceGcSafePoint)

	// common APIs
	v2.GET("/tso", api.GetTso)
}
