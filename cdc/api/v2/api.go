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
	capture capture.Capture
	helpers APIV2Helpers
}

// NewOpenAPIV2 creates a new OpenAPIV2.
func NewOpenAPIV2(c capture.Capture) OpenAPIV2 {
	return OpenAPIV2{c, APIV2HelpersImpl{}}
}

// NewOpenAPIV2ForTest creates a new OpenAPIV2.
func NewOpenAPIV2ForTest(c capture.Capture, h APIV2Helpers) OpenAPIV2 {
	return OpenAPIV2{c, h}
}

// RegisterOpenAPIV2Routes registers routes for OpenAPI
func RegisterOpenAPIV2Routes(router *gin.Engine, api OpenAPIV2) {
	v2 := router.Group("/api/v2")

	v2.Use(middleware.CheckServerReadyMiddleware(api.capture))
	v2.Use(middleware.LogMiddleware())
	v2.Use(middleware.ErrorHandleMiddleware())

	v2.GET("health", api.health)
	v2.GET("status", api.serverStatus)
	v2.POST("log", api.setLogLevel)

	ownerMiddleware := middleware.ForwardToOwnerMiddleware(api.capture)
	authenticateMiddleware := middleware.AuthenticateMiddleware(api.capture)

	// changefeed apis
	changefeedGroup := v2.Group("/changefeeds")
	changefeedGroup.GET("/:changefeed_id", ownerMiddleware, api.getChangeFeed)
	changefeedGroup.POST("", ownerMiddleware, authenticateMiddleware, api.createChangefeed)
	changefeedGroup.GET("", ownerMiddleware, api.listChangeFeeds)
	changefeedGroup.PUT("/:changefeed_id", ownerMiddleware, authenticateMiddleware, api.updateChangefeed)
	changefeedGroup.DELETE("/:changefeed_id", ownerMiddleware, authenticateMiddleware, api.deleteChangefeed)
	changefeedGroup.GET("/:changefeed_id/meta_info", ownerMiddleware, api.getChangeFeedMetaInfo)
	changefeedGroup.POST("/:changefeed_id/resume", ownerMiddleware, authenticateMiddleware, api.resumeChangefeed)
	changefeedGroup.POST("/:changefeed_id/pause", ownerMiddleware, authenticateMiddleware, api.pauseChangefeed)
	changefeedGroup.GET("/:changefeed_id/status", ownerMiddleware, api.status)
	changefeedGroup.GET("/:changefeed_id/synced", ownerMiddleware, api.synced)

	// capture apis
	captureGroup := v2.Group("/captures")
	captureGroup.Use(ownerMiddleware)
	captureGroup.POST("/:capture_id/drain", api.drainCapture)
	captureGroup.GET("", api.listCaptures)

	// processor apis
	processorGroup := v2.Group("/processors")
	processorGroup.GET("/:changefeed_id/:capture_id", ownerMiddleware, api.getProcessor)
	processorGroup.GET("", ownerMiddleware, api.listProcessors)

	verifyTableGroup := v2.Group("/verify_table")
	verifyTableGroup.POST("", api.verifyTable)

	// unsafe apis
	unsafeGroup := v2.Group("/unsafe")
	unsafeGroup.Use(ownerMiddleware)
	unsafeGroup.GET("/metadata", authenticateMiddleware, api.CDCMetaData)
	unsafeGroup.POST("/resolve_lock", authenticateMiddleware, api.ResolveLock)
	unsafeGroup.DELETE("/service_gc_safepoint", authenticateMiddleware, api.DeleteServiceGcSafePoint)

	// owner apis
	ownerGroup := v2.Group("/owner")
	unsafeGroup.Use(ownerMiddleware)
	ownerGroup.POST("/resign", api.resignOwner)

	// common APIs
	v2.POST("/tso", api.QueryTso)
}
