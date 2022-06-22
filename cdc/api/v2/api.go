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

	"github.com/gin-gonic/gin"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/api/middleware"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/pkg/security"
	pd "github.com/tikv/pd/client"
)

type APIV2Helper interface {
	verifyCreateChangefeedConfig(
		context.Context,
		*ChangefeedConfig,
		pd.Client,
		owner.StatusProvider,
		string,
		tidbkv.Storage,
	) (*model.ChangeFeedInfo, error)

	getPDClient(context.Context, []string, *security.Credential) (pd.Client, error)

	verifyUpdateChangefeedConfig(context.Context, *ChangefeedConfig, *model.ChangeFeedInfo,
		*model.UpstreamInfo) (*model.ChangeFeedInfo, *model.UpstreamInfo, error)
}

type APIV2HelperImpl struct{}

// OpenAPIV2 provides CDC v2 APIs
type OpenAPIV2 struct {
	capture     api.CaptureInfoProvider
	apiV2Helper APIV2Helper
}

// NewOpenAPIV2 creates a new OpenAPIV2.
func NewOpenAPIV2(c *capture.Capture) OpenAPIV2 {
	return OpenAPIV2{c, &APIV2HelperImpl{}}
}

// NewOpenAPIV2ForTest creates a new OpenAPIV2.
//func NewOpenAPIV2ForTest(capture api.CaptureInfoProvider, stubs *mockStubs) OpenAPIV2 {
//	return OpenAPIV2{capture, stubs}
//}

// RegisterOpenAPIV2Routes registers routes for OpenAPI
func RegisterOpenAPIV2Routes(router *gin.Engine, api OpenAPIV2) {
	v2 := router.Group("/api/v2")

	v2.Use(middleware.CheckServerReadyMiddleware(api.capture))
	v2.Use(middleware.LogMiddleware())
	v2.Use(middleware.ErrorHandleMiddleware())

	// changefeed apis
	changefeedGroup := v2.Group("/changefeeds")
	changefeedGroup.Use(middleware.ForwardToOwnerMiddleware(api.capture))
	changefeedGroup.POST("", api.CreateChangefeed)
	changefeedGroup.PUT("/:changefeed_id", api.UpdateChangefeed)
	changefeedGroup.GET("/:changefeed_id/meta_info", api.GetChangeFeedMetaInfo)

	verifyTableGroup := v2.Group("/verify_table")
	verifyTableGroup.Use(middleware.ForwardToOwnerMiddleware(api.capture))
	verifyTableGroup.POST("", api.VerifyTable)

	// unsafe apis
	unsafeGroup := v2.Group("/unsafe")
	unsafeGroup.Use(middleware.ForwardToOwnerMiddleware(api.capture))
	unsafeGroup.GET("/unsafe/metadata", api.CDCMetaData)
	unsafeGroup.POST("/unsafe/resolve_lock", api.ResolveLock)
	unsafeGroup.DELETE("/unsafe/service_gc_safepoint", api.DeleteServiceGcSafePoint)

	// common APIs
	v2.POST("/tso", api.QueryTso)
}
