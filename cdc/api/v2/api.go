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
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/api/middleware"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/upstream"
)

type mockStubs struct {
	testStatusProvider  owner.StatusProvider
	testEtcdClient      *etcd.CDCEtcdClient
	testUpstreamManager *upstream.Manager
	verifyCreateFunc    func(ctx context.Context, cfg *ChangefeedConfig,
		capture api.CaptureInfoProvider) (*model.ChangeFeedInfo, error)
	verifyUpdateFunc func(ctx context.Context, cfg *ChangefeedConfig, oldInfo *model.ChangeFeedInfo,
		oldUpInfo *model.UpstreamInfo) (*model.ChangeFeedInfo, *model.UpstreamInfo, error)
}

// OpenAPIV2 provides CDC v2 APIs
type OpenAPIV2 struct {
	capture api.CaptureInfoProvider
	// stubs, only work for unit tests
	stubs *mockStubs
}

// NewOpenAPIV2 creates a new OpenAPIV2.
func NewOpenAPIV2(c *capture.Capture) OpenAPIV2 {
	return OpenAPIV2{capture: c}
}

// NewOpenAPIV2ForTest creates a new OpenAPIV2.
func NewOpenAPIV2ForTest(capture api.CaptureInfoProvider, stubs *mockStubs) OpenAPIV2 {
	return OpenAPIV2{capture, stubs}
}

func (h *OpenAPIV2) statusProvider() owner.StatusProvider {
	if h.stubs != nil {
		return h.stubs.testStatusProvider
	}
	return h.capture.StatusProvider()
}

func (h *OpenAPIV2) etcdClient() *etcd.CDCEtcdClient {
	if h.stubs != nil {
		return h.stubs.testEtcdClient
	}
	return h.capture.GetEtcdClient()
}

func (h *OpenAPIV2) upstreamManager() *upstream.Manager {
	if h.stubs != nil {
		return h.stubs.testUpstreamManager
	}
	return h.capture.GetUpstreamManager()
}

func (h *OpenAPIV2) verifyUpdateChangefeedConfig() func(ctx context.Context, cfg *ChangefeedConfig,
	oldInfo *model.ChangeFeedInfo, oldUpInfo *model.UpstreamInfo) (*model.ChangeFeedInfo, *model.UpstreamInfo, error) {
	if h.stubs != nil {
		return h.stubs.verifyUpdateFunc
	}
	return verifyUpdateChangefeedConfig
}

func (h *OpenAPIV2) verifyCreateChangefeedConfig() func(ctx context.Context, cfg *ChangefeedConfig,
	capture api.CaptureInfoProvider) (*model.ChangeFeedInfo, error) {
	if h.stubs != nil {
		return h.stubs.verifyCreateFunc
	}
	return verifyCreateChangefeedConfig
}

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
