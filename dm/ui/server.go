// Copyright 2021 PingCAP, Inc.
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

// this file implement all of the APIs of the DataMigration service.

package ui

import (
	"io/fs"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// WebUIAssetsHandler returns a http handler for serving static files.
func WebUIAssetsHandler() http.FileSystem {
	stripped, err := fs.Sub(WebUIAssets, "dist")
	if err != nil {
		panic(err) // this should never happen
	}
	return http.FS(stripped)
}

// InitWebUIRouter initializes the webUI router.
func InitWebUIRouter() *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(openapi.ZapLogger(log.L().WithFields(zap.String("component", "webui")).Logger))
	router.StaticFS("/dashboard/", WebUIAssetsHandler())
	return router
}
