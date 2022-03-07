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
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

const (
	buildPath  = "dist"
	assetsPath = "assets"
	indexPath  = "/dashboard/"
)

// WebUIAssetsHandler returns a http handler for serving static files.
func WebUIAssetsHandler() http.FileSystem {
	stripped, err := fs.Sub(WebUIAssets, buildPath)
	if err != nil {
		panic(err) // this should never happen
	}
	return http.FS(stripped)
}

// we need this to handle this case: user want to access /dashboard/source.html/ but webui is a single page app,
// and it only can handle requests in index page, so we need to redirect to index page.
func alwaysRedirect(path string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// note that static file like css and js under the assets folder should not be redirected.
		if c.Request.URL.Path != path && !strings.Contains(c.Request.URL.Path, assetsPath) {
			c.Redirect(http.StatusPermanentRedirect, path)
			c.AbortWithStatus(http.StatusPermanentRedirect)
		} else {
			c.Next()
		}
	}
}

// InitWebUIRouter initializes the webUI router.
func InitWebUIRouter() *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(alwaysRedirect(indexPath))
	router.Use(openapi.ZapLogger(log.L().WithFields(zap.String("component", "webui")).Logger))
	router.StaticFS(indexPath, WebUIAssetsHandler())
	return router
}
