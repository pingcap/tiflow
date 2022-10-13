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

package cdc

import (
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"strconv"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tiflow/cdc/api/owner"
	"github.com/pingcap/tiflow/cdc/api/status"
	v1 "github.com/pingcap/tiflow/cdc/api/v1"
	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/cdc/capture"
	_ "github.com/pingcap/tiflow/docs/swagger" // use for OpenAPI online docs
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// RegisterRoutes create a router for OpenAPI
func RegisterRoutes(
	router *gin.Engine,
	capture capture.Capture,
	registry prometheus.Gatherer,
) {
	// online docs
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// Open API V1
	v1.RegisterOpenAPIRoutes(router, v1.NewOpenAPI(capture))
	// Open API V2
	v2.RegisterOpenAPIV2Routes(router, v2.NewOpenAPIV2(capture))

	// Owner API
	owner.RegisterOwnerAPIRoutes(router, capture)

	// Status API
	status.RegisterStatusAPIRoutes(router, capture)

	// Log API
	router.POST("/admin/log", gin.WrapF(owner.HandleAdminLogLevel))

	// pprof debug API
	pprofGroup := router.Group("/debug/pprof/")
	pprofGroup.GET("", gin.WrapF(pprof.Index))
	pprofGroup.GET("/:any", gin.WrapF(pprof.Index))
	pprofGroup.GET("/cmdline", gin.WrapF(pprof.Cmdline))
	pprofGroup.GET("/profile", gin.WrapF(pprof.Profile))
	pprofGroup.GET("/symbol", gin.WrapF(pprof.Symbol))
	pprofGroup.GET("/trace", gin.WrapF(pprof.Trace))
	pprofGroup.GET("/threadcreate", gin.WrapF(pprof.Handler("threadcreate").ServeHTTP))

	// Failpoint API
	if util.FailpointBuild {
		// `http.StripPrefix` is needed because `failpoint.HttpHandler` assumes that it handles the prefix `/`.
		router.Any("/debug/fail/*any", gin.WrapH(http.StripPrefix("/debug/fail", &failpoint.HttpHandler{})))
	}

	// Promtheus metrics API
	prometheus.DefaultGatherer = registry
	router.Any("/metrics", gin.WrapH(promhttp.Handler()))

	ballast := newBallast(s.cfg.MaxBallastObjectSize)
	{
		err := ballast.SetSize(s.cfg.BallastObjectSize)
		if err != nil {
			logutil.BgLogger().Error("set initial ballast object size failed", zap.Error(err))
		}
	}
	serverMux.HandleFunc("/debug/ballast-object-sz", ballast.GenHTTPHandler())
}

// Ballast try to reduce the GC frequency by using Ballast Object
type Ballast struct {
	ballast     []byte
	ballastLock sync.Mutex

	maxSize int
}

func newBallast(maxSize int) *Ballast {
	var b Ballast
	b.maxSize = 1024 * 1024 * 1024 * 2
	if maxSize > 0 {
		b.maxSize = maxSize
	} else {
		// we try to use the total amount of ram as a reference to set the default ballastMaxSz
		// since the fatal throw "runtime: out of memory" would never yield to `recover`
		totalRAMSz, err := memory.MemTotal()
		if err != nil {
			logutil.BgLogger().Error("failed to get the total amount of RAM on this system", zap.Error(err))
		} else {
			maxSzAdvice := totalRAMSz >> 2
			if uint64(b.maxSize) > maxSzAdvice {
				b.maxSize = int(maxSzAdvice)
			}
		}
	}
	return &b
}

// GetSize get the size of ballast object
func (b *Ballast) GetSize() int {
	var sz int
	b.ballastLock.Lock()
	sz = len(b.ballast)
	b.ballastLock.Unlock()
	return sz
}

// SetSize set the size of ballast object
func (b *Ballast) SetSize(newSz int) error {
	if newSz < 0 {
		return fmt.Errorf("newSz cannot be negative: %d", newSz)
	}
	if newSz > b.maxSize {
		return fmt.Errorf("newSz cannot be bigger than %d but it has value %d", b.maxSize, newSz)
	}
	b.ballastLock.Lock()
	b.ballast = make([]byte, newSz)
	b.ballastLock.Unlock()
	return nil
}

// GenHTTPHandler generate a HTTP handler to get/set the size of this ballast object
func (b *Ballast) GenHTTPHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_, err := w.Write([]byte(strconv.Itoa(b.GetSize())))
			terror.Log(err)
		case http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				terror.Log(err)
				return
			}
			newSz, err := strconv.Atoi(string(body))
			if err == nil {
				err = b.SetSize(newSz)
			}
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				errStr := err.Error()
				if _, err := w.Write([]byte(errStr)); err != nil {
					terror.Log(err)
				}
				return
			}
		}
	}
}
