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

package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/capture"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// startStatusHTTP starts the HTTP server.
// `lis` is a listener that gives us plain-text HTTP requests.
// TODO can we decouple the HTTP server from the capture server?
func (s *Server) startStatusHTTP(lis net.Listener) error {
	conf := config.GetGlobalServerConfig()

	// OpenAPI handling logic is injected here.
	router := newRouter(capture.NewHTTPHandler(s.capture))

	// Inject the legacy API handlers.
	router.GET("/status", gin.WrapF(s.handleStatus))
	router.GET("/debug/info", gin.WrapF(s.handleDebugInfo))
	router.POST("/capture/owner/resign", gin.WrapF(s.handleResignOwner))
	router.POST("/capture/owner/admin", gin.WrapF(s.handleChangefeedAdmin))
	router.POST("/capture/owner/rebalance_trigger", gin.WrapF(s.handleRebalanceTrigger))
	router.POST("/capture/owner/move_table", gin.WrapF(s.handleMoveTable))
	router.POST("/capture/owner/changefeed/query", gin.WrapF(s.handleChangefeedQuery))
	router.POST("/admin/log", gin.WrapF(handleAdminLogLevel))

	if util.FailpointBuild {
		// `http.StripPrefix` is needed because `failpoint.HttpHandler` assumes that it handles the prefix `/`.
		router.Any("/debug/fail/*any", gin.WrapH(http.StripPrefix("/debug/fail", &failpoint.HttpHandler{})))
	}

	prometheus.DefaultGatherer = registry
	router.Any("/metrics", gin.WrapH(promhttp.Handler()))

	// No need to configure TLS because it is already handled by `s.tcpServer`.
	s.statusServer = &http.Server{Handler: router}

	go func() {
		log.Info("http server is running", zap.String("addr", conf.Addr))
		err := s.statusServer.Serve(lis)
		if err != nil && err != http.ErrServerClosed {
			log.Error("http server error", zap.Error(cerror.WrapError(cerror.ErrServeHTTP, err)))
		}
	}()
	return nil
}

// status of cdc server
type status struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
	ID      string `json:"id"`
	Pid     int    `json:"pid"`
	IsOwner bool   `json:"is_owner"`
}

func (s *Server) writeEtcdInfo(ctx context.Context, cli *etcd.CDCEtcdClient, w io.Writer) {
	resp, err := cli.Client.Get(ctx, etcd.EtcdKeyBase, clientv3.WithPrefix())
	if err != nil {
		fmt.Fprintf(w, "failed to get info: %s\n\n", err.Error())
		return
	}

	for _, kv := range resp.Kvs {
		fmt.Fprintf(w, "%s\n\t%s\n\n", string(kv.Key), string(kv.Value))
	}
}

func (s *Server) handleDebugInfo(w http.ResponseWriter, req *http.Request) {
	s.capture.WriteDebugInfo(w)
	fmt.Fprintf(w, "\n\n*** etcd info ***:\n\n")
	s.writeEtcdInfo(req.Context(), s.etcdClient, w)
}

func (s *Server) handleStatus(w http.ResponseWriter, req *http.Request) {
	st := status{
		Version: version.ReleaseVersion,
		GitHash: version.GitHash,
		Pid:     os.Getpid(),
	}

	if s.capture != nil {
		st.ID = s.capture.Info().ID
		st.IsOwner = s.capture.IsOwner()
	}
	writeData(w, st)
}

func writeInternalServerError(w http.ResponseWriter, err error) {
	writeError(w, http.StatusInternalServerError, err)
}

func writeError(w http.ResponseWriter, statusCode int, err error) {
	w.WriteHeader(statusCode)
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		log.Error("write error", zap.Error(err))
	}
}

func writeData(w http.ResponseWriter, data interface{}) {
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		log.Error("invalid json data", zap.Reflect("data", data), zap.Error(err))
		writeInternalServerError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	if err != nil {
		log.Error("fail to write data", zap.Error(err))
	}
}
