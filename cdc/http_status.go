// Copyright 2020 PingCAP, Inc.
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
	"net/http/pprof"
	"os"

	"github.com/pingcap/tiflow/pkg/util"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

func (s *Server) startStatusHTTP() error {
	serverMux := http.NewServeMux()

	serverMux.HandleFunc("/debug/pprof/", pprof.Index)
	serverMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	serverMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	serverMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	serverMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	serverMux.HandleFunc("/status", s.handleStatus)
	serverMux.HandleFunc("/debug/info", s.handleDebugInfo)
	serverMux.HandleFunc("/capture/owner/resign", s.handleResignOwner)
	serverMux.HandleFunc("/capture/owner/admin", s.handleChangefeedAdmin)
	serverMux.HandleFunc("/capture/owner/rebalance_trigger", s.handleRebalanceTrigger)
	serverMux.HandleFunc("/capture/owner/move_table", s.handleMoveTable)
	serverMux.HandleFunc("/capture/owner/changefeed/query", s.handleChangefeedQuery)
	serverMux.HandleFunc("/admin/log", handleAdminLogLevel)
	serverMux.HandleFunc("/api/v1/changefeeds", s.handleChangefeeds)
	serverMux.HandleFunc("/api/v1/health", s.handleHealth)

	if util.FailpointBuild {
		// `http.StripPrefix` is needed because `failpoint.HttpHandler` assumes that it handles the prefix `/`.
		serverMux.Handle("/debug/fail/", http.StripPrefix("/debug/fail", &failpoint.HttpHandler{}))
	}

	prometheus.DefaultGatherer = registry
	serverMux.Handle("/metrics", promhttp.Handler())
	conf := config.GetGlobalServerConfig()
	tlsConfig, err := conf.Security.ToTLSConfigWithVerify()
	if err != nil {
		log.Error("status server get tls config failed", zap.Error(err))
		return errors.Trace(err)
	}
	s.statusServer = &http.Server{Addr: conf.Addr, Handler: serverMux, TLSConfig: tlsConfig}

	ln, err := net.Listen("tcp", conf.Addr)
	if err != nil {
		return cerror.WrapError(cerror.ErrServeHTTP, err)
	}
	go func() {
		log.Info("status http server is running", zap.String("addr", conf.Addr))
		if tlsConfig != nil {
			err = s.statusServer.ServeTLS(ln, conf.Security.CertPath, conf.Security.KeyPath)
		} else {
			err = s.statusServer.Serve(ln)
		}
		if err != nil && err != http.ErrServerClosed {
			log.Error("status server error", zap.Error(cerror.WrapError(cerror.ErrServeHTTP, err)))
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

func (s *Server) writeEtcdInfo(ctx context.Context, cli *kv.CDCEtcdClient, w io.Writer) {
	resp, err := cli.Client.Get(ctx, kv.EtcdKeyBase, clientv3.WithPrefix())
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
