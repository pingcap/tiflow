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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
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

	prometheus.DefaultGatherer = registry
	serverMux.Handle("/metrics", promhttp.Handler())

	credential := &security.Credential{}
	if s.opts.credential != nil {
		credential = s.opts.credential
	}
	tlsConfig, err := credential.ToTLSConfigWithVerify()
	if err != nil {
		log.Error("status server get tls config failed", zap.Error(err))
		return errors.Trace(err)
	}
	addr := s.opts.addr
	s.statusServer = &http.Server{Addr: addr, Handler: serverMux, TLSConfig: tlsConfig}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		log.Info("status http server is running", zap.String("addr", addr))
		if tlsConfig != nil {
			err = s.statusServer.ServeTLS(ln, credential.CertPath, credential.KeyPath)
		} else {
			err = s.statusServer.Serve(ln)
		}
		if err != nil && err != http.ErrServerClosed {
			log.Error("status server error", zap.Error(err))
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

func (s *Server) writeEtcdInfo(ctx context.Context, cli kv.CDCEtcdClient, w io.Writer) {
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
	fmt.Fprintf(w, "\n\n*** owner info ***:\n\n")
	s.owner.writeDebugInfo(w)

	fmt.Fprintf(w, "\n\n*** processors info ***:\n\n")
	for _, p := range s.capture.processors {
		p.writeDebugInfo(w)
		fmt.Fprintf(w, "\n")
	}

	fmt.Fprintf(w, "\n\n*** etcd info ***:\n\n")
	s.writeEtcdInfo(req.Context(), s.capture.etcdClient, w)
}

func (s *Server) handleStatus(w http.ResponseWriter, req *http.Request) {
	st := status{
		Version: util.ReleaseVersion,
		GitHash: util.GitHash,
		Pid:     os.Getpid(),
	}
	if s.capture != nil {
		st.ID = s.capture.info.ID
	}
	st.IsOwner = s.owner != nil
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
