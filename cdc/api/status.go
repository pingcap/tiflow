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

package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/version"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// status of cdc server
type status struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
	ID      string `json:"id"`
	Pid     int    `json:"pid"`
	IsOwner bool   `json:"is_owner"`
}

type statusAPI struct {
	capture *capture.Capture
}

// RegisterStatusAPIRoutes registers routes for status.
func RegisterStatusAPIRoutes(router *gin.Engine, capture *capture.Capture) {
	statusAPI := statusAPI{capture: capture}
	router.GET("/status", gin.WrapF(statusAPI.handleStatus))
	router.GET("/debug/info", gin.WrapF(statusAPI.handleDebugInfo))
}

func (h *statusAPI) writeEtcdInfo(ctx context.Context, cli *etcd.CDCEtcdClient, w io.Writer) {
	resp, err := cli.Client.Get(ctx, etcd.EtcdKeyBase, clientv3.WithPrefix())
	if err != nil {
		fmt.Fprintf(w, "failed to get info: %s\n\n", err.Error())
		return
	}

	for _, kv := range resp.Kvs {
		fmt.Fprintf(w, "%s\n\t%s\n\n", string(kv.Key), string(kv.Value))
	}
}

func (h *statusAPI) handleDebugInfo(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	h.capture.WriteDebugInfo(ctx, w)
	fmt.Fprintf(w, "\n\n*** etcd info ***:\n\n")
	h.writeEtcdInfo(ctx, h.capture.EtcdClient, w)
}

func (h *statusAPI) handleStatus(w http.ResponseWriter, req *http.Request) {
	st := status{
		Version: version.ReleaseVersion,
		GitHash: version.GitHash,
		Pid:     os.Getpid(),
	}

	if h.capture != nil {
		info, err := h.capture.Info()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		st.ID = info.ID
		st.IsOwner = h.capture.IsOwner()
	}
	writeData(w, st)
}
