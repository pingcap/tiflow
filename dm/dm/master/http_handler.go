// Copyright 2019 PingCAP, Inc.
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

package master

import (
	"context"
	"net/http"
	"net/http/pprof"

	"github.com/gogo/gateway"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// statusHandler handles process status.
type statusHandler struct{}

func (h *statusHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	text := utils.GetRawInfo()
	if _, err := w.Write([]byte(text)); err != nil {
		log.L().Error("write status response", log.ShortError(err))
	}
}

// getStatusHandle returns a HTTP handler to handle process status.
func getStatusHandle() http.Handler {
	return &statusHandler{}
}

// getHTTPAPIHandler returns a HTTP handler to handle DM-master APIs.
func getHTTPAPIHandler(ctx context.Context, addr string, securityOpt grpc.DialOption) (http.Handler, error) {
	// dial the real API server in non-blocking mode, it may not started yet.
	opts := []grpc.DialOption{securityOpt}
	// NOTE: should we need to replace `host` in `addr` to `127.0.0.1`?
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, terror.ErrMasterHandleHTTPApis.Delegate(err)
	}

	jsonpb := &gateway.JSONPb{
		EmitDefaults: true,
		Indent:       "  ",
		OrigName:     true,
	}

	gwmux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, jsonpb),
		// This is necessary to get error details properly
		// marshalled in unary requests.
		runtime.WithProtoErrorHandler(runtime.DefaultHTTPProtoErrorHandler),
	)
	err = pb.RegisterMasterHandler(ctx, gwmux, conn)
	if err != nil {
		return nil, terror.ErrMasterHandleHTTPApis.Delegate(err)
	}
	return gwmux, nil
}

// getDebugHandler returns a HTTP handler to handle debug information.
func getDebugHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
