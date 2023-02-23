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

package servermaster

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRegisterRoutes(t *testing.T) {
	t.Parallel()
	router := http.NewServeMux()
	grpcMux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions:   protojson.MarshalOptions{UseProtoNames: true},
			UnmarshalOptions: protojson.UnmarshalOptions{},
		}),
	)
	err := pb.RegisterJobManagerHandlerServer(context.Background(), grpcMux, pb.UnimplementedJobManagerServer{})
	require.NoError(t, err)

	forwardJobAPI := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/jobs/job1/status" {
			w.WriteHeader(http.StatusOK)
		} else {
			http.NotFound(w, r)
		}
	})
	registerRoutes(router, grpcMux, forwardJobAPI)

	testCases := []struct {
		method       string
		path         string
		expectedCode int
	}{
		{
			method:       http.MethodGet,
			path:         "/swagger",
			expectedCode: http.StatusOK,
		},
		{
			method:       http.MethodGet,
			path:         "/swagger/v1/openapiv2.json",
			expectedCode: http.StatusOK,
		},
		{
			method:       http.MethodGet,
			path:         "/swagger/v1/openapiv3.json",
			expectedCode: http.StatusNotFound,
		},
		{
			method:       http.MethodGet,
			path:         "/api/v1/jobs",
			expectedCode: http.StatusNotImplemented,
		},
		{
			method:       http.MethodPost,
			path:         "/api/v1/jobs",
			expectedCode: http.StatusNotImplemented,
		},
		{
			method:       http.MethodGet,
			path:         "/api/v1/jobs/job1",
			expectedCode: http.StatusNotImplemented,
		},
		{
			method:       http.MethodPost,
			path:         "/api/v1/jobs/job1",
			expectedCode: http.StatusNotImplemented,
		},
		{
			method:       http.MethodGet,
			path:         "/api/v1/jobs/job1/pause",
			expectedCode: http.StatusNotFound,
		},
		{
			method:       http.MethodGet,
			path:         "/api/v1/jobs/job1/cancel",
			expectedCode: http.StatusNotImplemented,
		},
		{
			method:       http.MethodGet,
			path:         "/api/v1/jobs/job1/status",
			expectedCode: http.StatusOK,
		},
		{
			method:       http.MethodGet,
			path:         "/api/v1/jobs/job1/config",
			expectedCode: http.StatusNotFound,
		},
		{
			method:       http.MethodGet,
			path:         "/debug/pprof/",
			expectedCode: http.StatusOK,
		},
		{
			method:       http.MethodGet,
			path:         "/metrics",
			expectedCode: http.StatusOK,
		},
	}

	for i, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, tc.expectedCode, w.Code)
		})
	}
}

func TestShouldForwardJobAPI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	testCases := []struct {
		method        string
		path          string
		shouldForward bool
	}{
		{
			method:        http.MethodGet,
			path:          "/api/v1/jobs",
			shouldForward: false,
		},
		{
			method:        http.MethodGet,
			path:          "/api/v1/jobs/job1",
			shouldForward: false,
		},
		{
			method:        http.MethodGet,
			path:          "/api/v1/jobs/job1/pause",
			shouldForward: true,
		},
		{
			method:        http.MethodPost,
			path:          "/api/v1/jobs/job1/pause",
			shouldForward: true,
		},
		{
			method:        http.MethodGet,
			path:          "/api/v1/jobs/job1/cancel",
			shouldForward: false,
		},
		{
			method:        http.MethodPost,
			path:          "/api/v1/jobs/job1/cancel",
			shouldForward: false,
		},
		{
			method:        http.MethodGet,
			path:          "/api/v1/jobs/job1/status",
			shouldForward: true,
		},
		{
			method:        http.MethodPost,
			path:          "/api/v1/jobs/job1/config",
			shouldForward: true,
		},
	}

	for i, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequestWithContext(ctx, tc.method, tc.path, nil)
			require.NoError(t, err)
			require.Equal(t, tc.shouldForward, shouldForwardJobAPI(req))
		})
	}
}
