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

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
)

func init() {
	gin.SetMode(gin.TestMode)
}

type mockManager struct {
	JobManager
	ExecutorManager

	execAddr string
}

func (m mockManager) QueryJob(ctx context.Context, req *pb.QueryJobRequest) *pb.QueryJobResponse {
	return &pb.QueryJobResponse{
		Status: pb.QueryJobResponse_online,
		JobMasterInfo: &pb.WorkerInfo{
			ExecutorId: "executor1",
		},
	}
}

func (m mockManager) GetAddr(executorID model.ExecutorID) (string, bool) {
	if executorID == "executor1" {
		return m.execAddr, true
	} else {
		return "", false
	}
}

func TestOpenAPI(t *testing.T) {
	reqs := make(chan *http.Request, 10)
	execSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs <- r
	}))
	defer execSrv.Close()

	mockMgr := mockManager{execAddr: execSrv.URL}
	openapi := NewOpenAPI(mockMgr, mockMgr)
	router := gin.New()
	RegisterOpenAPIRoutes(router, openapi)

	testCases := []struct {
		method        string
		path          string
		statusCode    int
		shouldForward bool
	}{
		{
			method:        http.MethodGet,
			path:          "/api/v1/jobs",
			statusCode:    http.StatusNotImplemented,
			shouldForward: false,
		},
		{
			method:        http.MethodGet,
			path:          "/api/v1/jobs/job1",
			statusCode:    http.StatusNotImplemented,
			shouldForward: false,
		},
		{
			method:        http.MethodGet,
			path:          "/api/v1/jobs/job1/pause",
			statusCode:    http.StatusMethodNotAllowed,
			shouldForward: false,
		},
		{
			method:        http.MethodPost,
			path:          "/api/v1/jobs/job1/pause",
			statusCode:    http.StatusNotImplemented,
			shouldForward: false,
		},
		{
			method:        http.MethodGet,
			path:          "/api/v1/jobs/job1/cancel",
			statusCode:    http.StatusMethodNotAllowed,
			shouldForward: false,
		},
		{
			method:        http.MethodPost,
			path:          "/api/v1/jobs/job1/cancel",
			statusCode:    http.StatusNotImplemented,
			shouldForward: false,
		},
		{
			method:        http.MethodGet,
			path:          "/api/v1/jobs/job1/status",
			statusCode:    http.StatusOK,
			shouldForward: true,
		},
		{
			method:        http.MethodPost,
			path:          "/api/v1/jobs/job1/config",
			statusCode:    http.StatusOK,
			shouldForward: true,
		},
	}

	masterSrv := httptest.NewServer(router)
	defer masterSrv.Close()

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			req, err := http.NewRequest(tc.method, masterSrv.URL+tc.path, nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, tc.statusCode, resp.StatusCode)

			if tc.shouldForward {
				forwardReq := <-reqs
				require.Equal(t, tc.method, forwardReq.Method)
				require.Equal(t, tc.path, forwardReq.URL.Path)
			}
		})
	}
	require.Len(t, reqs, 0)
}
