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
	job2ExecutorID map[model.JobID]model.ExecutorID
	executorAddrs  map[model.ExecutorID]string

	JobManager
	ExecutorManager
}

func (m *mockManager) QueryJob(ctx context.Context, req *pb.QueryJobRequest) *pb.QueryJobResponse {
	executorID, ok := m.job2ExecutorID[req.JobId]
	if !ok {
		return &pb.QueryJobResponse{
			Err: &pb.Error{Code: pb.ErrorCode_UnKnownJob},
		}
	}
	return &pb.QueryJobResponse{
		Status: pb.QueryJobResponse_online,
		JobMasterInfo: &pb.WorkerInfo{
			ExecutorId: string(executorID),
		},
	}
}

func (m *mockManager) GetAddr(executorID model.ExecutorID) (string, bool) {
	addr, ok := m.executorAddrs[executorID]
	return addr, ok
}

type mockServerInfoProvider struct {
	mockMgr    *mockManager
	serverAddr string
	leaderAddr string
}

func (m *mockServerInfoProvider) IsLeader() bool {
	return m.serverAddr == m.leaderAddr
}

func (m *mockServerInfoProvider) LeaderAddr() (string, bool) {
	return m.leaderAddr, true
}

func (m *mockServerInfoProvider) JobManager() (JobManager, bool) {
	return m.mockMgr, true
}

func (m *mockServerInfoProvider) ExecutorManager() (ExecutorManager, bool) {
	return m.mockMgr, true
}

func TestOpenAPIBasic(t *testing.T) {
	reqs := make(chan *http.Request, 10)
	executorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs <- r
	}))
	defer executorSrv.Close()

	infoProvider := &mockServerInfoProvider{
		mockMgr: &mockManager{
			job2ExecutorID: map[model.JobID]model.ExecutorID{
				"job1": "mock-executor",
			},
			executorAddrs: map[model.ExecutorID]string{
				"mock-executor": executorSrv.URL,
			},
		},
	}
	openapi := NewOpenAPI(infoProvider)
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

func TestForwardToLeader(t *testing.T) {
	reqs := make(chan *http.Request, 10)
	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs <- r
	}))
	defer leaderSrv.Close()

	infoProvider := &mockServerInfoProvider{
		mockMgr:    &mockManager{},
		serverAddr: "",
		leaderAddr: leaderSrv.URL,
	}
	openapi := NewOpenAPI(infoProvider)
	router := gin.New()
	RegisterOpenAPIRoutes(router, openapi)

	masterSrv := httptest.NewServer(router)
	defer masterSrv.Close()

	req, err := http.NewRequest(http.MethodGet, masterSrv.URL+"/api/v1/jobs", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	forwardReq := <-reqs
	require.Equal(t, "/api/v1/jobs", forwardReq.URL.Path)
}
