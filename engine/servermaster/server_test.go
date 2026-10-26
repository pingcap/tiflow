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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/phayes/freeport"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/openapi"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/election"
	electionMock "github.com/pingcap/tiflow/pkg/election/mock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/stretchr/testify/require"
)

func init() {
	err := logutil.InitLogger(&logutil.Config{Level: "warn"})
	if err != nil {
		panic(err)
	}
}

func prepareServerEnv(t *testing.T) *Config {
	ports, err := freeport.GetFreePorts(1)
	require.NoError(t, err)
	cfgTpl := `
addr = "127.0.0.1:%d"
advertise-addr = "127.0.0.1:%d"
[framework-meta]
store-id = "root"
endpoints = ["127.0.0.1:%d"]
schema = "test0"
user = "root"
[business-meta]
store-id = "default"
endpoints = ["127.0.0.1:%d"]
schema = "test1"
`
	cfgStr := fmt.Sprintf(cfgTpl, ports[0], ports[0], ports[0], ports[0])
	cfg := GetDefaultMasterConfig()
	err = cfg.configFromString(cfgStr)
	require.Nil(t, err)
	err = cfg.AdjustAndValidate()
	require.Nil(t, err)

	cfg.Addr = fmt.Sprintf("127.0.0.1:%d", ports[0])

	return cfg
}

func newMockElector(t *testing.T) election.Elector {
	elector := electionMock.NewMockElector(gomock.NewController(t))
	elector.EXPECT().IsLeader().AnyTimes().Return(true)
	return elector
}

// Disable parallel run for this case, because prometheus http handler will meet
// data race if parallel run is enabled
func TestServe(t *testing.T) {
	cfg := prepareServerEnv(t)
	s := &Server{
		cfg:            cfg,
		msgService:     p2p.NewMessageRPCServiceWithRPCServer("servermaster", nil, nil),
		leaderDegrader: newFeatureDegrader(),
		elector:        electionMock.NewMockElector(gomock.NewController(t)),
	}

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.serve(ctx)
	}()

	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", cfg.Addr)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, time.Second*5, time.Millisecond*100, "wait for server to start")

	apiURL := "http://" + cfg.Addr
	testPprof(t, apiURL)
	testPrometheusMetrics(t, apiURL)

	cancel()
	wg.Wait()
}

func testPprof(t *testing.T, addr string) {
	ctx := context.Background()
	urls := []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/symbol",
		// enable these two apis will make ut slow
		//"/debug/pprof/profile", http.MethodGet,
		//"/debug/pprof/trace", http.MethodGet,
		"/debug/pprof/threadcreate",
		"/debug/pprof/allocs",
		"/debug/pprof/block",
		"/debug/pprof/goroutine?debug=1",
		"/debug/pprof/mutex?debug=1",
	}
	cli, err := httputil.NewClient(nil)
	require.NoError(t, err)
	for _, uri := range urls {
		resp, err := cli.Get(ctx, addr+uri)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		_, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}
}

func testPrometheusMetrics(t *testing.T, addr string) {
	ctx := context.Background()
	cli, err := httputil.NewClient(nil)
	require.NoError(t, err)
	resp, err := cli.Get(ctx, addr+"/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
}

type mockJobManager struct {
	JobManager
	jobMu sync.RWMutex
	jobs  map[pb.Job_State][]*pb.Job
}

func (m *mockJobManager) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.Job, error) {
	for _, jobs := range m.jobs {
		for _, job := range jobs {
			if job.GetId() == req.GetId() {
				return job, nil
			}
		}
	}
	return nil, errors.ErrJobNotFound.GenWithStackByArgs(req.GetId())
}

func (m *mockJobManager) JobCount(status pb.Job_State) int {
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	return len(m.jobs[status])
}

type mockExecutorManager struct {
	ExecutorManager
	executorMu sync.RWMutex
	count      map[model.ExecutorStatus]int
}

func (m *mockExecutorManager) ExecutorCount(status model.ExecutorStatus) int {
	m.executorMu.RLock()
	defer m.executorMu.RUnlock()
	return m.count[status]
}

func TestCollectMetric(t *testing.T) {
	cfg := prepareServerEnv(t)

	s := &Server{
		cfg:            cfg,
		metrics:        newServerMasterMetric(),
		msgService:     p2p.NewMessageRPCServiceWithRPCServer("servermaster", nil, nil),
		leaderDegrader: newFeatureDegrader(),
		elector:        newMockElector(t),
	}
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.serve(ctx)
	}()

	jobManager := &mockJobManager{
		jobs: map[pb.Job_State][]*pb.Job{
			pb.Job_Running: {
				&pb.Job{
					Id: "job-1",
				},
				&pb.Job{
					Id: "job-2",
				},
				&pb.Job{
					Id: "job-3",
				},
			},
		},
	}
	executorManager := &mockExecutorManager{
		count: map[model.ExecutorStatus]int{
			model.Initing: 1,
			model.Running: 2,
		},
	}
	s.jobManager = jobManager
	s.executorManager = executorManager

	s.collectLeaderMetric()
	apiURL := fmt.Sprintf("http://%s", cfg.Addr)
	testCustomedPrometheusMetrics(t, apiURL)

	cancel()
	wg.Wait()
}

func testCustomedPrometheusMetrics(t *testing.T, addr string) {
	ctx := context.Background()
	cli, err := httputil.NewClient(nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, err := cli.Get(ctx, addr+"/metrics")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		metric := string(body)
		return strings.Contains(metric, "tiflow_server_master_job_num") &&
			strings.Contains(metric, "tiflow_server_master_executor_num")
	}, time.Second, time.Millisecond*20)
}

func TestHTTPErrorHandler(t *testing.T) {
	cfg := prepareServerEnv(t)

	s := &Server{
		cfg:        cfg,
		msgService: p2p.NewMessageRPCServiceWithRPCServer("servermaster", nil, nil),
		jobManager: &mockJobManager{
			jobs: map[pb.Job_State][]*pb.Job{
				pb.Job_Running: {
					&pb.Job{
						Id: "job-1",
					},
				},
			},
		},
		leaderDegrader: newFeatureDegrader(),
		elector:        newMockElector(t),
		forwardChecker: newForwardChecker(newMockElector(t)),
	}
	s.leaderDegrader.updateMasterWorkerManager(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.serve(ctx)
	}()

	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", cfg.Addr)
		if err != nil {
			return false
		}
		require.NoError(t, conn.Close())
		return true
	}, time.Second*5, time.Millisecond*100, "wait for server start")

	cli, err := httputil.NewClient(nil)
	require.NoError(t, err)

	resp, err := cli.Get(ctx, fmt.Sprintf("http://%s/api/v1/jobs/job-1", cfg.Addr))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = cli.Get(ctx, fmt.Sprintf("http://%s/api/v1/jobs/job-2", cfg.Addr))
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)

	var httpErr openapi.HTTPError
	err = json.Unmarshal(body, &httpErr)
	require.NoError(t, err)
	require.Equal(t, string(errors.ErrJobNotFound.RFCCode()), httpErr.Code)

	cancel()
	wg.Wait()
}
