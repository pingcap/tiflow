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
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	"github.com/pingcap/tiflow/engine/servermaster/cluster"
	"github.com/pingcap/tiflow/engine/servermaster/scheduler"
	"github.com/pingcap/tiflow/pkg/logutil"

	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
)

func init() {
	err := logutil.InitLogger(&logutil.Config{Level: "warn"})
	if err != nil {
		panic(err)
	}
}

func prepareServerEnv(t *testing.T, name string) (string, *Config) {
	dir := t.TempDir()

	ports, err := freeport.GetFreePorts(2)
	require.Nil(t, err)
	cfgTpl := `
master-addr = "127.0.0.1:%d"
advertise-addr = "127.0.0.1:%d"
[frame-metastore-conf]
store-id = "root"
endpoints = ["127.0.0.1:%d"]
[frame-metastore-conf.auth]
user = "root"
[user-metastore-conf]
store-id = "default"
endpoints = ["127.0.0.1:%d"]
[etcd]
name = "%s"
data-dir = "%s"
peer-urls = "http://127.0.0.1:%d"
initial-cluster = "%s=http://127.0.0.1:%d"`
	cfgStr := fmt.Sprintf(cfgTpl, ports[0], ports[0], ports[0], ports[0], name, dir, ports[1], name, ports[1])
	cfg := GetDefaultMasterConfig()
	err = cfg.configFromString(cfgStr)
	require.Nil(t, err)
	err = cfg.Adjust()
	require.Nil(t, err)

	masterAddr := fmt.Sprintf("127.0.0.1:%d", ports[0])

	return masterAddr, cfg
}

// Disable parallel run for this case, because prometheus http handler will meet
// data race if parallel run is enabled
func TestStartGrpcSrv(t *testing.T) {
	masterAddr, cfg := prepareServerEnv(t, "test-start-grpc-srv")

	s := &Server{cfg: cfg}
	ctx := context.Background()
	err := s.startGrpcSrv(ctx)
	require.Nil(t, err)

	apiURL := fmt.Sprintf("http://%s", masterAddr)
	testPprof(t, apiURL)

	testPrometheusMetrics(t, apiURL)
	s.Stop()
}

func TestStartGrpcSrvCancelable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ports, err := freeport.GetFreePorts(3)
	require.Nil(t, err)
	cfgTpl := `
master-addr = "127.0.0.1:%d"
advertise-addr = "127.0.0.1:%d"
[frame-metastore-conf]
store-id = "root"
endpoints = ["127.0.0.1:%d"]
[frame-metastore-conf.auth]
user = "root"
[user-metastore-conf]
store-id = "default"
endpoints = ["127.0.0.1:%d"]
[etcd]
name = "server-master-1"
data-dir = "%s"
peer-urls = "http://127.0.0.1:%d"
initial-cluster = "server-master-1=http://127.0.0.1:%d,server-master-2=http://127.0.0.1:%d"`
	cfgStr := fmt.Sprintf(cfgTpl, ports[0], ports[0], ports[0], ports[0], dir, ports[1], ports[1], ports[2])
	cfg := GetDefaultMasterConfig()
	err = cfg.configFromString(cfgStr)
	require.Nil(t, err)
	err = cfg.Adjust()
	require.Nil(t, err)

	s := &Server{cfg: cfg}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = s.startGrpcSrv(ctx)
	}()
	// sleep a short time to ensure embed etcd is being started
	time.Sleep(time.Millisecond * 100)
	cancel()
	wg.Wait()
	require.EqualError(t, err, context.Canceled.Error())
}

func testPprof(t *testing.T, addr string) {
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
	for _, uri := range urls {
		resp, err := http.Get(addr + uri)
		require.Nil(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		_, err = ioutil.ReadAll(resp.Body)
		require.Nil(t, err)
	}
}

func testPrometheusMetrics(t *testing.T, addr string) {
	resp, err := http.Get(addr + "/metrics")
	require.Nil(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, err = ioutil.ReadAll(resp.Body)
	require.Nil(t, err)
}

// Server master requires etcd/gRPC service as the minimum running environment,
// this case
// - starts an embed etcd with gRPC service, including message service and
//   server master pb service.
// - campaigns to be leader and then runs leader service.
// Disable parallel run for this case, because prometheus http handler will meet
// data race if parallel run is enabled
// FIXME: disable this test temporary for no proper mock of frame metastore
// nolint: deadcode
func testRunLeaderService(t *testing.T) {
	_, cfg := prepareServerEnv(t, "test-run-leader-service")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := NewServer(cfg, nil)
	require.Nil(t, err)

	// meta operation fail:context deadline exceeded
	_ = s.registerMetaStore()

	err = s.startResourceManager()
	require.NoError(t, err)

	err = s.startGrpcSrv(ctx)
	require.Nil(t, err)

	sessionCfg, err := s.generateSessionConfig()
	require.Nil(t, err)
	session, err := cluster.NewEtcdSession(ctx, s.etcdClient, sessionCfg)
	require.Nil(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.msgService.GetMessageServer().Run(ctx)
	}()

	_, _, err = session.Campaign(ctx, time.Second)
	require.Nil(t, err)

	ctx1, cancel1 := context.WithTimeout(ctx, time.Second)
	defer cancel1()
	err = s.runLeaderService(ctx1)
	require.EqualError(t, err, context.DeadlineExceeded.Error())

	// runLeaderService exits, try to campaign to be leader and run leader servcie again
	_, _, err = session.Campaign(ctx, time.Second)
	require.Nil(t, err)
	ctx2, cancel2 := context.WithTimeout(ctx, time.Second)
	defer cancel2()
	err = s.runLeaderService(ctx2)
	require.EqualError(t, err, context.DeadlineExceeded.Error())

	cancel()
	wg.Wait()
}

type mockJobManager struct {
	framework.BaseMaster
	jobMu sync.RWMutex
	jobs  map[pb.QueryJobResponse_JobStatus]int
}

func (m *mockJobManager) JobCount(status pb.QueryJobResponse_JobStatus) int {
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	return m.jobs[status]
}

func (m *mockJobManager) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse {
	panic("not implemented")
}

func (m *mockJobManager) QueryJob(ctx context.Context, req *pb.QueryJobRequest) *pb.QueryJobResponse {
	panic("not implemented")
}

func (m *mockJobManager) CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse {
	panic("not implemented")
}

func (m *mockJobManager) PauseJob(ctx context.Context, req *pb.PauseJobRequest) *pb.PauseJobResponse {
	panic("not implemented")
}

func (m *mockJobManager) DebugJob(ctx context.Context, req *pb.DebugJobRequest) *pb.DebugJobResponse {
	panic("not implemented")
}

func (m *mockJobManager) GetJobStatuses(ctx context.Context) (map[frameModel.MasterID]frameModel.MasterStatusCode, error) {
	panic("not implemented")
}

func (m *mockJobManager) WatchJobStatuses(
	ctx context.Context,
) (manager.JobStatusesSnapshot, *notifier.Receiver[manager.JobStatusChangeEvent], error) {
	// TODO implement me
	panic("implement me")
}

type mockExecutorManager struct {
	executorMu sync.RWMutex
	count      map[model.ExecutorStatus]int
}

func (m *mockExecutorManager) WatchExecutors(
	ctx context.Context,
) ([]model.ExecutorID, *notifier.Receiver[model.ExecutorStatusChange], error) {
	panic("implement me")
}

func (m *mockExecutorManager) GetAddr(executorID model.ExecutorID) (string, bool) {
	panic("implement me")
}

func (m *mockExecutorManager) CapacityProvider() scheduler.CapacityProvider {
	panic("implement me")
}

func (m *mockExecutorManager) HandleHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	panic("not implemented")
}

func (m *mockExecutorManager) AllocateNewExec(req *pb.RegisterExecutorRequest) (*model.NodeInfo, error) {
	panic("not implemented")
}

func (m *mockExecutorManager) RegisterExec(info *model.NodeInfo) {
	panic("not implemented")
}

func (m *mockExecutorManager) Start(ctx context.Context) {
	panic("not implemented")
}

func (m *mockExecutorManager) Stop() {
	panic("not implemented")
}

func (m *mockExecutorManager) HasExecutor(executorID string) bool {
	panic("not implemented")
}

func (m *mockExecutorManager) ListExecutors() []string {
	panic("not implemented")
}

func (m *mockExecutorManager) ExecutorCount(status model.ExecutorStatus) int {
	m.executorMu.RLock()
	defer m.executorMu.RUnlock()
	return m.count[status]
}

func TestCollectMetric(t *testing.T) {
	masterAddr, cfg := prepareServerEnv(t, "test-collect-metric")

	s := &Server{
		cfg:     cfg,
		metrics: newServerMasterMetric(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	err := s.startGrpcSrv(ctx)
	require.Nil(t, err)

	jobManager := &mockJobManager{
		jobs: map[pb.QueryJobResponse_JobStatus]int{
			pb.QueryJobResponse_online: 3,
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
	apiURL := fmt.Sprintf("http://%s", masterAddr)
	testCustomedPrometheusMetrics(t, apiURL)
	s.Stop()
	cancel()
}

func testCustomedPrometheusMetrics(t *testing.T, addr string) {
	require.Eventually(t, func() bool {
		resp, err := http.Get(addr + "/metrics")
		require.Nil(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body, err := ioutil.ReadAll(resp.Body)
		require.Nil(t, err)
		metric := string(body)
		return strings.Contains(metric, "dataflow_server_master_job_num") &&
			strings.Contains(metric, "dataflow_server_master_executor_num")
	}, time.Second, time.Millisecond*20)
}
