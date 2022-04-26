package executor

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/hanfei1991/microcosm/executor/worker"
)

func init() {
	err := log.InitLogger(&log.Config{Level: "warn"})
	if err != nil {
		panic(err)
	}
}

func TestStartTCPSrv(t *testing.T) {
	t.Parallel()

	cfg := NewConfig()
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg.WorkerAddr = addr
	s := NewServer(cfg, nil)

	s.grpcSrv = grpc.NewServer()
	registerMetrics()
	wg, ctx := errgroup.WithContext(context.Background())
	err = s.startTCPService(ctx, wg)
	require.Nil(t, err)

	apiURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	testPprof(t, apiURL)

	testPrometheusMetrics(t, apiURL)
	s.Stop()
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
	urls := []string{
		"/metrics",
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

func TestCollectMetric(t *testing.T) {
	wg, ctx := errgroup.WithContext(context.Background())
	cfg := NewConfig()
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg.WorkerAddr = addr
	s := NewServer(cfg, nil)
	s.taskRunner = worker.NewTaskRunner(defaultRuntimeIncomingQueueLen, defaultRuntimeInitConcurrency)

	s.grpcSrv = grpc.NewServer()
	registerMetrics()
	err = s.startTCPService(ctx, wg)
	require.Nil(t, err)

	wg.Go(func() error {
		return s.collectMetricLoop(ctx, time.Millisecond*10)
	})
	apiURL := fmt.Sprintf("http://%s", addr)
	testCustomedPrometheusMetrics(t, apiURL)
	s.Stop()
	wg.Wait()
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
		return strings.Contains(metric, "dataflow_executor_task_num")
	}, time.Second, time.Millisecond*20)
}
