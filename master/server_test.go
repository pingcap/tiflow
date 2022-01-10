package master

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func init() {
	err := log.InitLogger(&log.Config{Level: "warn"})
	if err != nil {
		panic(err)
	}
}

func TestStartGrpcSrv(t *testing.T) {
	t.Parallel()

	dir, err := ioutil.TempDir("", "test-start-grpc-srv")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	ports, err := freeport.GetFreePorts(2)
	require.Nil(t, err)
	cfgTpl := `
master-addr = "127.0.0.1:%d"
advertise-addr = "127.0.0.1:%d"
[etcd]
name = "server-master-1"
data-dir = "%s"
peer-urls = "http://127.0.0.1:%d"
initial-cluster = "server-master-1=http://127.0.0.1:%d"`
	cfgStr := fmt.Sprintf(cfgTpl, ports[0], ports[0], dir, ports[1], ports[1])
	cfg := NewConfig()
	err = cfg.configFromString(cfgStr)
	require.Nil(t, err)
	err = cfg.adjust()
	require.Nil(t, err)

	s := &Server{cfg: cfg}
	ctx := context.Background()
	err = s.startGrpcSrv(ctx)
	require.Nil(t, err)

	testPprof(t, fmt.Sprintf("http://127.0.0.1:%d", ports[0]))
	s.Stop()
}

func TestStartGrpcSrvCancelable(t *testing.T) {
	t.Parallel()

	dir, err := ioutil.TempDir("", "test-start-grpc-srv-cancelable")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	ports, err := freeport.GetFreePorts(3)
	require.Nil(t, err)
	cfgTpl := `
master-addr = "127.0.0.1:%d"
advertise-addr = "127.0.0.1:%d"
[etcd]
name = "server-master-1"
data-dir = "%s"
peer-urls = "http://127.0.0.1:%d"
initial-cluster = "server-master-1=http://127.0.0.1:%d,server-master-2=http://127.0.0.1:%d"`
	cfgStr := fmt.Sprintf(cfgTpl, ports[0], ports[0], dir, ports[1], ports[1], ports[2])
	cfg := NewConfig()
	err = cfg.configFromString(cfgStr)
	require.Nil(t, err)
	err = cfg.adjust()
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
