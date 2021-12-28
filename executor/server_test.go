package executor

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestStartTCPSrv(t *testing.T) {
	t.Parallel()

	err := log.InitLogger(&log.Config{Level: "warn"})
	require.Nil(t, err)

	cfg := NewConfig()
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg.WorkerAddr = addr
	s := NewServer(cfg, nil)

	wg, ctx := errgroup.WithContext(context.Background())
	err = s.startTCPService(ctx, wg)
	require.Nil(t, err)

	testPprof(t, fmt.Sprintf("http://127.0.0.1:%d", port))
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
