package executor

import (
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

func debugHandler(lis net.Listener) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	httpS := &http.Server{
		Handler: mux,
	}
	err := httpS.Serve(lis)
	if err != nil && !common.IsErrNetClosing(err) && err != http.ErrServerClosed {
		log.L().Error("debug server returned", log.ShortError(err))
	}
	return err
}
