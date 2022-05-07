package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/servermaster"
)

// 1. parse config
// 2. init logger
// 3. print log
// 4. start server
func main() {
	// 1. parse config
	cfg := servermaster.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		fmt.Print(err)
		os.Exit(2)
	}

	// 2. init logger
	err = log.InitLogger(&log.Config{
		File:   cfg.LogFile,
		Level:  strings.ToLower(cfg.LogLevel),
		Format: cfg.LogFormat,
	})
	if err != nil {
		os.Exit(2)
	}

	// 3. start server
	ctx, cancel := context.WithCancel(context.Background())
	server, err := servermaster.NewServer(cfg, nil)
	if err != nil {
		log.L().Error("fail to start dataflow master", zap.Error(err))
		os.Exit(2)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		select {
		case <-ctx.Done():
		case sig := <-sc:
			log.L().Info("got signal to exit", zap.Stringer("signal", sig))
			cancel()
		}
	}()

	err = server.Run(ctx)
	if err != nil && errors.Cause(err) != context.Canceled {
		log.L().Error("run dataflow master with error", zap.Error(err))
		os.Exit(2)
	}
	log.L().Info("server exits normally")
}
