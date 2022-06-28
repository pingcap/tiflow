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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/version"
	"github.com/pingcap/tiflow/engine/servermaster"
	"github.com/pingcap/tiflow/pkg/logutil"
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
	err = logutil.InitLogger(&cfg.LogConf)
	if err != nil {
		fmt.Printf("init logger failed: %s", err)
		os.Exit(2)
	}
	version.LogVersionInfo()
	if os.Getenv(gin.EnvGinMode) == "" {
		gin.SetMode(gin.ReleaseMode)
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
