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
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/executor"
	"go.uber.org/zap"
)

// 1. parse config
// 2. init logger
// 3. register singal handler
// 4. start server
func main() {
	// 1. parse config
	cfg := executor.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
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

	// 3. register signal handler
	ctx, cancel := context.WithCancel(context.Background())
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

	// 4. run executor server
	server := executor.NewServer(cfg, nil)
	if err != nil {
		log.L().Error("fail to start executor", zap.Error(err))
		os.Exit(2)
	}
	err = server.Run(ctx)
	if err != nil && errors.Cause(err) != context.Canceled {
		log.L().Error("run executor with error", zap.Error(err))
		os.Exit(2)
	}
	server.Stop()
	log.L().Info("executor server exits normally")
}
