// Copyright 2019 PingCAP, Inc.
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
	"strings"
	"syscall"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/master"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

func main() {
	// 1. parse config
	cfg := master.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		common.PrintLinesf("parse cmd flags err: %s", terror.Message(err))
		os.Exit(2)
	}

	// 2. init logger
	err = log.InitLogger(&log.Config{
		File:   cfg.LogFile,
		Level:  strings.ToLower(cfg.LogLevel),
		Format: cfg.LogFormat,
	})
	if err != nil {
		common.PrintLinesf("init logger error %s", terror.Message(err))
		os.Exit(2)
	}

	utils.LogHTTPProxies(true)

	// 3. print process version information
	utils.PrintInfo("dm-master", func() {
		log.L().Info("", zap.Stringer("dm-master config", cfg))
	})

	// 4. start the server
	ctx, cancel := context.WithCancel(context.Background())
	server := master.NewServer(cfg)
	err = server.Start(ctx)
	if err != nil {
		log.L().Error("fail to start dm-master", zap.Error(err))
		os.Exit(2)
	}

	// 5. wait for stopping the process
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		log.L().Info("got signal to exit", zap.Stringer("signal", sig))
		cancel()
	}()
	<-ctx.Done()

	// 6. close the server
	server.Close()
	log.L().Info("dm-master exit")

	// 7. flush log
	if syncErr := log.L().Sync(); syncErr != nil {
		fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
		os.Exit(1)
	}
}
