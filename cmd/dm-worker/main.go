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
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/worker"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

func main() {
	collate.SetNewCollationEnabledForTest(false)

	cfg := worker.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		common.PrintLinesf("parse cmd flags err: %s", terror.Message(err))
		os.Exit(2)
	}

	err = log.InitLogger(&log.Config{
		File:   cfg.LogFile,
		Format: cfg.LogFormat,
		Level:  strings.ToLower(cfg.LogLevel),
	})
	if err != nil {
		common.PrintLinesf("init logger error %s", terror.Message(err))
		os.Exit(2)
	}

	utils.LogHTTPProxies(true)

	version.LogVersionInfo("dm-worker")
	log.L().Info("", zap.Stringer("dm-worker config", cfg))

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	s := worker.NewServer(cfg)
	err = s.JoinMaster(worker.GetJoinURLs(cfg.Join))
	if err != nil {
		log.L().Info("join the cluster meet error", zap.Error(err))
		os.Exit(2)
	}

	go func() {
		sig := <-sc
		log.L().Info("got signal to exit", zap.Stringer("signal", sig))
		s.Close()
	}()
	err = s.Start()
	switch errors.Cause(err) {
	case nil:
	case terror.ErrWorkerServerClosed:
		log.L().Info("server closed")
	default:
		log.L().Error("fail to start dm-worker", zap.Error(err))
	}

	s.Close() // wait until closed
	log.L().Info("dm-worker exit")

	syncErr := log.L().Sync()
	if syncErr != nil {
		fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
	}

	if err != nil || syncErr != nil {
		os.Exit(1)
	}
}
