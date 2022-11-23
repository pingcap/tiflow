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
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

func main() {
	// 1. init conf
	commonConfig := newCommonConfig()
	conf, err := commonConfig.parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		common.PrintLinesf("parse cmd flags err: %s", terror.Message(err))
		os.Exit(2)
	}

	conf.Mode = config.ModeIncrement
	conf.UseRelay = false

	// 2. init logger
	err = log.InitLogger(&log.Config{
		File:   conf.LogFile,
		Format: conf.LogFormat,
		Level:  strings.ToLower(conf.LogLevel),
	})
	if err != nil {
		common.PrintLinesf("init logger error %s", terror.Message(err))
		os.Exit(2)
	}

	// 3. print process version information
	version.LogVersionInfo("dm-syncer")
	log.L().Info("", zap.Stringer("dm-syncer conf", conf))

	sync := syncer.NewSyncer(conf, nil, nil) // do not support shard DDL for singleton syncer.
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		sig := <-sc
		log.L().Info("got signal to exit", zap.Stringer("signal", sig))
		cancel()
	}()

	// 4. start the syncer
	err = sync.Init(ctx)
	if err != nil {
		fmt.Printf("init syncer error %s", terror.Message(err))
		os.Exit(2)
	}
	pr := make(chan pb.ProcessResult, 1)
	sync.Process(ctx, pr)

	pResult := <-pr
	if len(pResult.Errors) > 0 {
		fmt.Printf("run syncer error %v", pResult.Errors)
		os.Exit(2)
	}

	// 5. close the syncer
	sync.Close()
	log.L().Info("dm-syncer exit")

	// 6. flush log
	if syncErr := log.L().Sync(); syncErr != nil {
		fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
		os.Exit(1)
	}
}
