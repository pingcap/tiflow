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
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"go.uber.org/zap"
)

// main starts to run the test case logic after MySQL, TiDB and DM have been set up.
// NOTE: run this in the same K8s namespace as DM-master.
func main() {
	code := 0
	defer func() {
		os.Exit(code)
	}()

	cfg := newConfig()
	err := cfg.parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		return
	default:
		fmt.Println("parse cmd flags err:", err.Error())
		code = 2
		return
	}

	err = logutil.InitLogger(&logutil.Config{
		Level: "info",
	})
	if err != nil {
		fmt.Println("init logger error:", err.Error())
		code = 2
		return
	}

	go func() {
		//nolint:errcheck,gosec
		http.ListenAndServe("0.0.0.0:8899", nil) // for pprof
	}()

	rand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		log.Info("got signal to exit", zap.Stringer("signal", sig))
		cancel()
	}()

	// run tests cases
	err = runCases(ctx, cfg)
	if err != nil {
		log.Error("run cases failed", zap.Error(err))
		code = 2
		return
	}
}
