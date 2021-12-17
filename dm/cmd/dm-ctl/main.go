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
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/tiflow/dm/dm/ctl"
	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

func main() {
	// now, we use checker in dmctl while it using some pkg which log some thing when running
	// to make dmctl output more clear, simply redirect log to file rather output to stdout
	err := log.InitLogger(&log.Config{
		File:  "dmctl.log",
		Level: "info",
	})
	if err != nil {
		common.PrintLinesf("init logger error %s", terror.Message(err))
		os.Exit(2)
	}

	utils.LogHTTPProxies(false)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		switch sig {
		case syscall.SIGTERM:
			os.Exit(0)
		default:
			os.Exit(1)
		}
	}()

	ctl.MainStart(common.AdjustArgumentsForPflags(os.Args[1:]))
}
