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

// Reference: https://dzone.com/articles/measuring-integration-test-coverage-rate-in-pouchc

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"

	"go.uber.org/zap"
)

func TestRunMain(_ *testing.T) {
	fmt.Println("dm-worker startup", time.Now())
	var (
		args   []string
		exit   = make(chan int)
		waitCh = make(chan interface{}, 1)
	)
	for _, arg := range os.Args {
		switch {
		case arg == "DEVEL":
		case strings.HasPrefix(arg, "-test."):
		default:
			args = append(args, arg)
		}
	}

	// golang cover tool rewrites ast with coverage annotations based on block.
	// whenever the cover tool detects the first line of one block has runned,
	// the coverage counter will be added. So we mock of `utils.OsExit` is able
	// to collect the code run. While if we run `main()` in the main routine,
	// when a code block from `main()` executes `utils.OsExit` and we forcedly
	// exit the program by `os.Exit(0)` in other routine, the coverage counter
	// fails to add for this block, the different behavior of these two scenarios
	// comes from the difference between `os.Exit` and return from a function call.
	oldOsExit := utils.OsExit
	defer func() { utils.OsExit = oldOsExit }()
	utils.OsExit = func(code int) {
		log.L().Info("os exits", zap.Int("exit code", code))
		exit <- code
		// sleep here to prevent following code execution in the caller routine
		time.Sleep(time.Second * 60)
	}

	os.Args = args
	go func() {
		main()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		fmt.Println("dm-worker exit", time.Now())
		return
	case <-exit:
		fmt.Println("dm-worker exit", time.Now())
		return
	}
}
