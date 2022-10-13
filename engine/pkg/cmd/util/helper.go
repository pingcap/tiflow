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

package util

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	cmdconetxt "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// InitCmd initializes the default context and returns its cancel function.
func InitCmd(cmd *cobra.Command) context.CancelFunc {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sig := <-sc
		log.Info("got signal to exit", zap.Stringer("signal", sig))
		cancel()
	}()

	cmdconetxt.SetDefaultContext(ctx)

	return cancel
}
