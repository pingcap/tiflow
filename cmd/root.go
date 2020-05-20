// Copyright 2020 PingCAP, Inc.
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

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	logFile  string
	logLevel string
)

var rootCmd = &cobra.Command{
	Use:   "cdc",
	Short: "CDC",
	Long:  `Change Data Capture`,
}

func init() {
	cobra.OnInitialize(func() {
		err := initLog()
		if err != nil {
			fmt.Printf("fail to init log: %v", err)
			os.Exit(1)
		}
	})
	rootCmd.PersistentFlags().StringVar(&logFile, "log-file", "cdc.log", "log file path")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (etc: debug|info|warn|error)")
}

func initLog() error {
	// Init log.
	err := util.InitLogger(&util.Config{
		File:  logFile,
		Level: logLevel,
	})
	if err != nil {
		fmt.Printf("init logger error %v", errors.ErrorStack(err))
		os.Exit(1)
	}
	log.Info("init log", zap.String("file", logFile), zap.String("level", logLevel))

	return nil
}

// Execute runs the root command
func Execute() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defaultContext = ctx
	go func() {
		sig := <-sc
		log.Info("got signal to exit", zap.Stringer("signal", sig))
		cancel()
	}()

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
