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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		log.Warn("received signal to exit", zap.Stringer("signal", sig))
		switch sig {
		case syscall.SIGTERM:
			cancel()
			os.Exit(0)
		default:
			cancel()
			os.Exit(1)
		}
	}()

	var (
		upstream, downstream          string
		accounts, tables, concurrency int
		interval, testRound           int64
		cleanupOnly                   bool
	)
	cmd := &cobra.Command{
		Use:   "bank",
		Short: "bank is a testcase case that simulates bank scenarios",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			if len(upstream) == 0 || len(downstream) == 0 {
				log.Fatal("upstream and downstream should not be empty")
			}
			run(ctx, upstream, downstream, accounts, tables, concurrency, interval, testRound, cleanupOnly)
		},
	}
	cmd.PersistentFlags().StringVarP(&upstream, "upstream", "u", "", "Upstream TiDB DSN, please specify target database in DSN")
	cmd.PersistentFlags().StringVarP(&downstream, "downstream", "d", "", "Downstream TiDB DSN, please specify target database in DSN")
	cmd.PersistentFlags().Int64Var(&interval, "interval", 1000, "Interval of verify tables")
	cmd.PersistentFlags().Int64Var(&testRound, "test-round", 50000, "Total around of upstream updates")
	cmd.PersistentFlags().IntVar(&tables, "tables", 5, "The number of tables for db")
	cmd.PersistentFlags().IntVar(&accounts, "accounts", 100, "The number of Accounts for each table")
	cmd.PersistentFlags().IntVar(&concurrency, "concurrency", 10, "concurrency of transaction for each table")
	cmd.PersistentFlags().BoolVar(&cleanupOnly, "cleanup", false, "cleanup all tables used in the testcase")

	// Outputs cmd.Print to stdout.
	cmd.SetOut(os.Stdout)
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
