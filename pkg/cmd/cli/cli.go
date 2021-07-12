// Copyright 2021 PingCAP, Inc.
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

package cli

import (
	"fmt"
	"github.com/pingcap/ticdc/pkg/cmd/cli/capture"
	"github.com/pingcap/ticdc/pkg/cmd/cli/changefeed"
	"github.com/pingcap/ticdc/pkg/cmd/cli/processor"
	"github.com/pingcap/ticdc/pkg/cmd/cli/tso"
	"github.com/pingcap/ticdc/pkg/cmd/cli/unsafe"
	"io"
	"os"

	"github.com/chzyer/readline"
	"github.com/mattn/go-shellwords"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/pingcap/ticdc/pkg/logutil"
	"github.com/spf13/cobra"
)

// options defines flags and other configuration parameters for the `cli` command.
type options struct {
	interact    bool
	cliLogLevel string
	cliPdAddr   string
}

// newOptions creates new options for the `cli` command.
func newOptions() *options {
	return &options{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *options) addFlags(c *cobra.Command) {
	if o == nil {
		return
	}
	c.PersistentFlags().StringVar(&o.cliPdAddr, "pd", "http://127.0.0.1:2379", "PD address, use ',' to separate multiple PDs")
	c.PersistentFlags().BoolVarP(&o.interact, "interact", "i", false, " cdc cli with readline")
	c.PersistentFlags().StringVar(&o.cliLogLevel, "log-level", "warn", "log level (etc: debug|info|warn|error)")
}

// NewCmdCli creates the `cli` command.
func NewCmdCli() *cobra.Command {
	o := newOptions()

	cmds := &cobra.Command{
		Use:   "cli",
		Short: "Manage replication task and TiCDC cluster",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Here we will initialize the logging configuration and set the current default context.
			util.InitCmd(cmd, &logutil.Config{Level: o.cliLogLevel})
			util.LogHTTPProxies()

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			// Whether to run interactively or not.
			if o.interact {
				run()
			}
		},
	}

	// Binding the `cmd` command flags.
	o.addFlags(cmds)

	// Bind the certificate options and construct the client construction factory.
	cf := util.NewCredentialFlags()
	cf.AddFlags(cmds)
	f := util.NewFactory(cf)

	// Add subcommands.
	cmds.AddCommand(capture.NewCmdCapture(f))
	cmds.AddCommand(changefeed.NewCmdChangefeed(f))
	cmds.AddCommand(processor.NewCmdProcessor(f))
	cmds.AddCommand(tso.NewCmdTso(f))
	cmds.AddCommand(unsafe.NewCmdUnsafe(f))

	return cmds
}

func run() {
	l, err := readline.NewEx(&readline.Config{
		Prompt:            "\033[31m»\033[0m ",
		HistoryFile:       "/tmp/readline.tmp",
		InterruptPrompt:   "^C",
		EOFPrompt:         "^D",
		HistorySearchFold: true,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		line, err := l.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				break
			} else if err == io.EOF {
				break
			}
			continue
		}
		if line == "exit" {
			os.Exit(0)
		}
		args, err := shellwords.Parse(line)
		if err != nil {
			fmt.Printf("parse command err: %v\n", err)
			continue
		}

		command := NewCmdCli()
		command.SetArgs(args)
		_ = command.ParseFlags(args)
		command.SetOut(os.Stdout)
		command.SetErr(os.Stdout)
		if err = command.Execute(); err != nil {
			command.Println(err)
		}
	}
}
