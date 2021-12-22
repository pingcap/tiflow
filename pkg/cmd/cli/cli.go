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
	"io"
	"os"

	"github.com/chzyer/readline"
	"github.com/mattn/go-shellwords"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/spf13/cobra"
)

// options defines flags for the `cli` command.
type options struct {
	interact bool
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
	c.PersistentFlags().BoolVarP(&o.interact, "interact", "i", false, "Run cdc cli with readline")
}

// NewCmdCli creates the `cli` command.
func NewCmdCli() *cobra.Command {
	// Bind the certificate and log options.
	cf := factory.NewClientFlags()

	o := newOptions()

	cmds := &cobra.Command{
		Use:   "cli",
		Short: "Manage replication task and TiCDC cluster",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Here we will initialize the logging configuration and set the current default context.
			util.InitCmd(cmd, &logutil.Config{Level: cf.GetLogLevel()})
			util.LogHTTPProxies()
			return nil
		},
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			// Whether to run interactively or not.
			if o.interact {
				run()
			}
		},
	}

	// Binding the `cli` command flags.
	o.addFlags(cmds)
	cf.AddFlags(cmds)

	// Construct the client construction factory.
	f := factory.NewFactory(cf)

	// Add subcommands.
	cmds.AddCommand(newCmdCapture(f))
	cmds.AddCommand(newCmdChangefeed(f))
	cmds.AddCommand(newCmdProcessor(f))
	cmds.AddCommand(newCmdTso(f))
	cmds.AddCommand(newCmdUnsafe(f))

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
