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
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/spf13/cobra"
)

// NewCmdCli creates the `cli` command.
func NewCmdCli() *cobra.Command {
	// Bind the certificate and log options.
	cf := factory.NewClientFlags()

	cmds := &cobra.Command{
		Use:   "cli",
		Short: "Manage replication task and TiCDC cluster",
		Args:  cobra.NoArgs,
	}

	// Binding the `cli` command flags.
	cf.AddFlags(cmds)
	cmds.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		// Here we will initialize the logging configuration and set the current default context.
		cancel := util.InitCmd(cmd, &logutil.Config{Level: cf.GetLogLevel()})
		util.LogHTTPProxies()
		// A notify that complete immediately, it skips the second signal essentially.
		doneNotify := func() <-chan struct{} {
			done := make(chan struct{})
			close(done)
			return done
		}
		util.InitSignalHandling(doneNotify, cancel)

		util.CheckErr(cf.CompleteAuthParameters(cmd))
	}

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
