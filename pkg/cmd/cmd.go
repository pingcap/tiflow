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

package cmd

import (
	"os"

	"github.com/pingcap/tiflow/pkg/cmd/cli"
	"github.com/pingcap/tiflow/pkg/cmd/redo"
	"github.com/pingcap/tiflow/pkg/cmd/server"
	"github.com/pingcap/tiflow/pkg/cmd/version"
	"github.com/spf13/cobra"
)

// NewCmd creates the root command.
func NewCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cdc",
		Short: "CDC",
		Long:  `Change Data Capture`,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
}

// AddTiCDCCommandTo add all cdc subcommands to `cmd`.
// Exported for ticdc new arch.
func AddTiCDCCommandTo(cmd *cobra.Command) {
	cmd.AddCommand(server.NewCmdServer())
	cmd.AddCommand(cli.NewCmdCli())
	cmd.AddCommand(version.NewCmdVersion())
	cmd.AddCommand(redo.NewCmdRedo())
}

// Run runs the root command.
func Run() {
	cmd := NewCmd()

	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)

	AddTiCDCCommandTo(cmd)

	if err := cmd.Execute(); err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
}
