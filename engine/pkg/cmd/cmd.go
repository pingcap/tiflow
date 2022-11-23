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

package cmd

import (
	"os"

	"github.com/pingcap/tiflow/engine/pkg/cmd/cli"
	"github.com/pingcap/tiflow/engine/pkg/cmd/executor"
	"github.com/pingcap/tiflow/engine/pkg/cmd/master"
	"github.com/pingcap/tiflow/engine/pkg/cmd/version"
	"github.com/spf13/cobra"
)

// NewCmd creates the root command.
func NewCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "tiflow",
		Short: "tiflow",
		Long:  `dataflow engine`,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
}

// Run runs the root command.
func Run() {
	cmd := NewCmd()

	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)

	cmd.AddCommand(version.NewCmdVersion())
	cmd.AddCommand(master.NewCmdMaster())
	cmd.AddCommand(executor.NewCmdExecutor())
	cmd.AddCommand(cli.NewCmdCli())

	if err := cmd.Execute(); err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
}
