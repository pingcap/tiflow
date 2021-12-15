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
	"os"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/spf13/cobra"
)

func newTsoCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "tso",
		Args:  cobra.NoArgs,
		Short: "Manage tso",
	}
	command.AddCommand(
		newQueryTsoCommand(),
	)
	return command
}

func newQueryTsoCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "query",
		Args:  cobra.NoArgs,
		Short: "Get tso from PD",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext
			ts, logic, err := pdCli.GetTS(ctx)
			if err != nil {
				return err
			}
			cmd.Println(oracle.ComposeTS(ts, logic))
			return nil
		},
	}
	command.SetOut(os.Stdout)
	command.SetErr(os.Stdout)
	return command
}
