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
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/spf13/cobra"
)

func newCaptureCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "capture",
		Args:  cobra.NoArgs,
		Short: "Manage capture (capture is a CDC server instance)",
	}
	command.AddCommand(
		newListCaptureCommand(),
		// TODO: add resign owner command
	)
	return command
}

func newListCaptureCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Args:  cobra.NoArgs,
		Short: "List all captures in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext
			captures, err := getAllCaptures(ctx)
			if err != nil {
				return err
			}
			return jsonPrint(cmd, captures)
		},
	}
	return command
}
