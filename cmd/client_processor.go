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
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/spf13/cobra"
)

func newProcessorCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "processor",
		Args:  cobra.NoArgs,
		Short: "Manage processor (processor is a sub replication task running on a specified capture)",
	}
	command.AddCommand(
		newListProcessorCommand(),
		newQueryProcessorCommand(),
	)
	return command
}

func newListProcessorCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Args:  cobra.NoArgs,
		Short: "List all processors in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext
			info, err := cdcEtcdCli.GetProcessors(ctx)
			if err != nil {
				return err
			}
			return jsonPrint(cmd, info)
		},
	}
	return command
}

func newQueryProcessorCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "query",
		Args:  cobra.NoArgs,
		Short: "Query information and status of a sub replication task (processor)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext
			_, status, err := cdcEtcdCli.GetTaskStatus(ctx, changefeedID, captureID)
			if err != nil && cerror.ErrTaskStatusNotExists.Equal(err) {
				return err
			}
			_, position, err := cdcEtcdCli.GetTaskPosition(ctx, changefeedID, captureID)
			if err != nil && cerror.ErrTaskPositionNotExists.Equal(err) {
				return err
			}
			meta := &processorMeta{Status: status, Position: position}
			return jsonPrint(cmd, meta)
		},
	}
	command.PersistentFlags().StringVarP(&changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	command.PersistentFlags().StringVarP(&captureID, "capture-id", "p", "", "Capture ID")
	_ = command.MarkPersistentFlagRequired("changefeed-id")
	_ = command.MarkPersistentFlagRequired("capture-id")
	return command
}
