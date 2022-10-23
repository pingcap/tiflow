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

package master

import (
	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/spf13/cobra"
)

// NewBinlogCmd creates a binlog command.
func NewBinlogCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "binlog <command>",
		Short: "manage or show binlog operations",
	}
	cmd.PersistentFlags().StringP("binlog-pos", "b", "", "position used to match binlog event if matched the binlog operation will be applied. The format like \"mysql-bin|000001.000003:3270\"")
	cmd.AddCommand(
		newBinlogSkipCmd(),
		newBinlogReplaceCmd(),
		newBinlogRevertCmd(),
		newBinlogInjectCmd(),
		newBinlogListCmd(),
	)

	return cmd
}

func newBinlogSkipCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "skip <task-name>",
		Short: "skip the current error event or a specific binlog position (binlog-pos) event",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return cmd.Help()
			}
			taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
			request := &pb.HandleErrorRequest{
				Op:   pb.ErrorOp_Skip,
				Task: taskName,
				Sqls: nil,
			}
			return sendHandleErrorRequest(cmd, request)
		},
	}
	return cmd
}

func newBinlogListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list <task-name>",
		Short: "list error handle command at binlog position (binlog-pos) or after binlog position (binlog-pos)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return cmd.Help()
			}
			taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
			request := &pb.HandleErrorRequest{
				Op:   pb.ErrorOp_List,
				Task: taskName,
				Sqls: nil,
			}
			return sendHandleErrorRequest(cmd, request)
		},
	}
	return cmd
}

func newBinlogReplaceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replace <task-name> <replace-sql1> <replace-sql2>...",
		Short: "replace the current error event or a specific binlog position (binlog-pos) with some ddls",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) <= 1 {
				return cmd.Help()
			}
			taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
			sqls, err := common.ExtractSQLsFromArgs(cmd.Flags().Args()[1:])
			if err != nil {
				return err
			}
			request := &pb.HandleErrorRequest{
				Op:   pb.ErrorOp_Replace,
				Task: taskName,
				Sqls: sqls,
			}
			return sendHandleErrorRequest(cmd, request)
		},
	}
	return cmd
}

func newBinlogRevertCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revert <task-name>",
		Short: "revert the current binlog operation or a specific binlog position (binlog-pos) operation",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
			request := &pb.HandleErrorRequest{
				Op:   pb.ErrorOp_Revert,
				Task: taskName,
				Sqls: nil,
			}
			return sendHandleErrorRequest(cmd, request)
		},
	}
	return cmd
}

func newBinlogInjectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inject <task-name> <inject-sql1> <inject-sql2>...",
		Short: "inject the current error event or a specific binlog position (binlog-pos) with some ddls",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) <= 1 {
				return cmd.Help()
			}
			taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
			sqls, err := common.ExtractSQLsFromArgs(cmd.Flags().Args()[1:])
			if err != nil {
				return err
			}
			request := &pb.HandleErrorRequest{
				Op:   pb.ErrorOp_Inject,
				Task: taskName,
				Sqls: sqls,
			}
			return sendHandleErrorRequest(cmd, request)
		},
	}
	return cmd
}
