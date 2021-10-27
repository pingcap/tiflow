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
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewBinlogCmd creates a binlog command.
func NewBinlogCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "binlog <command>",
		Short: "manage upstream binlog operations",
	}
	cmd.PersistentFlags().StringP("binlog-pos", "b", "", "position used to match binlog event if matched the binlog operation will be applied. The format like \"mysql-bin|000001.000003:3270\"")
	cmd.AddCommand(
		newBinlogSkipCmd(),
		newBinlogReplaceCmd(),
		newBinlogRevertCmd(),
		newBinlogInjectCmd(),
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
			return sendHandleErrorRequest(cmd, pb.ErrorOp_Skip, taskName, nil)
		},
	}
	return cmd
}

func newBinlogReplaceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replace <task-name> <replace-sql1> <replace-sql2>...",
		Short: "replace the current error event or a specific binlog position (binlog-pos) ddl event with some ddls",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) <= 1 {
				return cmd.Help()
			}
			taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
			sqls, err := common.ExtractSQLsFromArgs(cmd.Flags().Args()[1:])
			if err != nil {
				return err
			}
			return sendHandleErrorRequest(cmd, pb.ErrorOp_Replace, taskName, sqls)
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
			return sendHandleErrorRequest(cmd, pb.ErrorOp_Revert, taskName, nil)
		},
	}
	return cmd
}

// FIXME: implement this later.
func newBinlogInjectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "inject <task-name> <inject-sql1> <inject-sql2>...",
		Short:  "inject the current error event or a specific binlog position (binlog-pos) ddl event with some ddls",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.Errorf("this function will be supported later")
		},
	}
	return cmd
}
