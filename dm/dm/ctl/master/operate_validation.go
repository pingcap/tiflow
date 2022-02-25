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

package master

import (
	"context"
	"errors"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
)

func NewIgnoreValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ignore-error <task-name> <error-id|--all>",
		Short: "ignore validation error",
		RunE:  operateValidationError(pb.ValidationErrOp_IgnoreValidationErrOp),
	}
	cmd.Flags().Bool("all", false, "all task")
	return cmd
}

func NewResolveValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "make-resolve <task-name> <error-id|--all>",
		Short: "resolve validation error",
		RunE:  operateValidationError(pb.ValidationErrOp_ResolveValidationErrOp),
	}
	cmd.Flags().Bool("all", false, "all task")
	return cmd
}

func NewClearValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clear <task-name> <error-id|--all>",
		Short: "clear validation error",
		RunE:  operateValidationError(pb.ValidationErrOp_ClearValidationErrOp),
	}
	cmd.Flags().Bool("all", false, "all task")
	return cmd
}

func operateValidationError(typ pb.ValidationErrOp) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, _ []string) error {
		var (
			resp     *pb.OperateValidationErrorResponse
			taskName string
			isAll    bool
			errID    string
			err      error
		)
		if len(cmd.Flags().Args()) < 1 {
			cmd.SetOut(os.Stdout)
			common.PrintCmdUsage(cmd)
			return errors.New("task name should be specified")
		}
		if len(cmd.Flags().Args()) > 2 {
			cmd.SetOut(os.Stdout)
			common.PrintCmdUsage(cmd)
			return errors.New("too many arguments are specified")
		}
		taskName = cmd.Flags().Arg(0)
		if len(cmd.Flags().Args()) > 1 {
			errID = cmd.Flags().Arg(1)
		}
		isAll, err = cmd.Flags().GetBool("all")
		if err != nil {
			return err
		}
		if (errID == "" && !isAll) || (errID != "" && isAll) {
			cmd.SetOut(os.Stdout)
			common.PrintCmdUsage(cmd)
			return errors.New("either `--all` or `error-id` should be set")
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resp = &pb.OperateValidationErrorResponse{}
		err = common.SendRequest(
			ctx,
			"OperateValidationError",
			&pb.OperateValidationErrorRequest{
				Op:         typ,
				TaskName:   taskName,
				ErrId:      errID,
				IsAllError: isAll,
			},
			&resp,
		)
		if err != nil {
			return err
		}
		common.PrettyPrintResponse(resp)
		return nil
	}
}
