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
	"strconv"

	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/spf13/cobra"
)

func NewIgnoreValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ignore-error <task-name> <error-id|--all>",
		Short: "ignore validation error row change",
		RunE:  operateValidationError(pb.ValidationErrOp_IgnoreErrOp),
	}
	cmd.Flags().Bool("all", false, "all errors")
	return cmd
}

func NewResolveValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resolve-error <task-name> <error-id|--all>",
		Short: "resolve validation error row change",
		RunE:  operateValidationError(pb.ValidationErrOp_ResolveErrOp),
	}
	cmd.Flags().Bool("all", false, "all errors")
	return cmd
}

func NewClearValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clear-error <task-name> <error-id|--all>",
		Short: "clear validation error row change",
		RunE:  operateValidationError(pb.ValidationErrOp_ClearErrOp),
	}
	cmd.Flags().Bool("all", false, "all errors")
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
			intErrID int
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
		if errID != "" {
			intErrID, err = strconv.Atoi(errID)
			if err != nil {
				cmd.SetOut(os.Stdout)
				common.PrintCmdUsage(cmd)
				return errors.New("`error-id` should be integer when `--all` is not set")
			}
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
				ErrId:      uint64(intErrID),
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
