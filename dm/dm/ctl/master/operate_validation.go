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

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
)

func NewIgnoreValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ignore-error <task-name> <error-id|--all>",
		Short: "operate validation error",
		RunE:  ignoreError,
	}
	cmd.Flags().Bool("all", false, "all task")
	return cmd
}

func NewResolveValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "make-resolve <task-name> <error-id|--all>",
		Short: "operate validation error",
		RunE:  resolveError,
	}
	cmd.Flags().Bool("all", false, "all task")
	return cmd
}

func NewClearValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clear <task-name> <error-id|--all>",
		Short: "operate validation error",
		RunE:  clearError,
	}
	cmd.Flags().Bool("all", false, "all task")
	return cmd
}

func getFlags(cmd *cobra.Command) (taskName string, errID int, isAll bool, err error) {
	if len(cmd.Flags().Args()) < 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return "", -1, false, errors.New("task name should be specified")
	}
	var errIDStr string
	taskName = cmd.Flags().Arg(0)
	if len(cmd.Flags().Args()) > 1 {
		errIDStr = cmd.Flags().Arg(1)
		errID, err = strconv.Atoi(errIDStr)
		if err != nil {
			return "", -1, false, errors.New("error-id not valid")
		}
	} else {
		errID = -1
	}
	isAll, err = cmd.Flags().GetBool("all")
	if err != nil {
		return "", -1, false, err
	}
	if (errID < 0 && !isAll) || (errID > 0 && isAll) {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return "", -1, false, errors.New("either `--all` or `error-id` should be set")
	}
	return taskName, errID, isAll, nil
}

func operateError(taskName string, errID int, isAll bool, op string) (resp *pb.OperateValidationErrorResponse, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp = &pb.OperateValidationErrorResponse{}
	err = common.SendRequest(
		ctx,
		"OperateValidationError",
		&pb.OperateValidationErrorRequest{
			Op:         op,
			TaskName:   taskName,
			Id:         int32(errID),
			IsAllError: isAll,
		},
		&resp,
	)
	return resp, err
}

func ignoreError(cmd *cobra.Command, _ []string) (err error) {
	var (
		taskName string
		errID    int
		isAll    bool
		resp     *pb.OperateValidationErrorResponse
	)
	taskName, errID, isAll, err = getFlags(cmd)
	if err != nil {
		return err
	}
	resp, err = operateError(taskName, errID, isAll, "ignore")
	if err != nil {
		return err
	}
	common.PrettyPrintResponse(resp)
	return nil
}

func resolveError(cmd *cobra.Command, _ []string) (err error) {
	var (
		taskName string
		errID    int
		isAll    bool
		resp     *pb.OperateValidationErrorResponse
	)
	taskName, errID, isAll, err = getFlags(cmd)
	if err != nil {
		return err
	}
	resp, err = operateError(taskName, errID, isAll, "resolve")
	if err != nil {
		return err
	}
	common.PrettyPrintResponse(resp)
	return nil
}

func clearError(cmd *cobra.Command, _ []string) (err error) {
	var (
		taskName string
		errID    int
		isAll    bool
		resp     *pb.OperateValidationErrorResponse
	)
	taskName, errID, isAll, err = getFlags(cmd)
	if err != nil {
		return err
	}
	resp, err = operateError(taskName, errID, isAll, "clear")
	if err != nil {
		return err
	}
	common.PrettyPrintResponse(resp)
	return nil
}
