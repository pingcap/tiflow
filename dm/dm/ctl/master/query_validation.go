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
	"os"

	"github.com/pingcap/errors"

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
)

const (
	ValidationStageRunning = "running"
	ValidationStageStopped = "stopped"
	ValidationAllErr       = "all"
	ValidationIgnoredErr   = "ignored"
)

func NewQueryValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show-errors [--error] <task-name>",
		Short: "show error of the validation task",
		RunE:  queryValidationError,
	}
	cmd.Flags().String("error", "all", "filtering type of error: all (default) or ignored")
	return cmd
}

func queryValidationError(cmd *cobra.Command, _ []string) (err error) {
	var (
		errType  string
		taskName string
	)
	if len(cmd.Flags().Args()) != 1 {
		return errors.New("task name should be specified")
	}
	taskName = cmd.Flags().Arg(0)
	errType, err = cmd.Flags().GetString("error")
	if err != nil {
		return err
	}
	if errType != ValidationAllErr && errType != ValidationIgnoredErr {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.Errorf("error flag should be either `%s` or `%s`", ValidationAllErr, ValidationIgnoredErr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp := &pb.GetValidationErrorResponse{}
	err = common.SendRequest(
		ctx,
		"GetValidationError",
		&pb.GetValidationErrorRequest{
			ErrType:  errType,
			TaskName: taskName,
		},
		&resp,
	)
	if err != nil {
		return err
	}
	common.PrettyPrintResponse(resp)
	return nil
}

func NewQueryValidationStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status <task-name>",
		Short: "query validation status of a task",
		RunE:  queryValidationStatus,
	}
	cmd.Flags().String("stage", "running", "filter status")
	return cmd
}

func queryValidationStatus(cmd *cobra.Command, _ []string) error {
	var (
		status   string
		taskName string
		err      error
	)

	if len(cmd.Flags().Args()) == 0 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("task name should be specified")
	}
	taskName = cmd.Flags().Arg(0)
	status, err = cmd.Flags().GetString("stage")
	if status != "" && status != ValidationStageRunning && status != ValidationStageStopped {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.Errorf("stage should be either `%s` or `%s`", ValidationStageRunning, ValidationStageStopped)
	}
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.GetValidationStatusResponse{}
	err = common.SendRequest(
		ctx,
		"GetValidationStatus",
		&pb.GetValidationStatusRequest{
			TaskName:     taskName,
			FilterStatus: status,
		},
		&resp,
	)
	if err != nil {
		return err
	}
	common.PrettyPrintResponse(resp)
	return nil
}
