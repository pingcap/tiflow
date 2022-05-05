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
	"strings"

	"github.com/pingcap/errors"

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
)

const (
	ValidationAllErr         = "all"
	ValidationIgnoredErr     = "ignored"
	ValidationUnprocessedErr = "unprocessed"
	ValidationResolvedErr    = "resolved"
)

var mapStr2ErrState = map[string]pb.ValidateErrorState{
	ValidationAllErr:         pb.ValidateErrorState_InvalidErr,
	ValidationIgnoredErr:     pb.ValidateErrorState_IgnoredErr,
	ValidationUnprocessedErr: pb.ValidateErrorState_NewErr,
	ValidationResolvedErr:    pb.ValidateErrorState_ResolvedErr,
}

func NewQueryValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show-errors [--error error-state] <task-name>",
		Short: "show error of the validation task",
		RunE:  queryValidationError,
	}
	cmd.Flags().String("error", ValidationUnprocessedErr, "filtering type of error: all, ignored, or unprocessed")
	return cmd
}

func queryValidationError(cmd *cobra.Command, _ []string) (err error) {
	var (
		errState   string
		taskName   string
		pbErrState pb.ValidateErrorState
		ok         bool
	)
	if len(cmd.Flags().Args()) != 1 {
		return errors.New("task name should be specified")
	}
	taskName = cmd.Flags().Arg(0)
	errState, err = cmd.Flags().GetString("error")
	if err != nil {
		return err
	}
	if pbErrState, ok = mapStr2ErrState[errState]; !ok || errState == ValidationResolvedErr {
		// todo: support querying resolved error?
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.Errorf("error flag should be either `%s`, `%s`, or `%s`", ValidationAllErr, ValidationIgnoredErr, ValidationUnprocessedErr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp := &pb.GetValidationErrorResponse{}
	err = common.SendRequest(
		ctx,
		"GetValidationError",
		&pb.GetValidationErrorRequest{
			ErrState: pbErrState, // using InvalidValidateError to represent `all``
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
		Use:   "status [--table-stage stage] <task-name>",
		Short: "query validation status of a task",
		RunE:  queryValidationStatus,
	}
	cmd.Flags().String("table-stage", "", "filter validation tables by stage: running/stopped")
	return cmd
}

func queryValidationStatus(cmd *cobra.Command, _ []string) error {
	var (
		stage    string
		taskName string
		err      error
	)

	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("task name should be specified")
	}
	taskName = cmd.Flags().Arg(0)
	stage, err = cmd.Flags().GetString("table-stage")
	if err != nil {
		return err
	}
	if stage != "" && stage != strings.ToLower(pb.Stage_Running.String()) && stage != strings.ToLower(pb.Stage_Stopped.String()) {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.Errorf(
			"stage should be either `%s` or `%s`",
			strings.ToLower(pb.Stage_Running.String()),
			strings.ToLower(pb.Stage_Stopped.String()),
		)
	}
	var pbStage pb.Stage
	switch stage {
	case "":
		// use invalid stage to represent `all` stages
		pbStage = pb.Stage_InvalidStage
	case strings.ToLower(pb.Stage_Running.String()):
		pbStage = pb.Stage_Running
	case strings.ToLower(pb.Stage_Stopped.String()):
		pbStage = pb.Stage_Stopped
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.GetValidationStatusResponse{}
	err = common.SendRequest(
		ctx,
		"GetValidationStatus",
		&pb.GetValidationStatusRequest{
			TaskName:     taskName,
			FilterStatus: pbStage,
		},
		&resp,
	)
	if err != nil {
		return err
	}
	common.PrettyPrintResponse(resp)
	return nil
}
