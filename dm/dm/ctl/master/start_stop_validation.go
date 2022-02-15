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
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
)

func NewStartValidationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start [-s source ...] [--all-task] [task-name]",
		Short: "start to validate the completeness of the data",
		RunE:  startStopValidation("start"),
	}
	cmd.Flags().Bool("all-task", false, "whether the validator applied to all tasks")
	cmd.Flags().String("mode", "full", "specify the mode of validation: full, fast")
	return cmd
}

func NewStopValidationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop [-s source ...] [--all-task] [task-name]",
		Short: "start to validate the completeness of the data",
		RunE:  startStopValidation("stop"),
	}
	cmd.Flags().Bool("all-task", false, "whether the validator applied to all tasks")
	return cmd
}

func formatStartStopValidationError(cmd *cobra.Command, errMsg string) error {
	cmd.SetOut(os.Stdout)
	common.PrintCmdUsage(cmd)
	return errors.New(errMsg)
}

func startStopValidation(typ string) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, _ []string) error {
		var (
			sources   []string
			isAllTask bool
			taskName  string
			err       error
		)
		sources, err = common.GetSourceArgs(cmd)
		if err != nil {
			return err
		}
		isAllTask, err = cmd.Flags().GetBool("all-task")
		if err != nil {
			return err
		}
		if len(cmd.Flags().Args()) == 1 {
			taskName = cmd.Flags().Arg(0)
			if isAllTask {
				// contradiction
				return formatStartStopValidationError(cmd, "either `task-name` or `all-task` should be set")
			}
		} else if len(cmd.Flags().Args()) == 0 {
			if !isAllTask {
				// contradiction
				return formatStartStopValidationError(cmd, "either `task-name` or `all-task` should be set")
			}
		} else {
			return formatStartStopValidationError(cmd, "too many arguments are specified")
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if typ == "start" {
			// TODO: get `from-time` flag
			var mode string
			mode, err = cmd.Flags().GetString("mode")
			if err != nil {
				return err
			}
			if mode != config.ValidationFull && mode != config.ValidationFast {
				errMsg := fmt.Sprintf("mode should be either `%s` or `%s`", config.ValidationFull, config.ValidationFast)
				return formatStartStopValidationError(cmd, errMsg)
			}
			resp := &pb.StartValidationResponse{}
			err = common.SendRequest(
				ctx,
				"StartValidation",
				&pb.StartValidationRequest{
					IsAllTask: isAllTask,
					TaskName:  taskName,
					Sources:   sources,
					Mode:      mode,
				},
				&resp,
			)
			if err != nil {
				return err
			}
			common.PrettyPrintResponse(resp)
		} else {
			resp := &pb.StopValidationResponse{}
			err = common.SendRequest(
				ctx,
				"StopValidation",
				&pb.StopValidationRequest{
					IsAllTask: isAllTask,
					TaskName:  taskName,
					Sources:   sources,
				},
				&resp,
			)
			if err != nil {
				return err
			}
			common.PrettyPrintResponse(resp)
		}
		return nil
	}
}
