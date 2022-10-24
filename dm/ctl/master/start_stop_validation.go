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

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/spf13/cobra"
)

const (
	StartValidationOp = "start"
	StopValidationOp  = "stop"
)

func NewStartValidationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start [--all-task] [task-name]",
		Short: "start to validate the completeness of the data",
		RunE:  startValidation,
	}
	cmd.Flags().Bool("all-task", false, "whether applied to all tasks")
	cmd.Flags().String("mode", "full", "specify the mode of validation: full (default), fast; this flag will be ignored if the validation task has been ever enabled but currently paused")
	cmd.Flags().String("start-time", "", "specify the start time of binlog for validation, e.g. '2021-10-21 00:01:00' or 2021-10-21T00:01:00")
	return cmd
}

func NewStopValidationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop [--all-task] [task-name]",
		Short: "stop validating the completeness of the data",
		RunE:  stopValidation,
	}
	cmd.Flags().Bool("all-task", false, "whether applied to all tasks")
	return cmd
}

type validationStartStopArgs struct {
	sources  []string
	allTask  bool
	taskName string

	mode      string
	startTime string
	// whether user has set --mode or --start-time explicitly
	explicitMode      bool
	explicitStartTime bool
}

func printUsageAndFailWithMessage(cmd *cobra.Command, errMsg string) error {
	cmd.SetOut(os.Stdout)
	common.PrintCmdUsage(cmd)
	return errors.New(errMsg)
}

func parseValidationStartStopArgs(cmd *cobra.Command, op string) (validationStartStopArgs, string, bool) {
	var err error
	args := validationStartStopArgs{}
	if args.sources, err = common.GetSourceArgs(cmd); err != nil {
		return args, err.Error(), false
	}
	if args.allTask, err = cmd.Flags().GetBool("all-task"); err != nil {
		return args, err.Error(), false
	}

	if op == StartValidationOp {
		args.explicitMode = cmd.Flags().Changed("mode")
		if args.explicitMode {
			args.mode, err = cmd.Flags().GetString("mode")
			if err != nil {
				return args, err.Error(), false
			}
			if args.mode != config.ValidationFull && args.mode != config.ValidationFast {
				errMsg := fmt.Sprintf("mode should be either `%s` or `%s`, current is `%s`",
					config.ValidationFull, config.ValidationFast, args.mode)
				return args, errMsg, false
			}
		}
		args.explicitStartTime = cmd.Flags().Changed("start-time")
		if args.explicitStartTime {
			if args.startTime, err = cmd.Flags().GetString("start-time"); err != nil {
				return args, err.Error(), false
			}
			if _, err = utils.ParseStartTime(args.startTime); err != nil {
				return args, "start-time should be in the format like '2006-01-02 15:04:05' or '2006-01-02T15:04:05'", false
			}
		}
	}

	switch len(cmd.Flags().Args()) {
	case 1:
		args.taskName = cmd.Flags().Arg(0)
		if args.allTask {
			// contradiction
			return args, "either `task-name` or `all-task` should be set", false
		}
	case 0:
		if !args.allTask {
			// contradiction
			return args, "either `task-name` or `all-task` should be set", false
		}
	default:
		return args, "too many arguments are specified", false
	}

	return args, "", true
}

func startValidation(cmd *cobra.Command, _ []string) error {
	args, msg, ok := parseValidationStartStopArgs(cmd, StartValidationOp)
	if !ok {
		return printUsageAndFailWithMessage(cmd, msg)
	}
	// we use taskName="" to represent we do operation on all task, so args.allTask don't need to pass
	req := &pb.StartValidationRequest{
		TaskName: args.taskName,
		Sources:  args.sources,
	}
	// "validation start" has 2 usages:
	// 1. if validator never started, starts it.
	// 2. if validator ever started, resumes it, in this case user can't set mode or start-time explicitly.
	if args.explicitMode {
		req.Mode = &pb.StartValidationRequest_ModeValue{ModeValue: args.mode}
	}
	if args.explicitStartTime {
		req.StartTime = &pb.StartValidationRequest_StartTimeValue{StartTimeValue: args.startTime}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp := &pb.StartValidationResponse{}
	err := common.SendRequest(ctx, "StartValidation", req, &resp)
	if err != nil {
		return err
	}
	common.PrettyPrintResponse(resp)
	return nil
}

func stopValidation(cmd *cobra.Command, _ []string) error {
	args, msg, ok := parseValidationStartStopArgs(cmd, StopValidationOp)
	if !ok {
		return printUsageAndFailWithMessage(cmd, msg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp := &pb.StopValidationResponse{}
	err := common.SendRequest(
		ctx,
		"StopValidation",
		// we use taskName="" to represent we do operation on all task, so args.allTask don't need to pass
		&pb.StopValidationRequest{
			TaskName: args.taskName,
			Sources:  args.sources,
		},
		&resp,
	)
	if err != nil {
		return err
	}
	common.PrettyPrintResponse(resp)
	return nil
}
