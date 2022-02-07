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

func NewStartValidationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start [-s source ...] [--all-task] <task-name>",
		Short: "start to validate the completeness of the data",
		RunE:  startValidation,
	}
	cmd.Flags().Bool("all-task", false, "whether the validator applied to all tasks")
	cmd.Flags().String("from-time", "", "specify a starting time to validate")
	cmd.Flags().String("mode", "full", "specify the mode of validation: full, fast")
	return cmd
}

func NewStopValidationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop [-s source ...] [--all-task] <task-name>",
		Short: "start to validate the completeness of the data",
		RunE:  stopValidation,
	}
	cmd.Flags().Bool("all-task", false, "whether the validator applied to all tasks")
	return cmd
}

func startValidation(cmd *cobra.Command, _ []string) (err error) {
	var (
		sources   []string
		timeStart string
		mode      string
		isAllTask bool
		taskName  string
	)
	mode, err = cmd.Flags().GetString("mode")
	if err != nil {
		return err
	}
	if mode != "full" && mode != "fast" {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("mode should be either `full` or `fast`")
	}
	sources, err = common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}
	timeStart, err = cmd.Flags().GetString("from-time")
	if err != nil {
		return err
	}
	isAllTask, err = cmd.Flags().GetBool("all-task")
	if err != nil {
		return err
	}
	if len(cmd.Flags().Args()) > 0 {
		taskName = cmd.Flags().Arg(0)
	}

	// contradiction
	if (len(taskName) > 0 && isAllTask) || (len(taskName) == 0 && !isAllTask) {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("either `task-name` or `all-task` should be set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start validation
	resp := &pb.StartValidationResponse{}
	err = common.SendRequest(
		ctx,
		"StartValidation",
		&pb.StartValidationRequest{
			Mode:      mode,
			FromTime:  timeStart,
			IsAllTask: isAllTask,
			Sources:   sources,
			TaskName:  taskName,
		},
		&resp,
	)
	if err != nil {
		return err
	}
	common.PrettyPrintResponse(resp)
	return nil
}

func stopValidation(cmd *cobra.Command, _ []string) (err error) {
	var (
		sources   []string
		isAllTask bool
		taskName  string
	)
	sources, err = common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}
	isAllTask, err = cmd.Flags().GetBool("all-task")
	if err != nil {
		return err
	}
	if len(cmd.Flags().Args()) > 0 {
		taskName = cmd.Flags().Arg(0)
	}
	// contradiction
	if (len(taskName) > 0 && isAllTask) || (len(taskName) == 0 && !isAllTask) {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("either `task-name` or `all-task` should be set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	return nil
}
