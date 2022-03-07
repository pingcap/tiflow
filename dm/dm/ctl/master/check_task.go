// Copyright 2019 PingCAP, Inc.
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

	"github.com/pingcap/tiflow/dm/checker"
	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
)

// NewCheckTaskCmd creates a CheckTask command.
func NewCheckTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check-task <config-file> [--error count] [--warn count]",
		Short: "Checks the configuration file of the task",
		RunE:  checkTaskFunc,
	}
	cmd.Flags().Int64P("error", "e", common.DefaultErrorCnt, "max count of errors to display")
	cmd.Flags().Int64P("warn", "w", common.DefaultWarnCnt, "max count of warns to display")
	cmd.Flags().String("start-time", "", "specify the start time of binlog replication, e.g. '2021-10-21 00:01:00' or 2021-10-21T00:01:00")
	return cmd
}

// checkTaskFunc does check task request.
func checkTaskFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	errCnt, err := cmd.Flags().GetInt64("error")
	if err != nil {
		return err
	}
	warnCnt, err := cmd.Flags().GetInt64("warn")
	if err != nil {
		return err
	}
	startTime, err := cmd.Flags().GetString("start-time")
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start task
	resp := &pb.CheckTaskResponse{}
	err = common.SendRequest(
		ctx,
		"CheckTask",
		&pb.CheckTaskRequest{
			Task:      string(content),
			ErrCnt:    errCnt,
			WarnCnt:   warnCnt,
			StartTime: startTime,
		},
		&resp,
	)

	if err != nil {
		return err
	}

	if !common.PrettyPrintResponseWithCheckTask(resp, checker.CheckTaskMsgHeader) {
		common.PrettyPrintResponse(resp)
	}
	return nil
}
