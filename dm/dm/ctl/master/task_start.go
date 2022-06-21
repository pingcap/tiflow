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
	"net/http"
	"os"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/spf13/cobra"
)

// NewTaskStartCmd creates a Task Start command.
func NewTaskStartCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start <task-name | config-file> [-s source ...] [--remove-meta] [--start-time] [--safe-mode-duration]",
		Short: "Start a task with specified conditions",
		RunE:  taskStartFunc,
	}
	cmd.Flags().BoolP("remove-meta", "", false, "whether to remove task's meta data")
	cmd.Flags().String("start-time", "", "specify the start time of binlog replication, e.g. '2021-10-21 00:01:00' or 2021-10-21T00:01:00")
	cmd.Flags().String("safe-mode-duration", "", "specify time duration of safe mode, e.g. '10s'")

	return cmd
}

func taskStartFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	arg0 := cmd.Flags().Arg(0)
	taskName := common.GetTaskNameFromArgOrFile(arg0)

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}
	sourceList := openapi.SourceNameList(sources)

	removeMeta, err := cmd.Flags().GetBool("remove-meta")
	if err != nil {
		common.PrintLinesf("error in parse `--remove-meta`")
		return err
	}
	startTime, err := cmd.Flags().GetString("start-time")
	if err != nil {
		common.PrintLinesf("error in parse `--start-time`")
		return err
	}
	safeModeTime, err := cmd.Flags().GetString("safe-mode-duration")
	if err != nil {
		common.PrintLinesf("error in parse `--safe-mode-duration`")
		return err
	}

	// check task is exist
	notExist, err := isTaskNotExist(taskName)
	if notExist && strings.HasSuffix(arg0, ".yaml") || strings.HasSuffix(arg0, ".yml") {
		// create to keep compatible start-task
		_, err := createTask(arg0)
		if err != nil {
			return errors.Trace(err)
		}
	} else if err != nil {
		return errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// start task
	startReq := openapi.StartTaskRequest{
		RemoveMeta:           &removeMeta,
		SafeModeTimeDuration: &safeModeTime,
		SourceNameList:       &sourceList,
		StartTime:            &startTime,
	}
	params := []interface{}{taskName, startReq}
	errResp := common.SendOpenapiRequest(ctx, "DMAPIStartTask", params, http.StatusOK, nil)
	if errResp != nil {
		return errResp
	}
	common.PrettyPrintOpenapiResp(true, "Start task "+taskName+" success.")
	return nil
}
