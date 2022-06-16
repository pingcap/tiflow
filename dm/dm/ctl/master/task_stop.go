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

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/openapi"
)

// NewTaskStopCmd creates a Task Stop command.
func NewTaskStopCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop [task-name | config-file] [-s source ...] [-t duration]",
		Short: "Stop a task with specified conditions",
		RunE:  taskStopFunc,
	}
	addSourceTaskOperateFlags(cmd)
	cmd.Flags().StringP("timeout", "t", "", "specify timeout duration waiting task stop, e.g. '10s'")
	return cmd
}

func taskStopFunc(cmd *cobra.Command, _ []string) (err error) {
	return taskOperateFunc(cmd, taskStopOperateFunc)
}

func taskStopOperateFunc(cmd *cobra.Command, taskName string, sources []string) *taskOperateResult {
	taskResult := &taskOperateResult{Task: taskName, Op: "stop"}
	timeOut, err := cmd.Flags().GetString("timeout")
	if err != nil {
		taskResult.Result = false
		taskResult.Msg = err.Error()
		return taskResult
	}

	sourceList := openapi.SourceNameList(sources)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopReq := openapi.StopTaskRequest{
		SourceNameList:  &sourceList,
		TimeoutDuration: &timeOut,
	}
	params := []interface{}{taskName, stopReq}
	sendErr := common.SendOpenapiRequest(ctx, "DMAPIStopTask", params, http.StatusOK, nil)
	if sendErr != nil {
		taskResult.Result = false
		taskResult.Msg = err.Error()
	} else {
		taskResult.Result = true
		taskResult.Msg = "Stop task " + taskName + " success."
	}
	return taskResult
}
