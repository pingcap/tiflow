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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/spf13/cobra"
)

// NewTaskUpdateCmd creates a task update command.
func NewTaskUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <config-file>",
		Short: "Update a task by task configuration file",
		RunE:  taskUpdateFunc,
	}
	return cmd
}

func taskUpdateFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	task, err := convertToOpenapiTaskFromFile(cmd.Flags().Arg(0))
	if err != nil {
		return errors.Trace(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// update task
	updateReq := openapi.UpdateTaskRequest{
		Task: *task,
	}
	params := []interface{}{task.Name, updateReq}
	resp := &openapi.OperateTaskResponse{}
	errResp := common.SendOpenapiRequest(ctx, "DMAPIUpdateTask", params, http.StatusOK, resp)
	if errResp != nil {
		return errResp
	}
	common.PrettyPrintOpenapiResp(true, resp)
	return nil
}
