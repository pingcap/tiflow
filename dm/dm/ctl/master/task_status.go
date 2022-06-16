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
	"net/http"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/openapi"
)

// NewTaskStatusCmd creates a task status command.
func NewTaskStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status <task-name | config-file> [-s source ...]",
		Short: "Query task status",
		RunE:  taskStatusFunc,
	}
	return cmd
}

func taskStatusFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}

	var sourceListPtr *openapi.SourceNameList
	if len(sources) != 0 {
		sourceList := openapi.SourceNameList(sources)
		sourceListPtr = &sourceList
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// get status
	getStatusReq := &openapi.DMAPIGetTaskStatusParams{
		SourceNameList: sourceListPtr,
	}

	params := []interface{}{taskName, getStatusReq}
	res := &openapi.GetTaskStatusResponse{}
	errResp := common.SendOpenapiRequest(ctx, "DMAPIGetTaskStatus", params, http.StatusOK, res)
	if errResp != nil {
		return errResp
	}
	common.PrettyPrintOpenapiResp(true, res)
	return nil
}
