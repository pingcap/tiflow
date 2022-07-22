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

// NewTaskListCmd creates a task list command.
func NewTaskListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list [[--filter status=Running | source=...]..][--more]",
		Short: "Query task status",
		RunE:  taskListFunc,
	}
	cmd.Flags().BoolP("more", "", false, "whether to print the detailed task information")
	cmd.Flags().StringArray("filter", nil, "filter task, support status and source")
	return cmd
}

func taskListFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) > 0 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	var stage *openapi.TaskStage
	var sourceList *openapi.SourceNameList

	filters, err := cmd.Flags().GetStringArray("filter")
	if err != nil {
		common.PrintLinesf("error in parse `--filter`")
		return err
	}

	if len(filters) > 2 {
		common.PrintLinesf("error in parse `--filter`")
		return errors.New("too many filters")
	}

	for _, filter := range filters {
		switch {
		case strings.HasPrefix(filter, "status"):
			split := strings.Split(filter, "=")
			if len(split) != 2 {
				common.PrintLinesf("error in parse `--filter status`")
				return errors.New("status format is error")
			}
			status := openapi.TaskStage(split[1])
			stage = &status
		case strings.HasPrefix(filter, "source"):
			split := strings.Split(filter, "=")
			if len(split) != 2 {
				common.PrintLinesf("error in parse `--filter source`")
				return errors.New("source format is error")
			}
			sources := openapi.SourceNameList(strings.Split(split[1], ","))
			sourceList = &sources
		default:
			common.PrintLinesf("error in parse `--filter`")
			return errors.New("invalid filter")
		}
	}

	more, err := cmd.Flags().GetBool("more")
	if err != nil {
		common.PrintLinesf("error in parse `--more`")
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()
	listReq := &openapi.DMAPIGetTaskListParams{WithStatus: &more, Stage: stage, SourceNameList: sourceList}
	params := []interface{}{listReq}
	listResp := &openapi.GetTaskListResponse{}
	sendErr := common.SendOpenapiRequest(ctx, "DMAPIGetTaskList", params, http.StatusOK, listResp)
	if sendErr != nil {
		return errors.Trace(sendErr)
	}
	common.PrettyPrintOpenapiResp(true, listResp)
	return nil
}
