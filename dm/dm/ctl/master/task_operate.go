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
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/spf13/cobra"
)

const (
	operateBatchSizeFlag    = "batch-size"
	defaultOperateBatchSize = 5
)

type batchTaskOperateResult struct {
	Result bool                 `json:"result"`
	Msg    string               `json:"msg"`
	Tasks  []*taskOperateResult `json:"tasks"`
}

type taskOperateResult struct {
	Task   string      `json:"task"`
	Op     string      `json:"op"`
	Result bool        `json:"result"`
	Msg    interface{} `json:"msg"`
}

func taskOperateFunc(cmd *cobra.Command, operateFunc func(cmd *cobra.Command, taskName string, sources []string) *taskOperateResult) error {
	argLen := len(cmd.Flags().Args())
	if argLen == 0 {
		// may want to operate tasks bound to a source
		return sourceTaskOperateFunc(cmd, operateFunc)
	} else if argLen > 1 {
		// can pass at most one task-name/task-conf
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}

	// check task is exist
	notExist, err := isTaskNotExist(taskName)
	if notExist || err != nil {
		return err
	}

	common.PrettyPrintInterface(operateFunc(cmd, taskName, sources))
	return nil
}

func addSourceTaskOperateFlags(cmd *cobra.Command) {
	// control workload to dm-cluster for sources with large number of tasks.
	cmd.Flags().Int(operateBatchSizeFlag, defaultOperateBatchSize, "batch size when operating all (sub)tasks bound to a source")
}

func parseSourceTaskOperateParams(cmd *cobra.Command) (string, int, error) {
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return "", 0, err
	}
	if len(sources) == 0 {
		common.PrintLinesf(`must give one source-name when task-name/task-conf is not specified`)
		return "", 0, errors.New("missing source")
	} else if len(sources) > 1 {
		common.PrintLinesf(`can give only one source-name when task-name/task-conf is not specified`)
		return "", 0, errors.New("too many source")
	}
	batchSize, err := cmd.Flags().GetInt(batchSizeFlag)
	if err != nil {
		common.PrintLinesf("error in parse `--" + batchSizeFlag + "`")
		return "", 0, err
	}
	return sources[0], batchSize, nil
}

func sourceTaskOperateFunc(cmd *cobra.Command,
	operateFunc func(cmd *cobra.Command, taskName string, sources []string) *taskOperateResult) error {
	source, batchSize, err := parseSourceTaskOperateParams(cmd)
	if err != nil {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	sources := []string{source}
	sourceList := openapi.SourceNameList(sources)
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()

	listReq := &openapi.DMAPIGetTaskListParams{SourceNameList: &sourceList}
	resp := &openapi.GetTaskListResponse{}
	params := []interface{}{listReq}
	sendErr := common.SendOpenapiRequest(ctx, "DMAPIGetTaskList", params, http.StatusOK, resp)
	if sendErr != nil {
		common.PrintLinesf("cannot query status of source: %v", sources)
		return errors.Trace(sendErr)
	}

	if resp.Total == 0 {
		common.PrettyPrintOpenapiResp(false, fmt.Sprintf("there is no tasks of source: %v", sources))
		return nil
	}
	result := batchTaskOperate(cmd, batchSize, sources, resp.Data, operateFunc)
	common.PrettyPrintInterface(result)
	return nil
}

func batchTaskOperate(cmd *cobra.Command, batchSize int, sources []string, tasks []openapi.Task,
	operateFunc func(cmd *cobra.Command, taskName string, sources []string) *taskOperateResult) *batchTaskOperateResult {
	result := batchTaskOperateResult{Result: true, Tasks: []*taskOperateResult{}}

	if len(tasks) < batchSize {
		batchSize = len(tasks)
	}

	workCh := make(chan string)
	go func() {
		for _, task := range tasks {
			workCh <- task.Name
		}
		close(workCh)
	}()

	var wg sync.WaitGroup
	resultCh := make(chan *taskOperateResult, 1)
	for i := 0; i < batchSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for name := range workCh {
				taskResult := operateFunc(cmd, name, sources)
				resultCh <- taskResult
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for item := range resultCh {
		result.Tasks = append(result.Tasks, item)
	}

	sort.Slice(result.Tasks, func(i, j int) bool {
		return result.Tasks[i].Task < result.Tasks[j].Task
	})

	return &result
}
