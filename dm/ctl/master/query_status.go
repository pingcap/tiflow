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
	"strings"

	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/spf13/cobra"
)

const stageError = "Error"

type taskResult struct {
	Result bool        `json:"result"`
	Msg    string      `json:"msg"`
	Tasks  []*taskInfo `json:"tasks"`
}

type taskInfo struct {
	TaskName   string   `json:"taskName,omitempty"`
	TaskStatus string   `json:"taskStatus,omitempty"`
	Sources    []string `json:"sources,omitempty"`
}

// NewQueryStatusCmd creates a QueryStatus command.
func NewQueryStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-status [-s source ...] [task-name | task-file] [--more]",
		Short: "Queries task status",
		RunE:  queryStatusFunc,
	}
	cmd.Flags().BoolP("more", "", false, "whether to print the detailed task information")
	return cmd
}

// queryStatusFunc does query task's status.
func queryStatusFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) > 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0)) // maybe empty

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()

	resp := &pb.QueryStatusListResponse{}
	err = common.SendRequest(
		ctx,
		"QueryStatus",
		&pb.QueryStatusListRequest{
			Name:    taskName,
			Sources: sources,
		},
		&resp,
	)
	if err != nil {
		common.PrintLinesf("can not query %s task's status(in sources %v)", taskName, sources)
		return err
	}

	more, err := cmd.Flags().GetBool("more")
	if err != nil {
		common.PrintLinesf("error in parse `--more`")
		return err
	}

	if resp.Result && taskName == "" && len(sources) == 0 && !more {
		result, hasFalseResult := wrapTaskResult(resp)
		if !hasFalseResult { // if any result is false, we still print the full status.
			common.PrettyPrintInterface(result)
			return nil
		}
	}
	common.PrettyPrintResponse(resp)
	return nil
}

// errorOccurred checks ProcessResult and return true if some error occurred.
func errorOccurred(result *pb.ProcessResult) bool {
	return result != nil && len(result.Errors) > 0
}

// getRelayStage returns current relay stage (including stageError).
func getRelayStage(relayStatus *pb.RelayStatus) string {
	if errorOccurred(relayStatus.Result) {
		return stageError
	}
	return relayStatus.Stage.String()
}

// wrapTaskResult picks task info and generate tasks' status and relative workers.
func wrapTaskResult(resp *pb.QueryStatusListResponse) (result *taskResult, hasFalseResult bool) {
	taskStatusMap := make(map[string]string)
	taskCorrespondingSources := make(map[string][]string)
	hasFalseResult = !resp.Result
	for _, source := range resp.Sources {
		hasFalseResult = hasFalseResult || !source.Result
		relayStatus := source.SourceStatus.RelayStatus
		for _, subTask := range source.SubTaskStatus {
			subTaskName := subTask.Name
			subTaskStage := subTask.Stage

			taskCorrespondingSources[subTaskName] = append(taskCorrespondingSources[subTaskName], source.SourceStatus.Source)
			taskStage := taskStatusMap[subTaskName]
			// the status of a task is decided by its subtasks, the rule is listed as follows:
			// |                     Subtasks' status                       |                Task's status                 |
			// | :--------------------------------------------------------: | :------------------------------------------: |
			// |           Any Paused and len(result.errors) > 0            |    Error - Some error occurred in subtask    |
			// | Any Running and unit is "Sync" and relay is Paused/Stopped | Error - Relay status is Error/Paused/Stopped |
			// |              Any Paused but without error                  |                    Paused                    |
			// |                        All New                             |                     New                      |
			// |                      All Finished                          |                   Finished                   |
			// |                      All Stopped                           |                   Stopped                    |
			// |                         Others                             |                   Running                    |
			switch {
			case strings.HasPrefix(taskStage, stageError):
			case subTaskStage == pb.Stage_Paused && errorOccurred(subTask.Result):
				taskStatusMap[subTaskName] = stageError + " - Some error occurred in subtask"
			case subTask.Unit == pb.UnitType_Sync && subTask.Stage == pb.Stage_Running && relayStatus != nil && (relayStatus.Stage == pb.Stage_Paused || relayStatus.Stage == pb.Stage_Stopped):
				taskStatusMap[subTaskName] = stageError + " - Relay status is " + getRelayStage(relayStatus)
			case taskStage == pb.Stage_Paused.String():
			case taskStage == "", subTaskStage == pb.Stage_Paused:
				taskStatusMap[subTaskName] = subTaskStage.String()
			case taskStage != subTaskStage.String():
				taskStatusMap[subTaskName] = pb.Stage_Running.String()
			}
		}
	}
	taskList := make([]*taskInfo, 0, len(taskStatusMap))
	for curTaskName, taskStatus := range taskStatusMap {
		if strings.HasPrefix(taskStatus, stageError) {
			taskStatus += ". Please run `query-status " + curTaskName + "` to get more details."
		}
		taskList = append(taskList,
			&taskInfo{
				TaskName:   curTaskName,
				TaskStatus: taskStatus,
				Sources:    taskCorrespondingSources[curTaskName],
			})
	}
	return &taskResult{
		Result: resp.Result,
		Msg:    resp.Msg,
		Tasks:  taskList,
	}, hasFalseResult
}
