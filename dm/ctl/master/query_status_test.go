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
	"sort"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/pb"
)

func TestCtlMaster(t *testing.T) {
	check.TestingT(t)
}

type testCtlMaster struct{}

var _ = check.Suite(&testCtlMaster{})

func generateAndCheckTaskResult(c *check.C, resp *pb.QueryStatusListResponse, expectedResult []*taskInfo) {
	result, hasFalseResult := wrapTaskResult(resp)
	c.Assert(hasFalseResult, check.IsFalse)
	c.Assert(result.Result, check.IsTrue)
	c.Assert(result.Tasks, check.HasLen, 1)
	sort.Strings(result.Tasks[0].Sources)
	c.Assert(result.Tasks, check.DeepEquals, expectedResult)
}

func subTestSameSubTaskStatus(c *check.C, resp *pb.QueryStatusListResponse, expectedResult []*taskInfo, stage pb.Stage) {
	for i := range resp.Sources {
		resp.Sources[i].SubTaskStatus[0].Stage = stage
	}
	expectedResult[0].TaskStatus = stage.String()
	generateAndCheckTaskResult(c, resp, expectedResult)
}

func (t *testCtlMaster) TestWrapTaskResult(c *check.C) {
	resp := new(pb.QueryStatusListResponse)
	resp.Result = true

	// Should return error when some error occurs in subtask
	resp.Sources = []*pb.QueryStatusResponse{
		{
			Result:       true,
			SourceStatus: &pb.SourceStatus{Source: "mysql-replica-01"},
			SubTaskStatus: []*pb.SubTaskStatus{{
				Name:  "test",
				Stage: pb.Stage_Running,
			}},
		},
		{
			Result:       true,
			SourceStatus: &pb.SourceStatus{Source: "mysql-replica-02"},
			SubTaskStatus: []*pb.SubTaskStatus{{
				Name:  "test",
				Stage: pb.Stage_Running,
			}},
		},
		{
			Result:       true,
			SourceStatus: &pb.SourceStatus{Source: "mysql-replica-03"},
			SubTaskStatus: []*pb.SubTaskStatus{{
				Name:  "test",
				Stage: pb.Stage_Paused,
				Result: &pb.ProcessResult{
					Errors: []*pb.ProcessError{{}},
				},
			}},
		},
	}
	extraInfo := ". Please run `query-status test` to get more details."
	expectedResult := []*taskInfo{{
		TaskName:   "test",
		TaskStatus: stageError + " - Some error occurred in subtask" + extraInfo,
		Sources:    []string{"mysql-replica-01", "mysql-replica-02", "mysql-replica-03"},
	}}
	generateAndCheckTaskResult(c, resp, expectedResult)
	// Should return error when subtask unit is "Sync" while relay status is not running
	resp.Sources[2].SubTaskStatus[0].Result = nil
	resp.Sources[0].SubTaskStatus[0].Unit = pb.UnitType_Sync
	// relay status is Error
	resp.Sources[0].SourceStatus.RelayStatus = &pb.RelayStatus{
		Stage: pb.Stage_Paused,
		Result: &pb.ProcessResult{
			Errors: []*pb.ProcessError{{}},
		},
	}
	expectedResult[0].TaskStatus = stageError + " - Relay status is " + stageError + extraInfo
	generateAndCheckTaskResult(c, resp, expectedResult)
	// relay status is Paused
	resp.Sources[0].SourceStatus.RelayStatus = &pb.RelayStatus{Stage: pb.Stage_Paused}
	expectedResult[0].TaskStatus = stageError + " - Relay status is " + pb.Stage_Paused.String() + extraInfo
	generateAndCheckTaskResult(c, resp, expectedResult)
	// relay status is Stopped
	resp.Sources[0].SourceStatus.RelayStatus = &pb.RelayStatus{Stage: pb.Stage_Stopped}
	expectedResult[0].TaskStatus = stageError + " - Relay status is " + pb.Stage_Stopped.String() + extraInfo
	generateAndCheckTaskResult(c, resp, expectedResult)

	// one subtask is paused and no error occurs, should return paused
	resp.Sources[2].SubTaskStatus[0].Result = nil
	resp.Sources[0].SubTaskStatus[0].Unit = 0
	resp.Sources[0].SourceStatus.RelayStatus = nil
	expectedResult[0].TaskStatus = pb.Stage_Paused.String()
	generateAndCheckTaskResult(c, resp, expectedResult)
	// All subtasks are Finished/Stopped/.../New
	stageArray := []pb.Stage{pb.Stage_Finished, pb.Stage_Stopped, pb.Stage_Paused, pb.Stage_Running, pb.Stage_New}
	for _, stage := range stageArray {
		subTestSameSubTaskStatus(c, resp, expectedResult, stage)
	}
	// All subtasks are New except the last one(which is Finished)
	resp.Sources[2].SubTaskStatus[0].Stage = pb.Stage_Finished
	expectedResult[0].TaskStatus = pb.Stage_Running.String()
	generateAndCheckTaskResult(c, resp, expectedResult)

	// test situation with two tasks
	resp.Sources = append(resp.Sources, &pb.QueryStatusResponse{
		Result:       true,
		SourceStatus: &pb.SourceStatus{Source: "mysql-replica-04"},
		SubTaskStatus: []*pb.SubTaskStatus{{
			Name:  "test2",
			Stage: pb.Stage_Paused,
			Result: &pb.ProcessResult{
				Errors: []*pb.ProcessError{{}},
			},
		}},
	})
	result, hasFalseResult := wrapTaskResult(resp)
	c.Assert(hasFalseResult, check.IsFalse)
	c.Assert(result.Tasks, check.HasLen, 2)
	if result.Tasks[0].TaskName == "test2" {
		result.Tasks[0], result.Tasks[1] = result.Tasks[1], result.Tasks[0]
	}
	sort.Strings(result.Tasks[0].Sources)
	expectedResult = []*taskInfo{
		{
			TaskName:   "test",
			TaskStatus: pb.Stage_Running.String(),
			Sources:    []string{"mysql-replica-01", "mysql-replica-02", "mysql-replica-03"},
		}, {
			TaskName:   "test2",
			TaskStatus: stageError + " - Some error occurred in subtask. Please run `query-status test2` to get more details.",
			Sources:    []string{"mysql-replica-04"},
		},
	}
	c.Assert(result.Tasks, check.DeepEquals, expectedResult)

	resp.Result = false
	_, hasFalseResult = wrapTaskResult(resp)
	c.Assert(hasFalseResult, check.IsTrue)

	resp.Result = true
	resp.Sources[0].Result = false
	_, hasFalseResult = wrapTaskResult(resp)
	c.Assert(hasFalseResult, check.IsTrue)

	resp.Sources[0].Result = true
	_, hasFalseResult = wrapTaskResult(resp)
	c.Assert(hasFalseResult, check.IsFalse)
}
