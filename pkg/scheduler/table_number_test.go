// Copyright 2020 PingCAP, Inc.
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

package scheduler

import (
	"fmt"

	"github.com/pingcap/ticdc/cdc/model"

	"github.com/pingcap/check"
)

type tableNumberSuite struct{}

var _ = check.Suite(&tableNumberSuite{})

func (s *tableNumberSuite) TestDistributeTables(c *check.C) {
	scheduler := NewTableNumberScheduler()
	scheduler.ResetWorkloads("capture1", model.TaskWorkload{
		1: model.WorkloadInfo{Workload: 1},
		2: model.WorkloadInfo{Workload: 1}})
	scheduler.ResetWorkloads("capture2", model.TaskWorkload{
		3: model.WorkloadInfo{Workload: 1},
		4: model.WorkloadInfo{Workload: 1}})
	scheduler.ResetWorkloads("capture3", model.TaskWorkload{
		5: model.WorkloadInfo{Workload: 1},
		6: model.WorkloadInfo{Workload: 1},
		7: model.WorkloadInfo{Workload: 1},
		8: model.WorkloadInfo{Workload: 1}})
	c.Assert(fmt.Sprintf("%.2f%%", scheduler.Skewness()*100), check.Equals, "35.36%")
	tableToAdd := map[model.TableID]model.Ts{10: 1, 11: 2, 12: 3, 13: 4, 14: 5, 15: 6, 16: 7, 17: 8}
	result := scheduler.DistributeTables(tableToAdd)
	c.Assert(len(result), check.Equals, 3) // there three captures
	totalTableNum := 0
	for _, ops := range result {
		for tableID, op := range ops {
			ts, exist := tableToAdd[tableID]
			c.Assert(exist, check.IsTrue)
			c.Assert(op.Delete, check.IsFalse)
			c.Assert(op.BoundaryTs, check.Equals, ts)
			c.Assert(op.Done, check.IsFalse)
			totalTableNum++
		}
	}
	c.Assert(totalTableNum, check.Equals, 8) // there eight tables to add
	c.Assert(fmt.Sprintf("%.2f%%", scheduler.Skewness()*100), check.Equals, "8.84%")
}

func (s *tableNumberSuite) TestCalRebalanceOperates(c *check.C) {
	scheduler := NewTableNumberScheduler()
	scheduler.ResetWorkloads("capture1", model.TaskWorkload{
		1: model.WorkloadInfo{Workload: 1},
		2: model.WorkloadInfo{Workload: 1}})
	scheduler.ResetWorkloads("capture2", model.TaskWorkload{
		3: model.WorkloadInfo{Workload: 1},
		4: model.WorkloadInfo{Workload: 1}})
	scheduler.ResetWorkloads("capture3", model.TaskWorkload{
		5:  model.WorkloadInfo{Workload: 1},
		6:  model.WorkloadInfo{Workload: 1},
		7:  model.WorkloadInfo{Workload: 1},
		8:  model.WorkloadInfo{Workload: 1},
		9:  model.WorkloadInfo{Workload: 1},
		10: model.WorkloadInfo{Workload: 1}})
	c.Assert(fmt.Sprintf("%.2f%%", scheduler.Skewness()*100), check.Equals, "56.57%")
	skewness, deleteOperations, addOperations := scheduler.CalRebalanceOperates(0, 66)
	resultSet := make(map[model.TableID]*struct {
		from model.CaptureID
		to   model.CaptureID
	})
	for captureID, ops := range deleteOperations {
		for tableID, op := range ops {
			r, exist := resultSet[tableID]
			if !exist {
				r = &struct {
					from model.CaptureID
					to   model.CaptureID
				}{}
				resultSet[tableID] = r
			}
			r.from = captureID
			c.Assert(op.BoundaryTs, check.Equals, model.Ts(66))
			c.Assert(op.Delete, check.IsTrue)
			c.Assert(op.Done, check.IsFalse)
		}
	}

	for captureID, ops := range addOperations {
		for tableID, op := range ops {
			r, exist := resultSet[tableID]
			if !exist {
				r = &struct {
					from model.CaptureID
					to   model.CaptureID
				}{}
				resultSet[tableID] = r
			}
			r.to = captureID
			c.Assert(op.BoundaryTs, check.Equals, model.Ts(66))
			c.Assert(op.Delete, check.IsFalse)
			c.Assert(op.Done, check.IsFalse)
		}
	}

	for _, r := range resultSet {
		c.Assert(len(r.from), check.Greater, 0)
		c.Assert(len(r.to), check.Greater, 0)
		c.Assert(r.from, check.Not(check.Equals), r.to)
	}
	c.Assert(fmt.Sprintf("%.2f%%", skewness*100), check.Equals, "14.14%")
}
