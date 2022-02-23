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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util/testleak"

	"github.com/pingcap/check"
)

type tableNumberSuite struct{}

var _ = check.Suite(&tableNumberSuite{})

func (s *tableNumberSuite) TestDistributeTables(c *check.C) {
	defer testleak.AfterTest(c)()
	scheduler := newTableNumberScheduler()
	scheduler.ResetWorkloads("capture1", model.TaskWorkload{
		1: model.WorkloadInfo{Workload: 1},
		2: model.WorkloadInfo{Workload: 1},
	})
	scheduler.ResetWorkloads("capture2", model.TaskWorkload{
		3: model.WorkloadInfo{Workload: 1},
		4: model.WorkloadInfo{Workload: 1},
	})
	scheduler.ResetWorkloads("capture3", model.TaskWorkload{
		5: model.WorkloadInfo{Workload: 1},
		6: model.WorkloadInfo{Workload: 1},
		7: model.WorkloadInfo{Workload: 1},
		8: model.WorkloadInfo{Workload: 1},
	})
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
	defer testleak.AfterTest(c)()
	scheduler := newTableNumberScheduler()
	scheduler.ResetWorkloads("capture1", model.TaskWorkload{
		1: model.WorkloadInfo{Workload: 1},
		2: model.WorkloadInfo{Workload: 1},
	})
	scheduler.ResetWorkloads("capture2", model.TaskWorkload{
		3: model.WorkloadInfo{Workload: 1},
		4: model.WorkloadInfo{Workload: 1},
	})
	scheduler.ResetWorkloads("capture3", model.TaskWorkload{
		5:  model.WorkloadInfo{Workload: 1},
		6:  model.WorkloadInfo{Workload: 1},
		7:  model.WorkloadInfo{Workload: 1},
		8:  model.WorkloadInfo{Workload: 1},
		9:  model.WorkloadInfo{Workload: 1},
		10: model.WorkloadInfo{Workload: 1},
	})
	c.Assert(fmt.Sprintf("%.2f%%", scheduler.Skewness()*100), check.Equals, "56.57%")
	skewness, moveJobs := scheduler.CalRebalanceOperates(0)

	for tableID, job := range moveJobs {
		c.Assert(len(job.From), check.Greater, 0)
		c.Assert(len(job.To), check.Greater, 0)
		c.Assert(job.TableID, check.Equals, tableID)
		c.Assert(job.From, check.Not(check.Equals), job.To)
		c.Assert(job.Status, check.Equals, model.MoveTableStatusNone)
	}

	c.Assert(fmt.Sprintf("%.2f%%", skewness*100), check.Equals, "14.14%")

	scheduler.ResetWorkloads("capture1", model.TaskWorkload{
		1: model.WorkloadInfo{Workload: 1},
		2: model.WorkloadInfo{Workload: 1},
		3: model.WorkloadInfo{Workload: 1},
	})
	scheduler.ResetWorkloads("capture2", model.TaskWorkload{})
	scheduler.ResetWorkloads("capture3", model.TaskWorkload{})
	c.Assert(fmt.Sprintf("%.2f%%", scheduler.Skewness()*100), check.Equals, "141.42%")
	skewness, moveJobs = scheduler.CalRebalanceOperates(0)

	for tableID, job := range moveJobs {
		c.Assert(len(job.From), check.Greater, 0)
		c.Assert(len(job.To), check.Greater, 0)
		c.Assert(job.TableID, check.Equals, tableID)
		c.Assert(job.From, check.Not(check.Equals), job.To)
		c.Assert(job.Status, check.Equals, model.MoveTableStatusNone)
	}
	c.Assert(fmt.Sprintf("%.2f%%", skewness*100), check.Equals, "0.00%")
}
