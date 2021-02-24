// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/stretchr/testify/mock"
)

type changeFeedStateTestSuite struct {
}

var _ = check.Suite(&changeFeedStateTestSuite{})

type mockScheduler struct {
	mock.Mock
}

func (m *mockScheduler) PutTasks(tables map[model.TableID]*tableTask) {
	m.Called(tables)
}

func (m *mockScheduler) SetAffinity(tableID model.TableID, captureID model.CaptureID, ttl int) {
	m.Called(tableID, captureID, ttl)
}

func (m *mockScheduler) IsReady() bool {
	args := m.Called()
	return args.Bool(0)
}

func (s *changeFeedStateTestSuite) TestBasics(c *check.C) {
	defer testleak.AfterTest(c)()
	tasks := map[model.TableID]*tableTask{
		1: {
			TableID:      1,
			CheckpointTs: 2000,
			ResolvedTs:   2000,
		},
		2: {
			TableID:      2,
			CheckpointTs: 2000,
			ResolvedTs:   2000,
		},
	}

	scheduler := &mockScheduler{}
	cfState := newChangeFeedState(tasks, 2000, scheduler)

	scheduler.On("IsReady").Return(false).Once()
	cfState.SyncTasks()
	scheduler.AssertNotCalled(c, "PutTasks")

	scheduler.On("IsReady").Return(true)
	scheduler.On("PutTasks", mock.Anything).Return()
	cfState.SyncTasks()
	scheduler.AssertCalled(c, "PutTasks", tasks)

	cfState.AddDDLBarrier(2500)
	cfState.SetDDLResolvedTs(3000)
	c.Assert(cfState.ShouldRunDDL(), check.IsNil)
	c.Assert(cfState.ResolvedTs(), check.Equals, uint64(2000))
	c.Assert(cfState.CheckpointTs(), check.Equals, uint64(2000))

	cfState.SetTableResolvedTs(1, 2500)
	cfState.SetTableCheckpointTs(1, 2500)
	c.Assert(cfState.ShouldRunDDL(), check.IsNil)
	c.Assert(cfState.ResolvedTs(), check.Equals, uint64(2000))
	c.Assert(cfState.CheckpointTs(), check.Equals, uint64(2000))

	cfState.SetTableResolvedTs(2, 2500)
	cfState.SetTableCheckpointTs(2, 2500)
	c.Assert(cfState.ResolvedTs(), check.Equals, uint64(2499))
	c.Assert(cfState.CheckpointTs(), check.Equals, uint64(2499))
	c.Assert(cfState.ShouldRunDDL(), check.DeepEquals, &barrier{
		BarrierType: DDLBarrier,
		BarrierTs:   2500,
	})

	cfState.MarkDDLDone(ddlResult{
		FinishTs: 2500,
		Actions: []tableAction{{
			Action:  AddTableAction,
			tableID: 3,
		}},
	})
	cfState.SyncTasks()
	tasks[3] = &tableTask{
		TableID:      3,
		CheckpointTs: 2499,
		ResolvedTs:   2499,
	}
	scheduler.AssertCalled(c, "PutTasks", tasks)
	c.Assert(cfState.CheckpointTs(), check.Equals, uint64(2499))
	c.Assert(cfState.ResolvedTs(), check.Equals, uint64(2499))

	cfState.AddDDLBarrier(3500)
	cfState.SetDDLResolvedTs(4000)
	cfState.SetTableResolvedTs(1, 3499)
	cfState.SetTableCheckpointTs(1, 3499)
	cfState.SetTableResolvedTs(2, 3000)
	cfState.SetTableCheckpointTs(2, 2600)
	cfState.SetTableResolvedTs(3, 3000)
	cfState.SetTableCheckpointTs(3, 2600)
	c.Assert(cfState.CheckpointTs(), check.Equals, uint64(2600))
	c.Assert(cfState.ResolvedTs(), check.Equals, uint64(3000))

	cfState.SetTableResolvedTs(1, 4000)
	cfState.SetTableCheckpointTs(1, 3499)
	cfState.SetTableResolvedTs(2, 4000)
	cfState.SetTableCheckpointTs(2, 3499)
	cfState.SetTableResolvedTs(3, 4000)
	cfState.SetTableCheckpointTs(3, 3499)
	c.Assert(cfState.ResolvedTs(), check.Equals, uint64(3499))
	c.Assert(cfState.CheckpointTs(), check.Equals, uint64(3499))
	c.Assert(cfState.ShouldRunDDL(), check.DeepEquals, &barrier{
		BarrierType: DDLBarrier,
		BarrierTs:   3500,
	})

	cfState.MarkDDLDone(ddlResult{
		FinishTs: 3500,
		Actions: []tableAction{{
			Action:  DropTableAction,
			tableID: 3,
		}, {
			Action:  DropTableAction,
			tableID: 2,
		}},
	})

	delete(tasks, 2)
	delete(tasks, 3)
	scheduler.AssertCalled(c, "PutTasks", tasks)

	cfState.SetTableCheckpointTs(2, 1000) // should be ok since table 2 does not exist
	cfState.SetTableResolvedTs(2, 1000)   // should be ok since table 2 does not exist
}

func (s *changeFeedStateTestSuite) TestPanicCases(c *check.C) {
	defer testleak.AfterTest(c)()
	tasks := map[model.TableID]*tableTask{
		1: {
			TableID:      1,
			CheckpointTs: 2000,
			ResolvedTs:   2000,
		},
		2: {
			TableID:      2,
			CheckpointTs: 2000,
			ResolvedTs:   2000,
		},
	}

	scheduler := &mockScheduler{}
	cfState := newChangeFeedState(tasks, 2000, scheduler)

	scheduler.On("IsReady").Return(true)
	scheduler.On("PutTasks", mock.Anything).Return()
	cfState.SyncTasks()
	scheduler.AssertCalled(c, "PutTasks", tasks)

	c.Assert(func() {
		cfState.SetTableCheckpointTs(1, 1000)
	}, check.PanicMatches, ".*table checkpoint regressed.*")

	cfState.AddDDLBarrier(5000)
	c.Assert(func() {
		cfState.AddDDLBarrier(4000)
	}, check.PanicMatches, ".*DDLBarrier too small.*")

	cfState.SetDDLResolvedTs(5500)
	c.Assert(func() {
		cfState.AddDDLBarrier(5400)
	}, check.PanicMatches, ".*DDLBarrier too small.*")

	cfState.SetDDLResolvedTs(7000)
	cfState.SetTableResolvedTs(1, 4999)
	cfState.SetTableCheckpointTs(1, 4999)
	cfState.SetTableResolvedTs(2, 4999)
	cfState.SetTableCheckpointTs(2, 4999)
	c.Assert(cfState.ShouldRunDDL(), check.DeepEquals, &barrier{
		BarrierType: DDLBarrier,
		BarrierTs:   5000,
	})
	c.Assert(func() {
		cfState.MarkDDLDone(ddlResult{
			FinishTs: 3000,
			Actions:  nil,
		})
	}, check.PanicMatches, ".*Unexpected checkpoint.*")
}
