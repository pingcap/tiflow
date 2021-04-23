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

package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/processor/pipeline"
	tablepipeline "github.com/pingcap/ticdc/cdc/processor/pipeline"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type processorSuite struct{}

var _ = check.Suite(&processorSuite{})

func newProcessor4Test() *processor {
	changefeedID := "test-changefeed"
	p := newProcessor(nil, "test-changefeed", nil, &model.CaptureInfo{
		ID:            "test-captureID",
		AdvertiseAddr: "127.0.0.1:0000",
	})
	p.lazyInit = func(ctx context.Context) error {
		if !p.firstTick {
			return nil
		}
		p.schemaStorage = &mockSchemaStorage{}
		return nil
	}
	p.createTablePipeline = func(ctx context.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error) {
		return &mockTablePipeline{
			tableID:      tableID,
			name:         fmt.Sprintf("`test`.`table%d`", tableID),
			status:       pipeline.TableStatusRunning,
			resolvedTs:   replicaInfo.StartTs,
			checkpointTs: replicaInfo.StartTs,
		}, nil
	}
	p.changefeed = newChangeFeedState(changefeedID, p.captureInfo.ID)
	p.changefeed.Info = &model.ChangeFeedInfo{
		SinkURI:    "blackhole://",
		CreateTime: time.Now(),
		StartTs:    0,
		TargetTs:   math.MaxUint64,
		Config:     config.GetDefaultReplicaConfig(),
	}
	p.changefeed.Status = &model.ChangeFeedStatus{}
	p.changefeed.TaskStatus = &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
	}
	p.cancel = func() {}
	return p
}

func applyPatches(c *check.C, state *changefeedState) {
	for _, patch := range state.pendingPatches {
		key := &etcd.CDCKey{}
		c.Assert(key.Parse(patch.Key.String()), check.IsNil)
		var value []byte
		var err error
		switch key.Tp {
		case etcd.CDCKeyTypeTaskPosition:
			if state.TaskPosition == nil {
				value = nil
				break
			}
			value, err = json.Marshal(state.TaskPosition)
			c.Assert(err, check.IsNil)
		case etcd.CDCKeyTypeTaskStatus:
			if state.TaskStatus == nil {
				value = nil
				break
			}
			value, err = json.Marshal(state.TaskStatus)
			c.Assert(err, check.IsNil)
		case etcd.CDCKeyTypeTaskWorkload:
			if state.Workload == nil {
				value = nil
				break
			}
			value, err = json.Marshal(state.Workload)
			c.Assert(err, check.IsNil)
		default:
			c.Fatal("unexpected key type")
		}
		newValue, err := patch.Fun(value)
		c.Assert(err, check.IsNil)
		err = state.UpdateCDCKey(key, newValue)
		c.Assert(err, check.IsNil)
	}
	state.pendingPatches = state.pendingPatches[:0]
}

type mockTablePipeline struct {
	tableID      model.TableID
	name         string
	resolvedTs   model.Ts
	checkpointTs model.Ts
	barrierTs    model.Ts
	stopTs       model.Ts
	status       pipeline.TableStatus
	canceled     bool
}

func (m *mockTablePipeline) ID() (tableID int64, markTableID int64) {
	return m.tableID, 0
}

func (m *mockTablePipeline) Name() string {
	return m.name
}

func (m *mockTablePipeline) ResolvedTs() model.Ts {
	return m.resolvedTs
}

func (m *mockTablePipeline) CheckpointTs() model.Ts {
	return m.checkpointTs
}

func (m *mockTablePipeline) UpdateBarrierTs(ts model.Ts) {
	m.barrierTs = ts
}

func (m *mockTablePipeline) AsyncStop(targetTs model.Ts) {
	m.stopTs = targetTs
}

func (m *mockTablePipeline) Workload() model.WorkloadInfo {
	return model.WorkloadInfo{Workload: 1}
}

func (m *mockTablePipeline) Status() pipeline.TableStatus {
	return m.status
}

func (m *mockTablePipeline) Cancel() {
	if m.canceled {
		log.Panic("cancel a canceled table pipeline")
	}
	m.canceled = true
}

func (m *mockTablePipeline) Wait() []error {
	panic("not implemented") // TODO: Implement
}

type mockSchemaStorage struct {
	resolvedTs model.Ts
	lastGcTs   model.Ts
}

func (m *mockSchemaStorage) GetSnapshot(ctx context.Context, ts uint64) (*entry.SingleSchemaSnapshot, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockSchemaStorage) GetLastSnapshot() *entry.SingleSchemaSnapshot {
	panic("not implemented") // TODO: Implement
}

func (m *mockSchemaStorage) HandleDDLJob(job *timodel.Job) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockSchemaStorage) AdvanceResolvedTs(ts uint64) {
	m.resolvedTs = ts
}

func (m *mockSchemaStorage) ResolvedTs() uint64 {
	return m.resolvedTs
}

func (m *mockSchemaStorage) DoGC(ts uint64) {
	m.lastGcTs = ts
}

func (s *processorSuite) TestCheckTablesNum(c *check.C) {
	defer testleak.AfterTest(c)()
	p := newProcessor4Test()
	ctx := context.Background()
	var err error
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals,
		&model.TaskPosition{
			CheckPointTs: 0,
			ResolvedTs:   0,
			Count:        0,
			Error:        nil,
		})

	p = newProcessor4Test()
	p.changefeed.Info.StartTs = 66
	p.changefeed.Status.CheckpointTs = 88
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals,
		&model.TaskPosition{
			CheckPointTs: 88,
			ResolvedTs:   88,
			Count:        0,
			Error:        nil,
		})
}

func (s *processorSuite) TestHandleTableOperation4SingleTable(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	p := newProcessor4Test()
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	p.changefeed.Status.CheckpointTs = 90
	p.changefeed.Status.ResolvedTs = 90
	p.changefeed.TaskPosition.ResolvedTs = 100
	p.schemaStorage.AdvanceResolvedTs(200)

	// no operation
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)

	// add table, in processing
	// in current implementation of owner, the startTs and BoundaryTs of add table operation should be always equaled.
	p.changefeed.TaskStatus.AddTable(66, &model.TableReplicaInfo{StartTs: 60}, 60)
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
		},
	})

	// add table, not finished
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
		},
	})

	// add table, push the resolvedTs
	table66 := p.tables[66].(*mockTablePipeline)
	table66.resolvedTs = 101
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
		},
	})
	c.Assert(p.changefeed.TaskPosition.ResolvedTs, check.Equals, uint64(101))

	// finish the operation
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Done: true, Status: model.OperFinished},
		},
	})

	// clear finished operations
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: nil,
	})

	// remove table, in processing
	p.changefeed.TaskStatus.RemoveTable(66, 120, false)
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
		},
	})
	c.Assert(table66.stopTs, check.Equals, uint64(120))

	// remove table, not finished
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
		},
	})

	// remove table, finished
	table66.status = pipeline.TableStatusStopped
	table66.checkpointTs = 121
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: true, BoundaryTs: 121, Done: true, Status: model.OperFinished},
		},
	})
	c.Assert(table66.canceled, check.IsTrue)
	c.Assert(p.tables[66], check.IsNil)
}

func (s *processorSuite) TestHandleTableOperation4MultiTable(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	p := newProcessor4Test()
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	p.schemaStorage.AdvanceResolvedTs(200)
	p.changefeed.Status.CheckpointTs = 90
	p.changefeed.Status.ResolvedTs = 90
	p.changefeed.TaskPosition.ResolvedTs = 100
	p.changefeed.TaskPosition.CheckPointTs = 90

	// no operation
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)

	// add table, in processing
	// in current implementation of owner, the startTs and BoundaryTs of add table operation should be always equaled.
	p.changefeed.TaskStatus.AddTable(1, &model.TableReplicaInfo{StartTs: 60}, 60)
	p.changefeed.TaskStatus.AddTable(2, &model.TableReplicaInfo{StartTs: 50}, 50)
	p.changefeed.TaskStatus.AddTable(3, &model.TableReplicaInfo{StartTs: 40}, 40)
	p.changefeed.TaskStatus.Tables[4] = &model.TableReplicaInfo{StartTs: 30}
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			3: {StartTs: 40},
			4: {StartTs: 30},
		},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: false, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
			2: {Delete: false, BoundaryTs: 50, Done: false, Status: model.OperProcessed},
			3: {Delete: false, BoundaryTs: 40, Done: false, Status: model.OperProcessed},
		},
	})
	c.Assert(p.tables, check.HasLen, 4)
	c.Assert(p.changefeed.TaskPosition.CheckPointTs, check.Equals, uint64(30))
	c.Assert(p.changefeed.TaskPosition.ResolvedTs, check.Equals, uint64(30))

	// add table, not finished
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			3: {StartTs: 40},
			4: {StartTs: 30},
		},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: false, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
			2: {Delete: false, BoundaryTs: 50, Done: false, Status: model.OperProcessed},
			3: {Delete: false, BoundaryTs: 40, Done: false, Status: model.OperProcessed},
		},
	})
	c.Assert(p.tables, check.HasLen, 4)

	// add table, push the resolvedTs
	table1 := p.tables[1].(*mockTablePipeline)
	table2 := p.tables[2].(*mockTablePipeline)
	table3 := p.tables[3].(*mockTablePipeline)
	table4 := p.tables[4].(*mockTablePipeline)
	table1.resolvedTs = 101
	table2.resolvedTs = 101
	table3.resolvedTs = 102
	table4.resolvedTs = 103
	// removed table 3
	p.changefeed.TaskStatus.RemoveTable(3, 60, false)
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			4: {StartTs: 30},
		},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: false, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
			2: {Delete: false, BoundaryTs: 50, Done: false, Status: model.OperProcessed},
			3: {Delete: true, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
		},
	})
	c.Assert(p.tables, check.HasLen, 4)
	c.Assert(table3.canceled, check.IsFalse)
	c.Assert(table3.stopTs, check.Equals, uint64(60))
	c.Assert(p.changefeed.TaskPosition.ResolvedTs, check.Equals, uint64(101))

	// finish remove and add operations
	table3.status = pipeline.TableStatusStopped
	table3.checkpointTs = 65
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			4: {StartTs: 30},
		},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: false, BoundaryTs: 60, Done: true, Status: model.OperFinished},
			2: {Delete: false, BoundaryTs: 50, Done: true, Status: model.OperFinished},
			3: {Delete: true, BoundaryTs: 65, Done: true, Status: model.OperFinished},
		},
	})
	c.Assert(p.tables, check.HasLen, 3)
	c.Assert(table3.canceled, check.IsTrue)

	// clear finished operations
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			4: {StartTs: 30},
		},
		Operation: nil,
	})
	c.Assert(p.tables, check.HasLen, 3)

	// remove table, in processing
	p.changefeed.TaskStatus.RemoveTable(1, 120, false)
	p.changefeed.TaskStatus.RemoveTable(4, 120, false)
	delete(p.changefeed.TaskStatus.Tables, 2)
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
			4: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
		},
	})
	c.Assert(table1.stopTs, check.Equals, uint64(120))
	c.Assert(table4.stopTs, check.Equals, uint64(120))
	c.Assert(table2.canceled, check.IsTrue)
	c.Assert(p.tables, check.HasLen, 2)

	// remove table, not finished
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
			4: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
		},
	})

	// remove table, finished
	table1.status = pipeline.TableStatusStopped
	table1.checkpointTs = 121
	table4.status = pipeline.TableStatusStopped
	table4.checkpointTs = 122
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: true, BoundaryTs: 121, Done: true, Status: model.OperFinished},
			4: {Delete: true, BoundaryTs: 122, Done: true, Status: model.OperFinished},
		},
	})
	c.Assert(table1.canceled, check.IsTrue)
	c.Assert(table4.canceled, check.IsTrue)
	c.Assert(p.tables, check.HasLen, 0)
}

func (s *processorSuite) TestInitTable(c *check.C) {
	defer testleak.AfterTest(c)()
	p := newProcessor4Test()
	ctx := context.Background()
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)

	p.changefeed.TaskStatus.Tables[1] = &model.TableReplicaInfo{StartTs: 20}
	p.changefeed.TaskStatus.Tables[2] = &model.TableReplicaInfo{StartTs: 30}
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.tables[1], check.Not(check.IsNil))
	c.Assert(p.tables[2], check.Not(check.IsNil))
}

func (s *processorSuite) TestProcessorError(c *check.C) {
	defer testleak.AfterTest(c)()
	p := newProcessor4Test()
	ctx := context.Background()
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)

	// send a abnormal error
	p.sendError(cerror.ErrSinkURIInvalid)
	_, err = p.Tick(ctx, p.changefeed)
	applyPatches(c, p.changefeed)
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals, &model.TaskPosition{
		Error: &model.RunningError{
			Addr:    "127.0.0.1:0000",
			Code:    "CDC:ErrProcessorUnknown",
			Message: "[CDC:ErrSinkURIInvalid]sink uri invalid",
		},
	})

	p = newProcessor4Test()
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)

	// send a normal error
	p.sendError(context.Canceled)
	_, err = p.Tick(ctx, p.changefeed)
	applyPatches(c, p.changefeed)
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals, &model.TaskPosition{
		Error: nil,
	})
}

func (s *processorSuite) TestProcessorExit(c *check.C) {
	defer testleak.AfterTest(c)()
	p := newProcessor4Test()
	ctx := context.Background()
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)

	// stop the changefeed
	p.changefeed.TaskStatus.AdminJobType = model.AdminStop
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals, &model.TaskPosition{
		Error: nil,
	})
}

func (s *processorSuite) TestProcessorClose(c *check.C) {
	defer testleak.AfterTest(c)()
	p := newProcessor4Test()
	ctx := context.Background()
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)

	// add tables
	p.changefeed.TaskStatus.Tables[1] = &model.TableReplicaInfo{StartTs: 20}
	p.changefeed.TaskStatus.Tables[2] = &model.TableReplicaInfo{StartTs: 30}
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)

	// push the resolvedTs and checkpointTs
	p.schemaStorage.AdvanceResolvedTs(100)
	p.changefeed.Status.ResolvedTs = 100
	p.tables[1].(*mockTablePipeline).resolvedTs = 110
	p.tables[2].(*mockTablePipeline).resolvedTs = 90
	p.tables[1].(*mockTablePipeline).checkpointTs = 90
	p.tables[2].(*mockTablePipeline).checkpointTs = 95
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 90,
		ResolvedTs:   90,
		Error:        nil,
	})
	c.Assert(p.changefeed.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{1: {StartTs: 20}, 2: {StartTs: 30}},
	})
	c.Assert(p.changefeed.Workload, check.DeepEquals, model.TaskWorkload{1: {Workload: 1}, 2: {Workload: 1}})

	c.Assert(p.Close(), check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.IsNil)
	c.Assert(p.changefeed.TaskStatus, check.IsNil)
	c.Assert(p.changefeed.Workload, check.IsNil)
	c.Assert(p.tables[1].(*mockTablePipeline).canceled, check.IsTrue)
	c.Assert(p.tables[2].(*mockTablePipeline).canceled, check.IsTrue)

	p = newProcessor4Test()
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)

	// add tables
	p.changefeed.TaskStatus.Tables[1] = &model.TableReplicaInfo{StartTs: 20}
	p.changefeed.TaskStatus.Tables[2] = &model.TableReplicaInfo{StartTs: 30}
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)

	// send error
	p.sendError(cerror.ErrSinkURIInvalid)
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	applyPatches(c, p.changefeed)

	c.Assert(p.Close(), check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals, &model.TaskPosition{
		Error: &model.RunningError{
			Addr:    "127.0.0.1:0000",
			Code:    "CDC:ErrProcessorUnknown",
			Message: "[CDC:ErrSinkURIInvalid]sink uri invalid",
		},
	})
	c.Assert(p.changefeed.TaskStatus, check.IsNil)
	c.Assert(p.changefeed.Workload, check.IsNil)
	c.Assert(p.tables[1].(*mockTablePipeline).canceled, check.IsTrue)
	c.Assert(p.tables[2].(*mockTablePipeline).canceled, check.IsTrue)
}

func (s *processorSuite) TestPositionDeleted(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	p := newProcessor4Test()
	p.changefeed.TaskStatus.Tables[1] = &model.TableReplicaInfo{StartTs: 30}
	p.changefeed.TaskStatus.Tables[2] = &model.TableReplicaInfo{StartTs: 40}
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	p.schemaStorage.AdvanceResolvedTs(200)

	// cal position
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 30,
		ResolvedTs:   30,
	})

	// some other delete the task position
	p.changefeed.TaskPosition = nil
	// position created again
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 0,
		ResolvedTs:   0,
	})

	// cal position
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 30,
		ResolvedTs:   30,
	})
}
