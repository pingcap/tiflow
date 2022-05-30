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

package tp

import (
	"context"
	"testing"

	"github.com/edwingeng/deque"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/base"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newBaseAgent4Test() *agent {
	return &agent{
		ownerInfo: ownerInfo{
			version:   "owner-version-1",
			captureID: "owner-1",
			revision:  schedulepb.OwnerRevision{Revision: 1},
		},
		version:      "agent-version-1",
		epoch:        schedulepb.ProcessorEpoch{Epoch: "agent-epoch-1"},
		pendingTasks: deque.NewDeque(),
		runningTasks: make(map[model.TableID]*dispatchTableTask),
	}
}

func TestAgentHandleDispatchTableTask(t *testing.T) {
	t.Parallel()

	a := newBaseAgent4Test()

	mockTableExecutor := NewMockTableExecutor()
	a.tableExec = mockTableExecutor

	tableID := model.TableID(1)
	epoch := schedulepb.ProcessorEpoch{}
	// all possible tasks can be received, and should be correctly handled no matter table's status
	var tasks []*dispatchTableTask
	for _, isRemove := range []bool{true, false} {
		for _, isPrepare := range []bool{true, false} {
			if isPrepare && isRemove {
				continue
			}
			task := &dispatchTableTask{
				TableID:   tableID,
				StartTs:   0,
				IsRemove:  isRemove,
				IsPrepare: isPrepare,
				Epoch:     epoch,
				status:    dispatchTableTaskReceived,
			}
			tasks = append(tasks, task)
		}
	}

	states := []schedulepb.TableState{
		schedulepb.TableStateAbsent,
		schedulepb.TableStatePreparing,
		schedulepb.TableStatePrepared,
		schedulepb.TableStateReplicating,
		schedulepb.TableStateStopping,
		schedulepb.TableStateStopped,
	}
	ctx := context.Background()
	for _, state := range states {
		iterPermutation([]int{0, 1, 2}, func(sequence []int) {
			t.Logf("test %v, %v", state, sequence)
			switch state {
			case schedulepb.TableStatePreparing:
				mockTableExecutor.tables[tableID] = pipeline.TableStatusPreparing
			case schedulepb.TableStatePrepared:
				mockTableExecutor.tables[tableID] = pipeline.TableStatusPrepared
			case schedulepb.TableStateReplicating:
				mockTableExecutor.tables[tableID] = pipeline.TableStatusReplicating
			case schedulepb.TableStateStopping:
				mockTableExecutor.tables[tableID] = pipeline.TableStatusStopping
			case schedulepb.TableStateStopped:
				mockTableExecutor.tables[tableID] = pipeline.TableStatusStopped
			case schedulepb.TableStateAbsent:
			default:
			}
			for _, idx := range sequence {
				task := tasks[idx]
				task.status = dispatchTableTaskReceived
				a.runningTasks[task.TableID] = task

				if task.IsRemove {
					for _, ok := range []bool{false, true} {
						mockTableExecutor.On("RemoveTable", mock.Anything, mock.Anything).Return(ok)
						for _, ok1 := range []bool{false, true} {
							mockTableExecutor.On("IsRemoveTableFinished", mock.Anything, mock.Anything).Return(0, ok1)

							response, err := a.handleDispatchTableTasks(ctx)
							require.NoError(t, err)
							require.NotNil(t, response)
							if ok && ok1 && len(response) != 0 {
								resp, ok := response[0].DispatchTableResponse.Response.(*schedulepb.DispatchTableResponse_RemoveTable)
								require.True(t, ok)
								require.Equal(t, schedulepb.TableStateStopped, resp.RemoveTable.Status.State)
							}
							mockTableExecutor.ExpectedCalls = mockTableExecutor.ExpectedCalls[:1]
						}
						mockTableExecutor.ExpectedCalls = nil
					}
				} else {
					for _, ok := range []bool{false, true} {
						mockTableExecutor.On("AddTable", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(ok, nil)
						for _, ok1 := range []bool{false, true} {
							mockTableExecutor.On("IsAddTableFinished", mock.Anything, mock.Anything, mock.Anything).Return(ok1, nil)

							response, err := a.handleDispatchTableTasks(ctx)
							require.NoError(t, err)
							require.NotNil(t, response)
							if ok && ok1 && len(response) != 0 {
								resp, ok := response[0].DispatchTableResponse.Response.(*schedulepb.DispatchTableResponse_AddTable)
								require.True(t, ok)
								if task.IsPrepare {
									require.Equal(t, schedulepb.TableStatePrepared, resp.AddTable.Status.State)
								} else {
									require.Equal(t, schedulepb.TableStateReplicating, resp.AddTable.Status.State)
								}
							}
							mockTableExecutor.ExpectedCalls = mockTableExecutor.ExpectedCalls[:1]
						}
						mockTableExecutor.ExpectedCalls = nil
					}
				}
			}
		})
	}
}

func TestAgentHandleMessage(t *testing.T) {
	t.Parallel()

	a := newBaseAgent4Test()
	a.tableExec = base.NewMockTableExecutor(t)

	heartbeat := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:       "version-1",
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		MsgType:   schedulepb.MsgHeartbeat,
		From:      "owner-1",
		Heartbeat: &schedulepb.Heartbeat{},
	}
	// handle the first heartbeat, from the known owner.
	response, err := a.handleMessage([]*schedulepb.Message{heartbeat})
	require.NoError(t, err)
	require.Len(t, response, 1)
	require.NotNil(t, response[0].HeartbeatResponse)
	require.Equal(t, response[0].Header.Version, a.version)
	require.Equal(t, response[0].Header.OwnerRevision, a.ownerInfo.revision)
	require.Equal(t, response[0].Header.ProcessorEpoch, a.epoch)

	addTableRequest := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        "version-1",
			OwnerRevision:  schedulepb.OwnerRevision{Revision: 1},
			ProcessorEpoch: schedulepb.ProcessorEpoch{Epoch: "agent-epoch-1"},
		},
		MsgType: schedulepb.MsgDispatchTableRequest,
		From:    "owner-1",
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					TableID:     1,
					IsSecondary: true,
					Checkpoint:  &schedulepb.Checkpoint{},
				},
			},
		},
	}
	// add table request in pending
	response, err = a.handleMessage([]*schedulepb.Message{addTableRequest})
	require.NoError(t, err)
	require.Equal(t, a.pendingTasks.Len(), 1)

	// a new owner in power, the pending task should be dropped.
	heartbeat.Header.OwnerRevision.Revision = 2
	response, err = a.handleMessage([]*schedulepb.Message{heartbeat})
	require.NoError(t, err)
	require.Equal(t, a.pendingTasks.Len(), 0)
	require.Len(t, response, 1)
}

func TestAgentUpdateOwnerInfo(t *testing.T) {
	t.Parallel()

	a := newBaseAgent4Test()
	ok := a.handleOwnerInfo("owner-1", 1, "version-1")
	require.True(t, ok)

	// staled owner
	ok = a.handleOwnerInfo("owner-2", 0, "version-1")
	require.False(t, ok)

	// new owner with higher revision
	ok = a.handleOwnerInfo("owner-2", 2, "version-1")
	require.True(t, ok)
}

// MockTableExecutor is a mock implementation of TableExecutor.
type MockTableExecutor struct {
	mock.Mock

	t *testing.T
	// it's preferred to use `pipeline.MockPipeline` here to make the test more vivid.
	tables map[model.TableID]pipeline.TableStatus
}

// NewMockTableExecutor creates a new mock table executor.
func NewMockTableExecutor() *MockTableExecutor {
	return &MockTableExecutor{
		tables: map[model.TableID]pipeline.TableStatus{},
	}
}

// AddTable adds a table to the executor.
func (e *MockTableExecutor) AddTable(
	ctx context.Context, tableID model.TableID, startTs model.Ts, isPrepare bool,
) (bool, error) {
	log.Info("AddTable",
		zap.Int64("tableID", tableID),
		zap.Any("startTs", startTs),
		zap.Bool("isPrepare", isPrepare))

	state, ok := e.tables[tableID]
	if ok {
		switch state {
		case pipeline.TableStatusPreparing:
			return true, nil
		case pipeline.TableStatusPrepared:
			if !isPrepare {
				e.tables[tableID] = pipeline.TableStatusReplicating
			}
			return true, nil
		case pipeline.TableStatusReplicating:
			return true, nil
		case pipeline.TableStatusStopped:
			delete(e.tables, tableID)
		}

	}

	args := e.Called(ctx, tableID, startTs, isPrepare)
	if args.Bool(0) {
		e.tables[tableID] = pipeline.TableStatusPreparing
	}

	return args.Bool(0), args.Error(1)
}

// RemoveTable removes a table from the executor.
func (e *MockTableExecutor) RemoveTable(ctx context.Context, tableID model.TableID) bool {
	state, ok := e.tables[tableID]
	if !ok {
		log.Warn("table to be remove is not found", zap.Int64("tableID", tableID))
		return true
	}
	switch state {
	case pipeline.TableStatusStopping, pipeline.TableStatusStopped:
		return true
	case pipeline.TableStatusPreparing, pipeline.TableStatusPrepared, pipeline.TableStatusReplicating:
	default:
	}
	// todo: how to handle table is already in `stopping` ? should return true directly ?
	// the current `processor implementation, does not consider table's state
	log.Info("RemoveTable", zap.Int64("tableID", tableID), zap.Any("state", state))

	args := e.Called(ctx, tableID)
	if args.Bool(0) {
		e.tables[tableID] = pipeline.TableStatusStopping
	}
	return args.Bool(0)
}

// IsAddTableFinished determines if the table has been added.
func (e *MockTableExecutor) IsAddTableFinished(ctx context.Context, tableID model.TableID, isPrepare bool) bool {
	_, ok := e.tables[tableID]
	if !ok {
		log.Panic("table which was added is not found",
			zap.Int64("tableID", tableID),
			zap.Bool("isPrepare", isPrepare))
	}

	args := e.Called(ctx, tableID, isPrepare)
	if args.Bool(0) {
		e.tables[tableID] = pipeline.TableStatusPrepared
		if !isPrepare {
			e.tables[tableID] = pipeline.TableStatusReplicating
		}
	}
	return args.Bool(0)
}

// IsRemoveTableFinished determines if the table has been removed.
func (e *MockTableExecutor) IsRemoveTableFinished(ctx context.Context, tableID model.TableID) (model.Ts, bool) {
	state, ok := e.tables[tableID]
	if !ok {
		log.Warn("table to be removed is not found",
			zap.Int64("tableID", tableID))
		return 0, true
	}
	args := e.Called(ctx, tableID)
	if args.Bool(1) {
		log.Info("remove table finished, remove it from the executor",
			zap.Int64("tableID", tableID), zap.Any("state", state))
		delete(e.tables, tableID)
	}
	return model.Ts(args.Int(0)), args.Bool(1)
}

// GetAllCurrentTables returns all tables that are currently being adding, running, or removing.
func (e *MockTableExecutor) GetAllCurrentTables() []model.TableID {
	var result []model.TableID
	for tableID := range e.tables {
		result = append(result, tableID)
	}
	return result
}

// GetCheckpoint returns the last checkpoint.
func (e *MockTableExecutor) GetCheckpoint() (checkpointTs, resolvedTs model.Ts) {
	args := e.Called()
	return args.Get(0).(model.Ts), args.Get(1).(model.Ts)
}

// GetTableMeta implements TableExecutor interface
func (e *MockTableExecutor) GetTableMeta(tableID model.TableID) *pipeline.TableMeta {
	state, ok := e.tables[tableID]
	if !ok {
		state = pipeline.TableStatusAbsent
	}
	return &pipeline.TableMeta{
		TableID:      tableID,
		CheckpointTs: 0,
		ResolvedTs:   0,
		Status:       state,
	}
}
