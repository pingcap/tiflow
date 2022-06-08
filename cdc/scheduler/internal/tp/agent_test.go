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
	"sort"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
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
		captureID:    "agent-1",
		runningTasks: make(map[model.TableID]*dispatchTableTask),
	}
}

func TestAgentCollectTableStatus(t *testing.T) {
	t.Parallel()

	a := newBaseAgent4Test()

	mockTableExecutor := newMockTableExecutor()
	a.tableExec = mockTableExecutor

	mockTableExecutor.tables[model.TableID(0)] = pipeline.TableStatePreparing
	mockTableExecutor.tables[model.TableID(1)] = pipeline.TableStatePrepared
	mockTableExecutor.tables[model.TableID(2)] = pipeline.TableStateReplicating
	mockTableExecutor.tables[model.TableID(3)] = pipeline.TableStateStopping
	mockTableExecutor.tables[model.TableID(4)] = pipeline.TableStateStopped

	expected := make([]model.TableID, 0, 10)
	for i := 0; i < 10; i++ {
		expected = append(expected, model.TableID(i))
	}

	result := a.collectTableStatus(expected)
	require.Len(t, result, 10)
	sort.Slice(result, func(i, j int) bool {
		return result[i].TableID < result[j].TableID
	})
	require.Equal(t, schedulepb.TableStatePreparing, result[0].State)
	require.Equal(t, schedulepb.TableStatePrepared, result[1].State)
	require.Equal(t, schedulepb.TableStateReplicating, result[2].State)
	require.Equal(t, schedulepb.TableStateStopping, result[3].State)
	require.Equal(t, schedulepb.TableStateStopped, result[4].State)
	for i := 5; i < 10; i++ {
		require.Equal(t, schedulepb.TableStateAbsent, result[i].State)
	}

	a.runningTasks[model.TableID(0)] = &dispatchTableTask{IsRemove: true}
	status := a.newTableStatus(model.TableID(0))
	require.Equal(t, schedulepb.TableStateStopping, status.State)

	a.runningTasks[model.TableID(10)] = &dispatchTableTask{IsRemove: true}
	status = a.newTableStatus(model.TableID(10))
	require.Equal(t, schedulepb.TableStateAbsent, status.State)
}

func TestAgentHandleDispatchTableTask(t *testing.T) {
	t.Parallel()

	a := newBaseAgent4Test()

	mockTableExecutor := newMockTableExecutor()
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
				mockTableExecutor.tables[tableID] = pipeline.TableStatePreparing
			case schedulepb.TableStatePrepared:
				mockTableExecutor.tables[tableID] = pipeline.TableStatePrepared
			case schedulepb.TableStateReplicating:
				mockTableExecutor.tables[tableID] = pipeline.TableStateReplicating
			case schedulepb.TableStateStopping:
				mockTableExecutor.tables[tableID] = pipeline.TableStateStopping
			case schedulepb.TableStateStopped:
				mockTableExecutor.tables[tableID] = pipeline.TableStateStopped
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

func TestAgentHandleMessageStopping(t *testing.T) {
	t.Parallel()

	a := newBaseAgent4Test()
	a.tableExec = newMockTableExecutor()
	a.stopping = true

	heartbeat := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:       "version-1",
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		MsgType:   schedulepb.MsgHeartbeat,
		From:      "owner-1",
		Heartbeat: &schedulepb.Heartbeat{},
	}
	response := a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Len(t, response, 1)
	require.NotNil(t, response[0].HeartbeatResponse)
	// agent is stopping, let coordinator know this.
	require.True(t, response[0].HeartbeatResponse.IsStopping)

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
				},
			},
		},
	}
	// add table request should not be handled, so the running task count is 0.
	response = a.handleMessage([]*schedulepb.Message{addTableRequest})
	require.Len(t, response, 0)
	require.Len(t, a.runningTasks, 0)

	// mock agent have running task before stopping but processed yet.
	a.runningTasks[model.TableID(1)] = &dispatchTableTask{
		TableID:   model.TableID(1),
		StartTs:   0,
		IsRemove:  false,
		IsPrepare: false,
		Epoch:     schedulepb.ProcessorEpoch{},
		status:    dispatchTableTaskReceived,
	}

	result, err := a.handleDispatchTableTasks(context.Background())
	require.NoError(t, err)
	require.Len(t, a.runningTasks, 0)
	require.Len(t, result, 1)

	addTableResponse, ok := result[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.True(t, addTableResponse.AddTable.Reject)
}

func TestAgentHandleRemoveTableRequest(t *testing.T) {
	t.Parallel()

	a := newBaseAgent4Test()
	a.tableExec = newMockTableExecutor()

	// remove a table not exist, should not generate the task.
	removeTableRequest := &schedulepb.DispatchTableRequest{
		Request: &schedulepb.DispatchTableRequest_RemoveTable{
			RemoveTable: &schedulepb.RemoveTableRequest{
				TableID: 2,
			},
		},
	}

	a.handleMessageDispatchTableRequest(removeTableRequest, a.epoch)
	require.Len(t, a.runningTasks, 0)
}

func TestAgentHandleMessage(t *testing.T) {
	t.Parallel()

	a := newBaseAgent4Test()
	a.tableExec = newMockTableExecutor()

	heartbeat := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:       a.ownerInfo.version,
			OwnerRevision: a.ownerInfo.revision,
		},
		MsgType:   schedulepb.MsgHeartbeat,
		From:      a.ownerInfo.captureID,
		Heartbeat: &schedulepb.Heartbeat{},
	}
	// handle the first heartbeat, from the known owner.
	response := a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Len(t, response, 1)
	require.NotNil(t, response[0].HeartbeatResponse)
	require.Equal(t, response[0].Header.Version, a.version)
	require.Equal(t, response[0].Header.OwnerRevision, a.ownerInfo.revision)
	require.Equal(t, response[0].Header.ProcessorEpoch, a.epoch)

	addTableRequest := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:       a.ownerInfo.version,
			OwnerRevision: a.ownerInfo.revision,
			// wrong epoch
			ProcessorEpoch: schedulepb.ProcessorEpoch{Epoch: "wrong-agent-epoch-1"},
		},
		MsgType: schedulepb.MsgDispatchTableRequest,
		From:    a.ownerInfo.captureID,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					TableID:     1,
					IsSecondary: true,
					Checkpoint:  schedulepb.Checkpoint{},
				},
			},
		},
	}
	// wrong epoch, ignored
	response = a.handleMessage([]*schedulepb.Message{addTableRequest})
	require.Len(t, a.runningTasks, 0)
	require.Len(t, response, 0)

	// correct epoch, processing.
	addTableRequest.Header.ProcessorEpoch = a.epoch
	response = a.handleMessage([]*schedulepb.Message{addTableRequest})
	require.Len(t, a.runningTasks, 1)
	require.Len(t, response, 0)

	heartbeat.Header.OwnerRevision.Revision = 2
	response = a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Equal(t, len(a.runningTasks), 1)
	require.Len(t, response, 1)

	// this should never happen in real world
	unknownMessage := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        a.ownerInfo.version,
			OwnerRevision:  schedulepb.OwnerRevision{Revision: 2},
			ProcessorEpoch: a.epoch,
		},
		MsgType: schedulepb.MsgUnknown,
		From:    a.ownerInfo.captureID,
	}

	response = a.handleMessage([]*schedulepb.Message{unknownMessage})
	require.Len(t, response, 0)

	// staled message
	heartbeat.Header.OwnerRevision.Revision = 1
	response = a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Len(t, response, 0)
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

func TestAgentTick(t *testing.T) {
	t.Parallel()

	a := newBaseAgent4Test()
	trans := newMockTrans()
	mockTableExecutor := newMockTableExecutor()
	a.trans = trans
	a.tableExec = mockTableExecutor

	heartbeat := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:       a.ownerInfo.version,
			OwnerRevision: a.ownerInfo.revision,
			// first heartbeat from the owner, no processor epoch
			ProcessorEpoch: schedulepb.ProcessorEpoch{},
		},
		MsgType:   schedulepb.MsgHeartbeat,
		From:      a.ownerInfo.captureID,
		Heartbeat: &schedulepb.Heartbeat{TableIDs: nil},
	}

	// receive first heartbeat from the owner
	messages := []*schedulepb.Message{heartbeat}
	trans.recvBuffer = append(trans.recvBuffer, messages...)

	require.NoError(t, a.Tick(context.Background()))
	require.Len(t, trans.sendBuffer, 1)
	heartbeatResponse := trans.sendBuffer[0]
	trans.sendBuffer = trans.sendBuffer[:0]

	require.Equal(t, schedulepb.MsgHeartbeatResponse, heartbeatResponse.MsgType)
	require.Equal(t, a.ownerInfo.captureID, heartbeatResponse.To)
	require.Equal(t, a.captureID, heartbeatResponse.From)

	addTableRequest := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        a.ownerInfo.version,
			OwnerRevision:  a.ownerInfo.revision,
			ProcessorEpoch: a.epoch,
		},
		MsgType: schedulepb.MsgDispatchTableRequest,
		From:    a.ownerInfo.captureID,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					TableID:     1,
					IsSecondary: true,
					Checkpoint:  schedulepb.Checkpoint{},
				},
			},
		},
	}

	removeTableRequest := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        a.ownerInfo.version,
			OwnerRevision:  a.ownerInfo.revision,
			ProcessorEpoch: a.epoch,
		},
		MsgType: schedulepb.MsgDispatchTableRequest,
		From:    a.ownerInfo.captureID,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{
					TableID: 2,
				},
			},
		},
	}
	messages = append(messages, addTableRequest)
	messages = append(messages, removeTableRequest)
	trans.recvBuffer = append(trans.recvBuffer, messages...)

	mockTableExecutor.On("AddTable", mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	mockTableExecutor.On("IsAddTableFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(false, nil)
	require.NoError(t, a.Tick(context.Background()))
	responses := trans.sendBuffer[:len(trans.sendBuffer)]
	trans.sendBuffer = trans.sendBuffer[:0]
	require.Equal(t, schedulepb.MsgHeartbeatResponse, responses[0].MsgType)

	messages = messages[:0]
	// this one should be ignored, since the previous one with the same tableID is not finished yet.
	messages = append(messages, addTableRequest)
	trans.recvBuffer = append(trans.recvBuffer, messages...)

	mockTableExecutor.ExpectedCalls = mockTableExecutor.ExpectedCalls[:1]
	mockTableExecutor.On("IsAddTableFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(true, nil)
	require.NoError(t, a.Tick(context.Background()))
	responses = trans.sendBuffer[:len(trans.sendBuffer)]
	trans.sendBuffer = trans.sendBuffer[:0]
	require.Len(t, responses, 1)
	require.Equal(t, schedulepb.MsgDispatchTableResponse, responses[0].MsgType)
	resp, ok := responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.Equal(t, schedulepb.TableStatePrepared, resp.AddTable.Status.State)

	require.NoError(t, a.Close())
}

// MockTableExecutor is a mock implementation of TableExecutor.
type MockTableExecutor struct {
	mock.Mock

	t *testing.T
	// it's preferred to use `pipeline.MockPipeline` here to make the test more vivid.
	tables map[model.TableID]pipeline.TableState
}

// newMockTableExecutor creates a new mock table executor.
func newMockTableExecutor() *MockTableExecutor {
	return &MockTableExecutor{
		tables: map[model.TableID]pipeline.TableState{},
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
		case pipeline.TableStatePreparing:
			return true, nil
		case pipeline.TableStatePrepared:
			if !isPrepare {
				e.tables[tableID] = pipeline.TableStateReplicating
			}
			return true, nil
		case pipeline.TableStateReplicating:
			return true, nil
		case pipeline.TableStateStopped:
			delete(e.tables, tableID)
		}
	}

	args := e.Called(ctx, tableID, startTs, isPrepare)
	if args.Bool(0) {
		e.tables[tableID] = pipeline.TableStatePreparing
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
	case pipeline.TableStateStopping, pipeline.TableStateStopped:
		return true
	case pipeline.TableStatePreparing, pipeline.TableStatePrepared, pipeline.TableStateReplicating:
	default:
	}
	// todo: how to handle table is already in `stopping` ? should return true directly ?
	// the current `processor implementation, does not consider table's state
	log.Info("RemoveTable", zap.Int64("tableID", tableID), zap.Any("state", state))

	args := e.Called(ctx, tableID)
	if args.Bool(0) {
		e.tables[tableID] = pipeline.TableStateStopping
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
		e.tables[tableID] = pipeline.TableStatePrepared
		if !isPrepare {
			e.tables[tableID] = pipeline.TableStateReplicating
		}
	}
	return args.Bool(0)
}

// IsRemoveTableFinished determines if the table has been removed.
func (e *MockTableExecutor) IsRemoveTableFinished(ctx context.Context, tableID model.TableID) (model.Ts, bool) {
	state, ok := e.tables[tableID]
	if !ok {
		// the real `table executor` processor, would panic in such case.
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
func (e *MockTableExecutor) GetTableMeta(tableID model.TableID) pipeline.TableMeta {
	state, ok := e.tables[tableID]
	if !ok {
		state = pipeline.TableStateAbsent
	}
	return pipeline.TableMeta{
		TableID:      tableID,
		CheckpointTs: 0,
		ResolvedTs:   0,
		State:        state,
	}
}
