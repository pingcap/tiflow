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

func newAgent4Test() *agent {
	return &agent{
		ownerInfo: ownerInfo{
			version:   "owner-version-1",
			captureID: "owner-1",
			revision:  schedulepb.OwnerRevision{Revision: 1},
		},
		version:   "agent-version-1",
		epoch:     schedulepb.ProcessorEpoch{Epoch: "agent-epoch-1"},
		captureID: "agent-1",
	}
}

func TestAgentHandleMessageDispatchTable(t *testing.T) {
	t.Parallel()

	a := newAgent4Test()
	mockTableExecutor := newMockTableExecutor()
	a.tableM = newTableManager(mockTableExecutor)

	removeTableRequest := &schedulepb.DispatchTableRequest{
		Request: &schedulepb.DispatchTableRequest_RemoveTable{
			RemoveTable: &schedulepb.RemoveTableRequest{
				TableID: 1,
			},
		},
	}
	processorEpoch := schedulepb.ProcessorEpoch{Epoch: "agent-epoch-1"}

	// remove table not exist
	ctx := context.Background()
	a.handleMessageDispatchTableRequest(removeTableRequest, processorEpoch)
	responses, err := a.tableM.poll(ctx, model.LivenessCaptureAlive)
	require.NoError(t, err)
	require.Len(t, responses, 0)

	addTableRequest := &schedulepb.DispatchTableRequest{
		Request: &schedulepb.DispatchTableRequest_AddTable{
			AddTable: &schedulepb.AddTableRequest{
				TableID:     1,
				IsSecondary: true,
			},
		},
	}

	// stopping, addTableRequest should be ignored.
	a.handleLivenessUpdate(model.LivenessCaptureStopping, livenessSourceTick)
	a.handleMessageDispatchTableRequest(addTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx, model.LivenessCaptureAlive)
	require.NoError(t, err)
	require.Len(t, responses, 0)

	// Force set liveness to alive.
	a.liveness = model.LivenessCaptureAlive
	mockTableExecutor.On("AddTable", mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(false, nil)
	a.handleMessageDispatchTableRequest(addTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx, a.liveness)
	require.NoError(t, err)
	require.Len(t, responses, 1)

	addTableResponse, ok := responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), addTableResponse.AddTable.Status.TableID)
	require.Equal(t, schedulepb.TableStateAbsent, addTableResponse.AddTable.Status.State)
	require.NotContains(t, a.tableM.tables, model.TableID(1))

	mockTableExecutor.ExpectedCalls = nil
	mockTableExecutor.On("AddTable", mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(true, nil)
	mockTableExecutor.On("IsAddTableFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(false, nil)
	a.handleMessageDispatchTableRequest(addTableRequest, processorEpoch)
	_, err = a.tableM.poll(ctx, model.LivenessCaptureAlive)
	require.NoError(t, err)

	mockTableExecutor.ExpectedCalls = mockTableExecutor.ExpectedCalls[:1]
	mockTableExecutor.On("IsAddTableFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(true, nil)
	a.handleMessageDispatchTableRequest(addTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx, model.LivenessCaptureAlive)
	require.NoError(t, err)
	require.Len(t, responses, 1)

	addTableResponse, ok = responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), addTableResponse.AddTable.Status.TableID)
	require.Equal(t, schedulepb.TableStatePrepared, addTableResponse.AddTable.Status.State)
	require.Contains(t, a.tableM.tables, model.TableID(1))

	// let the prepared table become replicating, by set `IsSecondary` to false.
	addTableRequest.Request.(*schedulepb.DispatchTableRequest_AddTable).
		AddTable.IsSecondary = false

	// only mock `IsAddTableFinished`, since `AddTable` by start a prepared table always success.
	mockTableExecutor.ExpectedCalls = nil
	mockTableExecutor.On("IsAddTableFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(false, nil)

	a.handleMessageDispatchTableRequest(addTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx, model.LivenessCaptureAlive)
	require.NoError(t, err)
	require.Len(t, responses, 1)

	addTableResponse, ok = responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), addTableResponse.AddTable.Status.TableID)
	require.Equal(t, schedulepb.TableStatePrepared, addTableResponse.AddTable.Status.State)
	require.Contains(t, a.tableM.tables, model.TableID(1))

	mockTableExecutor.ExpectedCalls = nil
	mockTableExecutor.On("IsAddTableFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(true, nil)
	a.handleMessageDispatchTableRequest(addTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx, model.LivenessCaptureAlive)
	require.NoError(t, err)
	require.Len(t, responses, 1)

	addTableResponse, ok = responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), addTableResponse.AddTable.Status.TableID)
	require.Equal(t, schedulepb.TableStateReplicating, addTableResponse.AddTable.Status.State)
	require.Contains(t, a.tableM.tables, model.TableID(1))

	mockTableExecutor.On("RemoveTable", mock.Anything, mock.Anything).
		Return(false)
	// remove table in the replicating state failed, should still in replicating.
	a.handleMessageDispatchTableRequest(removeTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx, model.LivenessCaptureAlive)
	require.NoError(t, err)
	require.Len(t, responses, 1)
	removeTableResponse, ok := responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_RemoveTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), removeTableResponse.RemoveTable.Status.TableID)
	require.Equal(t, schedulepb.TableStateStopping, removeTableResponse.RemoveTable.Status.State)
	require.Contains(t, a.tableM.tables, model.TableID(1))

	mockTableExecutor.ExpectedCalls = nil
	mockTableExecutor.On("RemoveTable", mock.Anything, mock.Anything).
		Return(true)
	mockTableExecutor.On("IsRemoveTableFinished", mock.Anything, mock.Anything).
		Return(3, false)
	// remove table in the replicating state failed, should still in replicating.
	a.handleMessageDispatchTableRequest(removeTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx, model.LivenessCaptureAlive)
	require.NoError(t, err)
	require.Len(t, responses, 1)
	removeTableResponse, ok = responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_RemoveTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), removeTableResponse.RemoveTable.Status.TableID)
	require.Equal(t, schedulepb.TableStateStopping, removeTableResponse.RemoveTable.Status.State)

	mockTableExecutor.ExpectedCalls = mockTableExecutor.ExpectedCalls[:1]
	mockTableExecutor.On("IsRemoveTableFinished", mock.Anything, mock.Anything).
		Return(3, true)
	// remove table in the replicating state success, should in stopped
	a.handleMessageDispatchTableRequest(removeTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx, model.LivenessCaptureAlive)
	require.NoError(t, err)
	require.Len(t, responses, 1)
	removeTableResponse, ok = responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_RemoveTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), removeTableResponse.RemoveTable.Status.TableID)
	require.Equal(t, schedulepb.TableStateStopped, removeTableResponse.RemoveTable.Status.State)
	require.Equal(t, model.Ts(3), removeTableResponse.RemoveTable.Checkpoint.CheckpointTs)
	require.NotContains(t, a.tableM.tables, model.TableID(1))
}

func TestAgentHandleMessageHeartbeat(t *testing.T) {
	t.Parallel()

	a := newAgent4Test()
	mockTableExecutor := newMockTableExecutor()
	a.tableM = newTableManager(mockTableExecutor)

	for i := 0; i < 5; i++ {
		a.tableM.addTable(model.TableID(i))
	}

	a.tableM.tables[model.TableID(0)].state = schedulepb.TableStatePreparing
	a.tableM.tables[model.TableID(1)].state = schedulepb.TableStatePrepared
	a.tableM.tables[model.TableID(2)].state = schedulepb.TableStateReplicating
	a.tableM.tables[model.TableID(3)].state = schedulepb.TableStateStopping
	a.tableM.tables[model.TableID(4)].state = schedulepb.TableStateStopped

	mockTableExecutor.tables[model.TableID(0)] = pipeline.TableStatePreparing
	mockTableExecutor.tables[model.TableID(1)] = pipeline.TableStatePrepared
	mockTableExecutor.tables[model.TableID(2)] = pipeline.TableStateReplicating
	mockTableExecutor.tables[model.TableID(3)] = pipeline.TableStateStopping
	mockTableExecutor.tables[model.TableID(4)] = pipeline.TableStateStopped

	heartbeat := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:       "version-1",
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		MsgType: schedulepb.MsgHeartbeat,
		From:    "owner-1",
		Heartbeat: &schedulepb.Heartbeat{
			TableIDs: []model.TableID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}

	response := a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Len(t, response, 1)
	require.Equal(t, model.LivenessCaptureAlive, response[0].GetHeartbeatResponse().Liveness)

	result := response[0].GetHeartbeatResponse().Tables
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

	a.tableM.tables[model.TableID(1)].task = &dispatchTableTask{IsRemove: true}
	response = a.handleMessage([]*schedulepb.Message{heartbeat})
	result = response[0].GetHeartbeatResponse().Tables
	sort.Slice(result, func(i, j int) bool {
		return result[i].TableID < result[j].TableID
	})
	require.Equal(t, schedulepb.TableStateStopping, result[1].State)

	a.handleLivenessUpdate(model.LivenessCaptureStopping, livenessSourceTick)
	response = a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Len(t, response, 1)
	require.Equal(t, model.LivenessCaptureStopping, response[0].GetHeartbeatResponse().Liveness)

	a.handleLivenessUpdate(model.LivenessCaptureAlive, livenessSourceTick)

	heartbeat.Heartbeat.IsStopping = true
	response = a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Equal(t, model.LivenessCaptureStopping, response[0].GetHeartbeatResponse().Liveness)
	require.Equal(t, model.LivenessCaptureStopping, a.liveness)
}

func TestAgentPermuteMessages(t *testing.T) {
	t.Parallel()

	a := newAgent4Test()
	mockTableExecutor := newMockTableExecutor()
	a.tableM = newTableManager(mockTableExecutor)

	trans := newMockTrans()
	a.trans = trans

	// all possible inbound Messages can be received
	var inboundMessages []*schedulepb.Message
	inboundMessages = append(inboundMessages, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        a.ownerInfo.version,
			OwnerRevision:  a.ownerInfo.revision,
			ProcessorEpoch: a.epoch,
		},
		MsgType: schedulepb.MsgDispatchTableRequest,
		From:    a.ownerInfo.captureID,
		To:      a.captureID,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{
					TableID: 1,
				},
			},
		},
	})
	for _, isSecondary := range []bool{true, false} {
		inboundMessages = append(inboundMessages, &schedulepb.Message{
			Header: &schedulepb.Message_Header{
				Version:        a.ownerInfo.version,
				OwnerRevision:  a.ownerInfo.revision,
				ProcessorEpoch: a.epoch,
			},
			MsgType: schedulepb.MsgDispatchTableRequest,
			From:    a.ownerInfo.captureID,
			To:      a.captureID,
			DispatchTableRequest: &schedulepb.DispatchTableRequest{
				Request: &schedulepb.DispatchTableRequest_AddTable{
					AddTable: &schedulepb.AddTableRequest{
						TableID:     1,
						IsSecondary: isSecondary,
					},
				},
			},
		})
	}

	inboundMessages = append(inboundMessages, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        "version-1",
			OwnerRevision:  schedulepb.OwnerRevision{Revision: 1},
			ProcessorEpoch: a.epoch,
		},
		MsgType: schedulepb.MsgHeartbeat,
		From:    "owner-1",
		Heartbeat: &schedulepb.Heartbeat{
			TableIDs: []model.TableID{1},
		},
	})

	states := []schedulepb.TableState{
		schedulepb.TableStateAbsent,
		schedulepb.TableStatePreparing,
		schedulepb.TableStatePrepared,
		schedulepb.TableStateReplicating,
		schedulepb.TableStateStopping,
		schedulepb.TableStateStopped,
	}
	ctx := context.Background()
	tableID := model.TableID(1)
	for _, state := range states {
		iterPermutation([]int{0, 1, 2, 3}, func(sequence []int) {
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
				message := inboundMessages[idx]
				if message.MsgType == schedulepb.MsgHeartbeat {
					trans.recvBuffer = append(trans.recvBuffer, message)
					err := a.Tick(ctx, model.LivenessCaptureAlive)
					require.NoError(t, err)
					require.Len(t, trans.sendBuffer, 1)
					heartbeatResponse := trans.sendBuffer[0].HeartbeatResponse
					trans.sendBuffer = trans.sendBuffer[:0]
					require.Equal(t, model.LivenessCaptureAlive, heartbeatResponse.Liveness)

					continue
				}

				switch message.DispatchTableRequest.Request.(type) {
				case *schedulepb.DispatchTableRequest_AddTable:
					for _, ok := range []bool{false, true} {
						mockTableExecutor.On("AddTable", mock.Anything, mock.Anything,
							mock.Anything, mock.Anything).Return(ok, nil)
						for _, ok1 := range []bool{false, true} {
							mockTableExecutor.On("IsAddTableFinished", mock.Anything,
								mock.Anything, mock.Anything).Return(ok1, nil)

							trans.recvBuffer = append(trans.recvBuffer, message)
							err := a.Tick(ctx, model.LivenessCaptureAlive)
							require.NoError(t, err)
							trans.sendBuffer = trans.sendBuffer[:0]

							mockTableExecutor.ExpectedCalls = mockTableExecutor.ExpectedCalls[:1]
						}
						mockTableExecutor.ExpectedCalls = nil
					}
				case *schedulepb.DispatchTableRequest_RemoveTable:
					for _, ok := range []bool{false, true} {
						mockTableExecutor.On("RemoveTable", mock.Anything,
							mock.Anything).Return(ok)
						for _, ok1 := range []bool{false, true} {
							trans.recvBuffer = append(trans.recvBuffer, message)
							mockTableExecutor.On("IsRemoveTableFinished",
								mock.Anything, mock.Anything).Return(0, ok1)
							err := a.Tick(ctx, model.LivenessCaptureAlive)
							require.NoError(t, err)
							if len(trans.sendBuffer) != 0 {
								require.Len(t, trans.sendBuffer, 1)
								response, yes := trans.sendBuffer[0].DispatchTableResponse.
									Response.(*schedulepb.DispatchTableResponse_RemoveTable)
								trans.sendBuffer = trans.sendBuffer[:0]
								require.True(t, yes)
								expected := schedulepb.TableStateStopping
								if ok && ok1 {
									expected = schedulepb.TableStateStopped
								}
								require.Equal(t, expected, response.RemoveTable.Status.State)
								mockTableExecutor.ExpectedCalls = mockTableExecutor.
									ExpectedCalls[:1]
							}
						}
						mockTableExecutor.ExpectedCalls = nil
					}
				default:
					panic("unknown request")
				}
			}
		})
	}
}

func TestAgentHandleMessage(t *testing.T) {
	t.Parallel()

	mockTableExecutor := newMockTableExecutor()
	tableM := newTableManager(mockTableExecutor)
	a := newAgent4Test()
	a.tableM = tableM

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
	responses := a.handleMessage([]*schedulepb.Message{addTableRequest})
	require.NotContains(t, tableM.tables, model.TableID(1))
	require.Len(t, responses, 0)

	// correct epoch, processing.
	addTableRequest.Header.ProcessorEpoch = a.epoch
	_ = a.handleMessage([]*schedulepb.Message{addTableRequest})
	require.Contains(t, tableM.tables, model.TableID(1))

	heartbeat.Header.OwnerRevision.Revision = 2
	response = a.handleMessage([]*schedulepb.Message{heartbeat})
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

	a := newAgent4Test()
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

	a := newAgent4Test()
	trans := newMockTrans()
	mockTableExecutor := newMockTableExecutor()
	a.trans = trans
	a.tableM = newTableManager(mockTableExecutor)

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
	trans.recvBuffer = append(trans.recvBuffer, heartbeat)

	ctx := context.Background()
	require.NoError(t, a.Tick(ctx, model.LivenessCaptureAlive))
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
	var messages []*schedulepb.Message
	messages = append(messages, addTableRequest)
	messages = append(messages, removeTableRequest)
	trans.recvBuffer = append(trans.recvBuffer, messages...)

	mockTableExecutor.On("AddTable", mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	mockTableExecutor.On("IsAddTableFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(false, nil)
	require.NoError(t, a.Tick(ctx, model.LivenessCaptureAlive))
	trans.sendBuffer = trans.sendBuffer[:0]

	trans.recvBuffer = append(trans.recvBuffer, addTableRequest)

	mockTableExecutor.ExpectedCalls = mockTableExecutor.ExpectedCalls[:1]
	mockTableExecutor.On("IsAddTableFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(true, nil)
	require.NoError(t, a.Tick(ctx, model.LivenessCaptureAlive))
	responses := trans.sendBuffer[:len(trans.sendBuffer)]
	trans.sendBuffer = trans.sendBuffer[:0]
	require.Len(t, responses, 1)
	require.Equal(t, schedulepb.MsgDispatchTableResponse, responses[0].MsgType)
	resp, ok := responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.Equal(t, schedulepb.TableStatePrepared, resp.AddTable.Status.State)

	require.NoError(t, a.Close())
}

func TestAgentHandleLivenessUpdate(t *testing.T) {
	t.Parallel()

	// Test liveness via tick.
	a := newAgent4Test()
	a.handleLivenessUpdate(model.LivenessCaptureAlive, livenessSourceTick)
	require.Equal(t, model.LivenessCaptureAlive, a.liveness)

	a.handleLivenessUpdate(model.LivenessCaptureStopping, livenessSourceTick)
	require.Equal(t, model.LivenessCaptureStopping, a.liveness)

	a.handleLivenessUpdate(model.LivenessCaptureAlive, livenessSourceTick)
	require.Equal(t, model.LivenessCaptureStopping, a.liveness)

	// Test liveness via heartbeat.
	mockTableExecutor := newMockTableExecutor()
	tableM := newTableManager(mockTableExecutor)
	a = newAgent4Test()
	a.tableM = tableM
	require.Equal(t, model.LivenessCaptureAlive, a.liveness)
	a.handleMessage([]*schedulepb.Message{{
		Header: &schedulepb.Message_Header{
			Version:        a.ownerInfo.version,
			OwnerRevision:  a.ownerInfo.revision,
			ProcessorEpoch: a.epoch,
		},
		MsgType: schedulepb.MsgHeartbeat,
		From:    a.ownerInfo.captureID,
		Heartbeat: &schedulepb.Heartbeat{
			IsStopping: true,
		},
	}})
	require.Equal(t, model.LivenessCaptureStopping, a.liveness)

	a.handleLivenessUpdate(model.LivenessCaptureAlive, livenessSourceTick)
	require.Equal(t, model.LivenessCaptureStopping, a.liveness)
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

// IsAddTableFinished determines if the table has been added.
func (e *MockTableExecutor) IsAddTableFinished(ctx context.Context,
	tableID model.TableID, isPrepare bool,
) bool {
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
		return true
	}

	e.tables[tableID] = pipeline.TableStatePreparing
	if !isPrepare {
		e.tables[tableID] = pipeline.TableStatePrepared
	}

	return false
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
	// the current `processor implementation, does not consider table's state
	log.Info("RemoveTable", zap.Int64("tableID", tableID), zap.Any("state", state))

	args := e.Called(ctx, tableID)
	if args.Bool(0) {
		e.tables[tableID] = pipeline.TableStateStopped
	}
	return args.Bool(0)
}

// IsRemoveTableFinished determines if the table has been removed.
func (e *MockTableExecutor) IsRemoveTableFinished(ctx context.Context,
	tableID model.TableID,
) (model.Ts, bool) {
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
	} else {
		// revert the state back to old state, assume it's `replicating`,
		// but `preparing` / `prepared` can also be removed.
		e.tables[tableID] = pipeline.TableStateReplicating
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
