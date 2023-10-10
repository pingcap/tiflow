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

package agent

import (
	"context"
	"sort"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/compat"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/transport"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	mock_etcd "github.com/pingcap/tiflow/pkg/etcd/mock"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

// See https://stackoverflow.com/a/30230552/3920448 for details.
func nextPerm(p []int) {
	for i := len(p) - 1; i >= 0; i-- {
		if i == 0 || p[i] < len(p)-i-1 {
			p[i]++
			return
		}
		p[i] = 0
	}
}

func getPerm(orig, p []int) []int {
	result := append([]int{}, orig...)
	for i, v := range p {
		result[i], result[i+v] = result[i+v], result[i]
	}
	return result
}

func iterPermutation(sequence []int, fn func(sequence []int)) {
	for p := make([]int, len(sequence)); p[0] < len(p); nextPerm(p) {
		fn(getPerm(sequence, p))
	}
}

func newAgent4Test() *agent {
	cfg := config.GetDefaultServerConfig().Debug.Scheduler
	cfg.ChangefeedSettings = config.GetDefaultReplicaConfig().Scheduler
	a := &agent{
		ownerInfo: ownerInfo{
			CaptureInfo: model.CaptureInfo{
				Version: "owner-version-1",
				ID:      "owner-1",
			},
			Revision: schedulepb.OwnerRevision{Revision: 1},
		},
		compat: compat.New(cfg, map[string]*model.CaptureInfo{}),
	}

	a.Version = "agent-version-1"
	a.Epoch = schedulepb.ProcessorEpoch{Epoch: "agent-epoch-1"}
	a.CaptureID = "agent-1"
	liveness := model.LivenessCaptureAlive
	a.liveness = &liveness
	return a
}

func TestNewAgent(t *testing.T) {
	t.Parallel()

	liveness := model.LivenessCaptureAlive
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	me := mock_etcd.NewMockCDCEtcdClient(gomock.NewController(t))

	tableExector := newMockTableExecutor()
	cfg := &config.SchedulerConfig{
		ChangefeedSettings: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        1,
		},
	}

	// owner and revision found successfully
	me.EXPECT().GetOwnerID(gomock.Any()).Return("ownerID", nil).Times(1)
	me.EXPECT().GetCaptures(
		gomock.Any()).Return(int64(0), []*model.CaptureInfo{{ID: "ownerID"}}, nil).Times(1)
	me.EXPECT().GetOwnerRevision(gomock.Any(), gomock.Any()).Return(int64(2333), nil).Times(1)
	a, err := newAgent(
		context.Background(), "capture-test", &liveness, changefeed, me, tableExector, 0, cfg)
	require.NoError(t, err)
	require.NotNil(t, a)

	// owner not found temporarily, it's ok.
	me.EXPECT().GetOwnerID(gomock.Any()).
		Return("", concurrency.ErrElectionNoLeader).Times(1)
	a, err = newAgent(
		context.Background(), "capture-test", &liveness, changefeed, me, tableExector, 0, cfg)
	require.NoError(t, err)
	require.NotNil(t, a)

	// owner not found since pd is unstable
	me.EXPECT().GetOwnerID(gomock.Any()).Return("", cerror.ErrPDEtcdAPIError).Times(1)
	a, err = newAgent(
		context.Background(), "capture-test", &liveness, changefeed, me, tableExector, 0, cfg)
	require.Error(t, err)
	require.Nil(t, a)

	// owner found, get revision failed.
	me.EXPECT().GetOwnerID(gomock.Any()).Return("ownerID", nil).Times(1)
	me.EXPECT().GetCaptures(
		gomock.Any()).Return(int64(0), []*model.CaptureInfo{{ID: "ownerID"}}, nil).Times(1)
	me.EXPECT().GetOwnerRevision(gomock.Any(), gomock.Any()).
		Return(int64(0), cerror.ErrPDEtcdAPIError).Times(1)
	a, err = newAgent(
		context.Background(), "capture-test", &liveness, changefeed, me, tableExector, 0, cfg)
	require.Error(t, err)
	require.Nil(t, a)

	me.EXPECT().GetOwnerID(gomock.Any()).Return("ownerID", nil).Times(1)
	me.EXPECT().GetCaptures(
		gomock.Any()).Return(int64(0), []*model.CaptureInfo{{ID: "ownerID"}}, nil).Times(1)
	me.EXPECT().GetOwnerRevision(gomock.Any(), gomock.Any()).
		Return(int64(0), cerror.ErrOwnerNotFound).Times(1)
	a, err = newAgent(
		context.Background(), "capture-test", &liveness, changefeed, me, tableExector, 0, cfg)
	require.NoError(t, err)
	require.NotNil(t, a)
}

func TestAgentHandleMessageDispatchTable(t *testing.T) {
	t.Parallel()

	a := newAgent4Test()
	mockTableExecutor := newMockTableExecutor()
	a.tableM = newTableSpanManager(model.ChangeFeedID{}, mockTableExecutor)

	removeTableRequest := &schedulepb.DispatchTableRequest{
		Request: &schedulepb.DispatchTableRequest_RemoveTable{
			RemoveTable: &schedulepb.RemoveTableRequest{
				Span: spanz.TableIDToComparableSpan(1),
			},
		},
	}
	processorEpoch := schedulepb.ProcessorEpoch{Epoch: "agent-epoch-1"}

	// remove table not exist
	ctx := context.Background()
	a.handleMessageDispatchTableRequest(removeTableRequest, processorEpoch)
	responses, err := a.tableM.poll(ctx)
	require.NoError(t, err)
	require.Len(t, responses, 0)

	addTableRequest := &schedulepb.DispatchTableRequest{
		Request: &schedulepb.DispatchTableRequest_AddTable{
			AddTable: &schedulepb.AddTableRequest{
				Span:        spanz.TableIDToComparableSpan(1),
				IsSecondary: true,
			},
		},
	}

	// addTableRequest should be not ignored even if it's stopping.
	a.handleLivenessUpdate(model.LivenessCaptureStopping)
	require.Equal(t, model.LivenessCaptureStopping, a.liveness.Load())
	mockTableExecutor.On("AddTableSpan", mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(false, nil)
	a.handleMessageDispatchTableRequest(addTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx)
	require.NoError(t, err)
	require.Len(t, responses, 1)

	addTableResponse, ok := responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), addTableResponse.AddTable.Status.Span.TableID)
	require.Equal(t, tablepb.TableStateAbsent, addTableResponse.AddTable.Status.State)
	require.False(t, a.tableM.tables.Has(spanz.TableIDToComparableSpan(1)))

	// Force set liveness to alive.
	*a.liveness = model.LivenessCaptureAlive
	require.Equal(t, model.LivenessCaptureAlive, a.liveness.Load())
	mockTableExecutor.ExpectedCalls = nil
	mockTableExecutor.On("AddTableSpan", mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(true, nil)
	mockTableExecutor.On("IsAddTableSpanFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(false, nil)
	a.handleMessageDispatchTableRequest(addTableRequest, processorEpoch)
	_, err = a.tableM.poll(ctx)
	require.NoError(t, err)

	mockTableExecutor.ExpectedCalls = mockTableExecutor.ExpectedCalls[:1]
	mockTableExecutor.On("IsAddTableSpanFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(true, nil)
	a.handleMessageDispatchTableRequest(addTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx)
	require.NoError(t, err)
	require.Len(t, responses, 1)

	addTableResponse, ok = responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), addTableResponse.AddTable.Status.Span.TableID)
	require.Equal(t, tablepb.TableStatePrepared, addTableResponse.AddTable.Status.State)
	require.True(t, a.tableM.tables.Has(spanz.TableIDToComparableSpan(1)))

	// let the prepared table become replicating, by set `IsSecondary` to false.
	addTableRequest.Request.(*schedulepb.DispatchTableRequest_AddTable).
		AddTable.IsSecondary = false

	// only mock `IsAddTableSpanFinished`, since `AddTable` by start a prepared
	// table span always success.
	mockTableExecutor.ExpectedCalls = nil
	mockTableExecutor.On("IsAddTableSpanFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(false, nil)

	a.handleMessageDispatchTableRequest(addTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx)
	require.NoError(t, err)
	require.Len(t, responses, 1)

	addTableResponse, ok = responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), addTableResponse.AddTable.Status.Span.TableID)
	require.Equal(t, tablepb.TableStatePrepared, addTableResponse.AddTable.Status.State)
	require.True(t, a.tableM.tables.Has(spanz.TableIDToComparableSpan(1)))

	mockTableExecutor.ExpectedCalls = nil
	mockTableExecutor.On("IsAddTableSpanFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(true, nil)
	a.handleMessageDispatchTableRequest(addTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx)
	require.NoError(t, err)
	require.Len(t, responses, 1)

	addTableResponse, ok = responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), addTableResponse.AddTable.Status.Span.TableID)
	require.Equal(t, tablepb.TableStateReplicating, addTableResponse.AddTable.Status.State)
	require.True(t, a.tableM.tables.Has(spanz.TableIDToComparableSpan(1)))

	mockTableExecutor.On("RemoveTableSpan", mock.Anything, mock.Anything).
		Return(false)
	// remove table in the replicating state failed, should still in replicating.
	a.handleMessageDispatchTableRequest(removeTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx)
	require.NoError(t, err)
	require.Len(t, responses, 1)
	removeTableResponse, ok := responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_RemoveTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), removeTableResponse.RemoveTable.Status.Span.TableID)
	require.Equal(t, tablepb.TableStateStopping, removeTableResponse.RemoveTable.Status.State)
	require.True(t, a.tableM.tables.Has(spanz.TableIDToComparableSpan(1)))

	mockTableExecutor.ExpectedCalls = nil
	mockTableExecutor.On("RemoveTableSpan", mock.Anything, mock.Anything).
		Return(true)
	mockTableExecutor.On("IsRemoveTableSpanFinished", mock.Anything, mock.Anything).
		Return(3, false)
	// remove table in the replicating state failed, should still in replicating.
	a.handleMessageDispatchTableRequest(removeTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx)
	require.NoError(t, err)
	require.Len(t, responses, 1)
	removeTableResponse, ok = responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_RemoveTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), removeTableResponse.RemoveTable.Status.Span.TableID)
	require.Equal(t, tablepb.TableStateStopping, removeTableResponse.RemoveTable.Status.State)

	mockTableExecutor.ExpectedCalls = mockTableExecutor.ExpectedCalls[:1]
	mockTableExecutor.On("IsRemoveTableSpanFinished", mock.Anything, mock.Anything).
		Return(3, true)
	// remove table in the replicating state success, should in stopped
	a.handleMessageDispatchTableRequest(removeTableRequest, processorEpoch)
	responses, err = a.tableM.poll(ctx)
	require.NoError(t, err)
	require.Len(t, responses, 1)
	removeTableResponse, ok = responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_RemoveTable)
	require.True(t, ok)
	require.Equal(t, model.TableID(1), removeTableResponse.RemoveTable.Status.Span.TableID)
	require.Equal(t, tablepb.TableStateStopped, removeTableResponse.RemoveTable.Status.State)
	require.Equal(t, model.Ts(3), removeTableResponse.RemoveTable.Checkpoint.CheckpointTs)
	require.False(t, a.tableM.tables.Has(spanz.TableIDToComparableSpan(1)))
}

func TestAgentHandleMessageHeartbeat(t *testing.T) {
	t.Parallel()

	a := newAgent4Test()
	mockTableExecutor := newMockTableExecutor()
	a.tableM = newTableSpanManager(model.ChangeFeedID{}, mockTableExecutor)

	for i := 0; i < 5; i++ {
		a.tableM.addTableSpan(spanz.TableIDToComparableSpan(int64(i)))
	}

	a.tableM.tables.GetV(spanz.TableIDToComparableSpan(0)).state = tablepb.TableStatePreparing
	a.tableM.tables.GetV(spanz.TableIDToComparableSpan(1)).state = tablepb.TableStatePrepared
	a.tableM.tables.GetV(spanz.TableIDToComparableSpan(2)).state = tablepb.TableStateReplicating
	a.tableM.tables.GetV(spanz.TableIDToComparableSpan(3)).state = tablepb.TableStateStopping
	a.tableM.tables.GetV(spanz.TableIDToComparableSpan(4)).state = tablepb.TableStateStopped

	mockTableExecutor.tables.ReplaceOrInsert(
		spanz.TableIDToComparableSpan(0), tablepb.TableStatePreparing)
	mockTableExecutor.tables.ReplaceOrInsert(
		spanz.TableIDToComparableSpan(1), tablepb.TableStatePrepared)
	mockTableExecutor.tables.ReplaceOrInsert(
		spanz.TableIDToComparableSpan(2), tablepb.TableStateReplicating)
	mockTableExecutor.tables.ReplaceOrInsert(
		spanz.TableIDToComparableSpan(3), tablepb.TableStateStopping)
	mockTableExecutor.tables.ReplaceOrInsert(
		spanz.TableIDToComparableSpan(4), tablepb.TableStateStopped)

	heartbeat := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:       "version-1",
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		MsgType: schedulepb.MsgHeartbeat,
		From:    "owner-1",
		Heartbeat: &schedulepb.Heartbeat{
			Spans: []tablepb.Span{
				spanz.TableIDToComparableSpan(0),
				spanz.TableIDToComparableSpan(1),
				spanz.TableIDToComparableSpan(2),
				spanz.TableIDToComparableSpan(3),
				spanz.TableIDToComparableSpan(4),
				spanz.TableIDToComparableSpan(5),
				spanz.TableIDToComparableSpan(6),
				spanz.TableIDToComparableSpan(7),
				spanz.TableIDToComparableSpan(8),
				spanz.TableIDToComparableSpan(9),
			},
		},
	}

	response, _ := a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Len(t, response, 1)
	require.Equal(t, model.LivenessCaptureAlive, response[0].GetHeartbeatResponse().Liveness)

	result := response[0].GetHeartbeatResponse().Tables
	require.Len(t, result, 10)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Span.Less(&result[j].Span)
	})

	require.Equal(t, tablepb.TableStatePreparing, result[0].State)
	require.Equal(t, tablepb.TableStatePrepared, result[1].State)
	require.Equal(t, tablepb.TableStateReplicating, result[2].State)
	require.Equal(t, tablepb.TableStateStopping, result[3].State)
	require.Equal(t, tablepb.TableStateStopped, result[4].State)
	for i := 5; i < 10; i++ {
		require.Equal(t, tablepb.TableStateAbsent, result[i].State)
	}

	a.tableM.tables.GetV(spanz.TableIDToComparableSpan(1)).task = &dispatchTableTask{IsRemove: true}
	response, _ = a.handleMessage([]*schedulepb.Message{heartbeat})
	result = response[0].GetHeartbeatResponse().Tables
	sort.Slice(result, func(i, j int) bool {
		return result[i].Span.TableID < result[j].Span.TableID
	})
	require.Equal(t, tablepb.TableStateStopping, result[1].State)

	a.handleLivenessUpdate(model.LivenessCaptureStopping)
	response, _ = a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Len(t, response, 1)
	require.Equal(t, model.LivenessCaptureStopping, response[0].GetHeartbeatResponse().Liveness)

	a.handleLivenessUpdate(model.LivenessCaptureAlive)
	heartbeat.Heartbeat.IsStopping = true
	response, _ = a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Equal(t, model.LivenessCaptureStopping, response[0].GetHeartbeatResponse().Liveness)
	require.Equal(t, model.LivenessCaptureStopping, a.liveness.Load())
}

func TestAgentPermuteMessages(t *testing.T) {
	t.Parallel()

	a := newAgent4Test()
	mockTableExecutor := newMockTableExecutor()
	a.tableM = newTableSpanManager(model.ChangeFeedID{}, mockTableExecutor)

	trans := transport.NewMockTrans()
	a.trans = trans

	// all possible inbound Messages can be received
	var inboundMessages []*schedulepb.Message
	inboundMessages = append(inboundMessages, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        a.ownerInfo.Version,
			OwnerRevision:  a.ownerInfo.Revision,
			ProcessorEpoch: a.Epoch,
		},
		MsgType: schedulepb.MsgDispatchTableRequest,
		From:    a.ownerInfo.ID,
		To:      a.CaptureID,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{
					Span: spanz.TableIDToComparableSpan(1),
				},
			},
		},
	})
	for _, isSecondary := range []bool{true, false} {
		inboundMessages = append(inboundMessages, &schedulepb.Message{
			Header: &schedulepb.Message_Header{
				Version:        a.ownerInfo.Version,
				OwnerRevision:  a.ownerInfo.Revision,
				ProcessorEpoch: a.Epoch,
			},
			MsgType: schedulepb.MsgDispatchTableRequest,
			From:    a.ownerInfo.ID,
			To:      a.CaptureID,
			DispatchTableRequest: &schedulepb.DispatchTableRequest{
				Request: &schedulepb.DispatchTableRequest_AddTable{
					AddTable: &schedulepb.AddTableRequest{
						Span:        spanz.TableIDToComparableSpan(1),
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
			ProcessorEpoch: a.Epoch,
		},
		MsgType: schedulepb.MsgHeartbeat,
		From:    "owner-1",
		Heartbeat: &schedulepb.Heartbeat{
			Spans: []tablepb.Span{{TableID: 1}},
		},
	})

	states := []tablepb.TableState{
		tablepb.TableStateAbsent,
		tablepb.TableStatePreparing,
		tablepb.TableStatePrepared,
		tablepb.TableStateReplicating,
		tablepb.TableStateStopping,
		tablepb.TableStateStopped,
	}
	ctx := context.Background()
	tableID := model.TableID(1)
	for _, state := range states {
		iterPermutation([]int{0, 1, 2, 3}, func(sequence []int) {
			t.Logf("test %v, %v", state, sequence)
			switch state {
			case tablepb.TableStatePreparing:
				mockTableExecutor.tables.ReplaceOrInsert(
					spanz.TableIDToComparableSpan(tableID), tablepb.TableStatePreparing)
			case tablepb.TableStatePrepared:
				mockTableExecutor.tables.ReplaceOrInsert(
					spanz.TableIDToComparableSpan(tableID), tablepb.TableStatePrepared)
			case tablepb.TableStateReplicating:
				mockTableExecutor.tables.ReplaceOrInsert(
					spanz.TableIDToComparableSpan(tableID), tablepb.TableStateReplicating)
			case tablepb.TableStateStopping:
				mockTableExecutor.tables.ReplaceOrInsert(
					spanz.TableIDToComparableSpan(tableID), tablepb.TableStateStopping)
			case tablepb.TableStateStopped:
				mockTableExecutor.tables.ReplaceOrInsert(
					spanz.TableIDToComparableSpan(tableID), tablepb.TableStateStopped)
			case tablepb.TableStateAbsent:
			default:
			}

			for _, idx := range sequence {
				message := inboundMessages[idx]
				if message.MsgType == schedulepb.MsgHeartbeat {
					trans.RecvBuffer = append(trans.RecvBuffer, message)
					_, err := a.Tick(ctx)
					require.NoError(t, err)
					require.Len(t, trans.SendBuffer, 1)
					heartbeatResponse := trans.SendBuffer[0].HeartbeatResponse
					trans.SendBuffer = trans.SendBuffer[:0]
					require.Equal(t, model.LivenessCaptureAlive, heartbeatResponse.Liveness)

					continue
				}

				switch message.DispatchTableRequest.Request.(type) {
				case *schedulepb.DispatchTableRequest_AddTable:
					for _, ok := range []bool{false, true} {
						mockTableExecutor.On("AddTableSpan", mock.Anything, mock.Anything,
							mock.Anything, mock.Anything).Return(ok, nil)
						for _, ok1 := range []bool{false, true} {
							mockTableExecutor.On("IsAddTableSpanFinished", mock.Anything,
								mock.Anything, mock.Anything).Return(ok1, nil)

							trans.RecvBuffer = append(trans.RecvBuffer, message)
							_, err := a.Tick(ctx)
							require.NoError(t, err)
							trans.SendBuffer = trans.SendBuffer[:0]

							mockTableExecutor.ExpectedCalls = mockTableExecutor.ExpectedCalls[:1]
						}
						mockTableExecutor.ExpectedCalls = nil
					}
				case *schedulepb.DispatchTableRequest_RemoveTable:
					for _, ok := range []bool{false, true} {
						mockTableExecutor.On("RemoveTableSpan", mock.Anything,
							mock.Anything).Return(ok)
						for _, ok1 := range []bool{false, true} {
							trans.RecvBuffer = append(trans.RecvBuffer, message)
							mockTableExecutor.On("IsRemoveTableSpanFinished",
								mock.Anything, mock.Anything).Return(0, ok1)
							_, err := a.Tick(ctx)
							require.NoError(t, err)
							if len(trans.SendBuffer) != 0 {
								require.Len(t, trans.SendBuffer, 1)
								response, yes := trans.SendBuffer[0].DispatchTableResponse.
									Response.(*schedulepb.DispatchTableResponse_RemoveTable)
								trans.SendBuffer = trans.SendBuffer[:0]
								require.True(t, yes)
								expected := tablepb.TableStateStopping
								if ok && ok1 {
									expected = tablepb.TableStateStopped
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
	tableM := newTableSpanManager(model.ChangeFeedID{}, mockTableExecutor)
	a := newAgent4Test()
	a.tableM = tableM

	heartbeat := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:       a.ownerInfo.Version,
			OwnerRevision: a.ownerInfo.Revision,
		},
		MsgType:   schedulepb.MsgHeartbeat,
		From:      a.ownerInfo.ID,
		Heartbeat: &schedulepb.Heartbeat{},
	}

	// handle the first heartbeat, from the known owner.
	response, _ := a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Len(t, response, 1)

	addTableRequest := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:       a.ownerInfo.Version,
			OwnerRevision: a.ownerInfo.Revision,
			// wrong epoch
			ProcessorEpoch: schedulepb.ProcessorEpoch{Epoch: "wrong-agent-epoch-1"},
		},
		MsgType: schedulepb.MsgDispatchTableRequest,
		From:    a.ownerInfo.ID,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        spanz.TableIDToComparableSpan(1),
					IsSecondary: true,
					Checkpoint:  tablepb.Checkpoint{},
				},
			},
		},
	}
	// wrong epoch, ignored
	responses, _ := a.handleMessage([]*schedulepb.Message{addTableRequest})
	require.False(t, tableM.tables.Has(spanz.TableIDToComparableSpan(1)))
	require.Len(t, responses, 0)

	// correct epoch, processing.
	addTableRequest.Header.ProcessorEpoch = a.Epoch
	_, _ = a.handleMessage([]*schedulepb.Message{addTableRequest})
	require.True(t, a.tableM.tables.Has(spanz.TableIDToComparableSpan(1)))

	heartbeat.Header.OwnerRevision.Revision = 2
	response, _ = a.handleMessage([]*schedulepb.Message{heartbeat})
	require.Len(t, response, 1)

	// this should never happen in real world
	unknownMessage := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        a.ownerInfo.Version,
			OwnerRevision:  schedulepb.OwnerRevision{Revision: 2},
			ProcessorEpoch: a.Epoch,
		},
		MsgType: schedulepb.MsgUnknown,
		From:    a.ownerInfo.ID,
	}

	response, _ = a.handleMessage([]*schedulepb.Message{unknownMessage})
	require.Len(t, response, 0)

	// staled message
	heartbeat.Header.OwnerRevision.Revision = 1
	response, _ = a.handleMessage([]*schedulepb.Message{heartbeat})
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
	trans := transport.NewMockTrans()
	mockTableExecutor := newMockTableExecutor()
	a.trans = trans
	a.tableM = newTableSpanManager(model.ChangeFeedID{}, mockTableExecutor)

	heartbeat := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:       a.ownerInfo.Version,
			OwnerRevision: a.ownerInfo.Revision,
			// first heartbeat from the owner, no processor epoch
			ProcessorEpoch: schedulepb.ProcessorEpoch{},
		},
		MsgType:   schedulepb.MsgHeartbeat,
		From:      a.ownerInfo.ID,
		Heartbeat: &schedulepb.Heartbeat{Spans: nil},
	}

	// receive first heartbeat from the owner
	trans.RecvBuffer = append(trans.RecvBuffer, heartbeat)

	ctx := context.Background()
	_, err := a.Tick(ctx)
	require.NoError(t, err)
	require.Len(t, trans.SendBuffer, 1)
	heartbeatResponse := trans.SendBuffer[0]
	trans.SendBuffer = trans.SendBuffer[:0]

	require.Equal(t, schedulepb.MsgHeartbeatResponse, heartbeatResponse.MsgType)
	require.Equal(t, a.ownerInfo.ID, heartbeatResponse.To)
	require.Equal(t, a.CaptureID, heartbeatResponse.From)

	addTableRequest := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        a.ownerInfo.Version,
			OwnerRevision:  a.ownerInfo.Revision,
			ProcessorEpoch: a.Epoch,
		},
		MsgType: schedulepb.MsgDispatchTableRequest,
		From:    a.ownerInfo.ID,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        spanz.TableIDToComparableSpan(1),
					IsSecondary: true,
					Checkpoint:  tablepb.Checkpoint{},
				},
			},
		},
	}

	removeTableRequest := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        a.ownerInfo.Version,
			OwnerRevision:  a.ownerInfo.Revision,
			ProcessorEpoch: a.Epoch,
		},
		MsgType: schedulepb.MsgDispatchTableRequest,
		From:    a.ownerInfo.ID,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{
					Span: tablepb.Span{TableID: 2},
				},
			},
		},
	}
	var messages []*schedulepb.Message
	messages = append(messages, addTableRequest)
	messages = append(messages, removeTableRequest)
	trans.RecvBuffer = append(trans.RecvBuffer, messages...)

	mockTableExecutor.On("AddTableSpan", mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	mockTableExecutor.On("IsAddTableSpanFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(false, nil)
	_, err = a.Tick(ctx)
	require.NoError(t, err)
	trans.SendBuffer = trans.SendBuffer[:0]

	trans.RecvBuffer = append(trans.RecvBuffer, addTableRequest)

	mockTableExecutor.ExpectedCalls = mockTableExecutor.ExpectedCalls[:1]
	mockTableExecutor.On("IsAddTableSpanFinished", mock.Anything,
		mock.Anything, mock.Anything).Return(true, nil)
	_, err = a.Tick(ctx)
	require.NoError(t, err)
	responses := trans.SendBuffer[:len(trans.SendBuffer)]
	trans.SendBuffer = trans.SendBuffer[:0]
	require.Len(t, responses, 1)
	require.Equal(t, schedulepb.MsgDispatchTableResponse, responses[0].MsgType)
	resp, ok := responses[0].DispatchTableResponse.
		Response.(*schedulepb.DispatchTableResponse_AddTable)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStatePrepared, resp.AddTable.Status.State)

	require.NoError(t, a.Close())
}

func TestAgentHandleLivenessUpdate(t *testing.T) {
	t.Parallel()

	// Test liveness via heartbeat.
	mockTableExecutor := newMockTableExecutor()
	tableM := newTableSpanManager(model.ChangeFeedID{}, mockTableExecutor)
	a := newAgent4Test()
	a.tableM = tableM
	require.Equal(t, model.LivenessCaptureAlive, a.liveness.Load())
	a.handleMessage([]*schedulepb.Message{{
		Header: &schedulepb.Message_Header{
			Version:        a.ownerInfo.Version,
			OwnerRevision:  a.ownerInfo.Revision,
			ProcessorEpoch: a.Epoch,
		},
		MsgType: schedulepb.MsgHeartbeat,
		From:    a.ownerInfo.ID,
		Heartbeat: &schedulepb.Heartbeat{
			IsStopping: true,
		},
	}})
	require.Equal(t, model.LivenessCaptureStopping, a.liveness.Load())

	a.handleLivenessUpdate(model.LivenessCaptureAlive)
	require.Equal(t, model.LivenessCaptureStopping, a.liveness.Load())
}

func TestAgentCommitAddTableDuringStopping(t *testing.T) {
	t.Parallel()

	a := newAgent4Test()
	mockTableExecutor := newMockTableExecutor()
	a.tableM = newTableSpanManager(model.ChangeFeedID{}, mockTableExecutor)
	trans := transport.NewMockTrans()
	a.trans = trans

	prepareTableMsg := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        "owner-version-1",
			OwnerRevision:  schedulepb.OwnerRevision{Revision: 1},
			ProcessorEpoch: schedulepb.ProcessorEpoch{Epoch: "agent-epoch-1"},
		},
		To:      "agent-1",
		From:    "owner-1",
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        spanz.TableIDToComparableSpan(1),
					IsSecondary: true,
				},
			},
		},
	}
	trans.RecvBuffer = []*schedulepb.Message{prepareTableMsg}

	// Prepare add table is still in-progress.
	mockTableExecutor.
		On("AddTableSpan", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(true, nil).Once()
	mockTableExecutor.
		On("IsAddTableSpanFinished", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(false, nil).Once()
	_, err := a.Tick(context.Background())
	require.Nil(t, err)
	require.Len(t, trans.SendBuffer, 0)

	mockTableExecutor.
		On("AddTableSpan", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(true, nil).Once()
	mockTableExecutor.
		On("IsAddTableSpanFinished", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(true, nil).Once()
	_, err = a.Tick(context.Background())
	require.Nil(t, err)
	require.Len(t, trans.SendBuffer, 1)
	require.Equal(t, trans.SendBuffer[0].MsgType, schedulepb.MsgDispatchTableResponse)

	// Commit add table request should not be rejected.
	commitTableMsg := &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:        "owner-version-1",
			OwnerRevision:  schedulepb.OwnerRevision{Revision: 1},
			ProcessorEpoch: schedulepb.ProcessorEpoch{Epoch: "agent-epoch-1"},
		},
		To:      "agent-1",
		From:    "owner-1",
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        spanz.TableIDToComparableSpan(1),
					IsSecondary: false,
				},
			},
		},
	}
	trans.RecvBuffer = []*schedulepb.Message{commitTableMsg}
	trans.SendBuffer = []*schedulepb.Message{}
	mockTableExecutor.
		On("AddTableSpan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(true, nil).Once()
	mockTableExecutor.
		On("IsAddTableSpanFinished", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(false, nil).Once()
	// Set liveness to stopping.
	a.liveness.Store(model.LivenessCaptureStopping)
	_, err = a.Tick(context.Background())
	require.Nil(t, err)
	require.Len(t, trans.SendBuffer, 1)

	trans.RecvBuffer = []*schedulepb.Message{}
	trans.SendBuffer = []*schedulepb.Message{}
	mockTableExecutor.
		On("IsAddTableSpanFinished", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(true, nil).Once()
	_, err = a.Tick(context.Background())
	require.Nil(t, err)
	require.Len(t, trans.SendBuffer, 1)
	require.Equal(t, schedulepb.MsgDispatchTableResponse, trans.SendBuffer[0].MsgType)
	addTableResp := trans.SendBuffer[0].DispatchTableResponse.GetAddTable()
	require.Equal(t, tablepb.TableStateReplicating, addTableResp.Status.State)
}

func TestAgentTransportCompat(t *testing.T) {
	t.Parallel()

	a := newAgent4Test()
	mockTableExecutor := newMockTableExecutor()
	a.tableM = newTableSpanManager(model.ChangeFeedID{}, mockTableExecutor)
	trans := transport.NewMockTrans()
	a.trans = trans
	a.compat = compat.New(&config.SchedulerConfig{
		ChangefeedSettings: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        1,
		},
	}, map[model.CaptureID]*model.CaptureInfo{})
	ctx := context.Background()

	// Disable span replication.
	a.handleOwnerInfo("a", a.ownerInfo.Revision.Revision+1, "4.0.0")
	require.False(t, a.compat.CheckSpanReplicationEnabled())

	// Test compat.BeforeTransportSend.
	a.sendMsgs(
		ctx, []*schedulepb.Message{{
			Header: &schedulepb.Message_Header{
				Version:        a.Version,
				ProcessorEpoch: a.Epoch,
			},
			From: a.CaptureID, To: "a", MsgType: schedulepb.MsgDispatchTableResponse,
			DispatchTableResponse: &schedulepb.DispatchTableResponse{
				Response: &schedulepb.DispatchTableResponse_AddTable{
					AddTable: &schedulepb.AddTableResponse{
						Status: &tablepb.TableStatus{
							Span: spanz.TableIDToComparableSpan(1),
						},
					},
				},
			},
		}})
	require.EqualValues(t, []*schedulepb.Message{{
		Header: &schedulepb.Message_Header{
			Version:        a.Version,
			ProcessorEpoch: a.Epoch,
			OwnerRevision:  a.ownerInfo.Revision,
		},
		From: a.CaptureID, To: "a", MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_AddTable{
				AddTable: &schedulepb.AddTableResponse{
					Status: &tablepb.TableStatus{
						TableID: 1,
						Span:    spanz.TableIDToComparableSpan(1),
					},
				},
			},
		},
	}}, trans.SendBuffer)
	// Test compat.AfterTransportReceive.
	trans.RecvBuffer = append(trans.RecvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:       a.Version,
			OwnerRevision: a.ownerInfo.Revision,
		},
		From: "a", To: a.CaptureID, MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					TableID: 1,
				},
			},
		},
	})
	msgs, err := a.recvMsgs(ctx)
	require.NoError(t, err)
	require.EqualValues(t, []*schedulepb.Message{{
		Header: &schedulepb.Message_Header{
			Version:       a.Version,
			OwnerRevision: a.ownerInfo.Revision,
		},
		From: "a", To: a.CaptureID, MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					TableID: 1,
					Span:    spanz.TableIDToComparableSpan(1),
				},
			},
		},
	}}, msgs)
}

func TestAgentDropMsgIfChangefeedEpochMismatch(t *testing.T) {
	t.Parallel()

	a := newAgent4Test()
	mockTableExecutor := newMockTableExecutor()
	a.tableM = newTableSpanManager(model.ChangeFeedID{}, mockTableExecutor)
	trans := transport.NewMockTrans()
	a.trans = trans
	a.compat = compat.New(&config.SchedulerConfig{
		ChangefeedSettings: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        1,
		},
	}, map[model.CaptureID]*model.CaptureInfo{})
	a.changefeedEpoch = 1
	ctx := context.Background()

	// Enable changefeed epoch.
	a.handleOwnerInfo(
		"a", a.ownerInfo.Revision.Revision+1, compat.ChangefeedEpochMinVersion.String())

	trans.RecvBuffer = append(trans.RecvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:         a.Version,
			OwnerRevision:   a.ownerInfo.Revision,
			ChangefeedEpoch: schedulepb.ChangefeedEpoch{Epoch: 1},
		},
		From: "a", To: a.CaptureID, MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					TableID: 1,
				},
			},
		},
	})
	trans.RecvBuffer = append(trans.RecvBuffer,
		&schedulepb.Message{
			Header: &schedulepb.Message_Header{
				Version:         a.Version,
				OwnerRevision:   a.ownerInfo.Revision,
				ChangefeedEpoch: schedulepb.ChangefeedEpoch{Epoch: 2}, // mismatch
			},
			From: "a", To: a.CaptureID, MsgType: schedulepb.MsgDispatchTableRequest,
			DispatchTableRequest: &schedulepb.DispatchTableRequest{
				Request: &schedulepb.DispatchTableRequest_AddTable{
					AddTable: &schedulepb.AddTableRequest{
						TableID: 1,
					},
				},
			},
		})
	msgs, err := a.recvMsgs(ctx)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, "a", msgs[0].From)

	// Disable changefeed epoch
	unsupported := *compat.ChangefeedEpochMinVersion
	unsupported.Major--
	a.handleOwnerInfo(
		"a", a.ownerInfo.Revision.Revision+1, unsupported.String())

	trans.RecvBuffer = trans.RecvBuffer[:0]
	trans.RecvBuffer = append(trans.RecvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			Version:         unsupported.String(),
			OwnerRevision:   a.ownerInfo.Revision,
			ChangefeedEpoch: schedulepb.ChangefeedEpoch{Epoch: 2}, // mistmatch
		},
		From: "a", To: a.CaptureID, MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					TableID: 1,
				},
			},
		},
	})
	msgs, err = a.recvMsgs(ctx)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, "a", msgs[0].From)
}

// MockTableExecutor is a mock implementation of TableExecutor.
type MockTableExecutor struct {
	mock.Mock

	// it's preferred to use `pipeline.MockPipeline` here to make the test more vivid.
	tables *spanz.BtreeMap[tablepb.TableState]
}

var _ internal.TableExecutor = (*MockTableExecutor)(nil)

// newMockTableExecutor creates a new mock table executor.
func newMockTableExecutor() *MockTableExecutor {
	return &MockTableExecutor{
		tables: spanz.NewBtreeMap[tablepb.TableState](),
	}
}

// AddTableSpan adds a table span to the executor.
func (e *MockTableExecutor) AddTableSpan(
	ctx context.Context, span tablepb.Span, checkpoint tablepb.Checkpoint, isPrepare bool,
) (bool, error) {
	startTs := checkpoint.CheckpointTs
	log.Info("AddTableSpan",
		zap.String("span", span.String()),
		zap.Any("startTs", startTs),
		zap.Bool("isPrepare", isPrepare))

	state, ok := e.tables.Get(span)
	if ok {
		switch state {
		case tablepb.TableStatePreparing:
			return true, nil
		case tablepb.TableStatePrepared:
			if !isPrepare {
				e.tables.ReplaceOrInsert(span, tablepb.TableStateReplicating)
			}
			return true, nil
		case tablepb.TableStateReplicating:
			return true, nil
		case tablepb.TableStateStopped:
			e.tables.Delete(span)
		}
	}
	args := e.Called(ctx, span, startTs, isPrepare)
	if args.Bool(0) {
		e.tables.ReplaceOrInsert(span, tablepb.TableStatePreparing)
	}
	return args.Bool(0), args.Error(1)
}

// IsAddTableSpanFinished determines if the table span has been added.
func (e *MockTableExecutor) IsAddTableSpanFinished(span tablepb.Span, isPrepare bool) bool {
	_, ok := e.tables.Get(span)
	if !ok {
		log.Panic("table which was added is not found",
			zap.String("span", span.String()),
			zap.Bool("isPrepare", isPrepare))
	}

	args := e.Called(span, isPrepare)
	if args.Bool(0) {
		e.tables.ReplaceOrInsert(span, tablepb.TableStatePrepared)
		if !isPrepare {
			e.tables.ReplaceOrInsert(span, tablepb.TableStateReplicating)
		}
		return true
	}

	e.tables.ReplaceOrInsert(span, tablepb.TableStatePreparing)
	if !isPrepare {
		e.tables.ReplaceOrInsert(span, tablepb.TableStatePrepared)
	}

	return false
}

// RemoveTableSpan removes a table span from the executor.
func (e *MockTableExecutor) RemoveTableSpan(span tablepb.Span) bool {
	state, ok := e.tables.Get(span)
	if !ok {
		log.Warn("table to be remove is not found", zap.String("span", span.String()))
		return true
	}
	switch state {
	case tablepb.TableStateStopping, tablepb.TableStateStopped:
		return true
	case tablepb.TableStatePreparing, tablepb.TableStatePrepared, tablepb.TableStateReplicating:
	default:
	}
	// the current `processor implementation, does not consider table's state
	log.Info("RemoveTableSpan", zap.String("span", span.String()), zap.Any("state", state))

	args := e.Called(span)
	if args.Bool(0) {
		e.tables.ReplaceOrInsert(span, tablepb.TableStateStopped)
	}
	return args.Bool(0)
}

// IsRemoveTableSpanFinished determines if the table span has been removed.
func (e *MockTableExecutor) IsRemoveTableSpanFinished(span tablepb.Span) (model.Ts, bool) {
	state, ok := e.tables.Get(span)
	if !ok {
		// the real `table executor` processor, would panic in such case.
		log.Warn("table to be removed is not found",
			zap.String("span", span.String()))
		return 0, true
	}
	args := e.Called(span)
	if args.Bool(1) {
		log.Info("remove table finished, remove it from the executor",
			zap.String("span", span.String()), zap.Any("state", state))
		e.tables.Delete(span)
	} else {
		// revert the state back to old state, assume it's `replicating`,
		// but `preparing` / `prepared` can also be removed.
		e.tables.ReplaceOrInsert(span, tablepb.TableStateReplicating)
	}

	return model.Ts(args.Int(0)), args.Bool(1)
}

// GetTableSpanCount returns all tables that are currently being adding, running, or removing.
func (e *MockTableExecutor) GetTableSpanCount() int {
	var result int
	e.tables.Ascend(func(span tablepb.Span, value tablepb.TableState) bool {
		result++
		return true
	})
	return result
}

// GetTableSpanStatus implements TableExecutor interface
func (e *MockTableExecutor) GetTableSpanStatus(
	span tablepb.Span, collectStat bool,
) tablepb.TableStatus {
	state, ok := e.tables.Get(span)
	if !ok {
		state = tablepb.TableStateAbsent
	}
	return tablepb.TableStatus{
		Span:  span,
		State: state,
	}
}
