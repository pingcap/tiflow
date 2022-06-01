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
	"fmt"
	"math"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/pingcap/tiflow/pkg/leakutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m)
}

type mockTrans struct {
	sendBuffer []*schedulepb.Message
	recvBuffer []*schedulepb.Message

	keepRecvBuffer bool
}

func newMockTrans() *mockTrans {
	return &mockTrans{
		sendBuffer: make([]*schedulepb.Message, 0),
		recvBuffer: make([]*schedulepb.Message, 0),
	}
}

func (m *mockTrans) Close() error {
	return nil
}

func (m *mockTrans) Send(ctx context.Context, msgs []*schedulepb.Message) error {
	m.sendBuffer = append(m.sendBuffer, msgs...)
	return nil
}

func (m *mockTrans) Recv(ctx context.Context) ([]*schedulepb.Message, error) {
	if m.keepRecvBuffer {
		return m.recvBuffer, nil
	}
	messages := m.recvBuffer[:len(m.recvBuffer)]
	m.recvBuffer = make([]*schedulepb.Message, 0)
	return messages, nil
}

func TestCoordinatorSendMsgs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	trans := newMockTrans()
	coord := coordinator{
		version:   "6.2.0",
		revision:  schedulepb.OwnerRevision{Revision: 3},
		captureID: "0",
		trans:     trans,
	}
	coord.sendMsgs(
		ctx, []*schedulepb.Message{{To: "1", MsgType: schedulepb.MsgDispatchTableRequest}})

	require.EqualValues(t, []*schedulepb.Message{{
		Header: &schedulepb.Message_Header{
			Version:       coord.version,
			OwnerRevision: coord.revision,
		},
		From: "0", To: "1", MsgType: schedulepb.MsgDispatchTableRequest,
	}}, trans.sendBuffer)
}

func TestCoordinatorRecvMsgs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	trans := &mockTrans{}
	coord := coordinator{
		version:   "6.2.0",
		revision:  schedulepb.OwnerRevision{Revision: 3},
		captureID: "0",
		trans:     trans,
	}

	trans.recvBuffer = append(trans.recvBuffer,
		&schedulepb.Message{
			Header: &schedulepb.Message_Header{
				OwnerRevision: coord.revision,
			},
			From: "1", To: coord.captureID, MsgType: schedulepb.MsgDispatchTableResponse,
		})
	trans.recvBuffer = append(trans.recvBuffer,
		&schedulepb.Message{
			Header: &schedulepb.Message_Header{
				OwnerRevision: schedulepb.OwnerRevision{Revision: 4},
			},
			From: "2", To: coord.captureID, MsgType: schedulepb.MsgDispatchTableResponse,
		})
	trans.recvBuffer = append(trans.recvBuffer,
		&schedulepb.Message{
			Header: &schedulepb.Message_Header{
				OwnerRevision: coord.revision,
			},
			From: "3", To: "lost", MsgType: schedulepb.MsgDispatchTableResponse,
		})

	msgs, err := coord.recvMsgs(ctx)
	require.NoError(t, err)
	require.EqualValues(t, []*schedulepb.Message{{
		Header: &schedulepb.Message_Header{
			OwnerRevision: coord.revision,
		},
		From: "1", To: "0", MsgType: schedulepb.MsgDispatchTableResponse,
	}}, msgs)
}

func TestCoordinatorInit(t *testing.T) {
}

func TestCoordinatorHeartbeat(t *testing.T) {
}

func TestCoordinatorHeartbeatResponse(t *testing.T) {
}

func TestCoordinatorAddCapture(t *testing.T) {
}

func TestCoordinatorRemoveCapture(t *testing.T) {
}

func benchmarkCoordinator(
	b *testing.B,
	factory func(total int) (
		name string,
		coord *coordinator,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	),
) {
	log.SetLevel(zapcore.DPanicLevel)
	ctx := context.Background()
	size := 16384
	for total := 1; total <= size; total *= 2 {
		name, coord, currentTables, captures := factory(total)
		b.ResetTimer()
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				coord.poll(ctx, 0, currentTables, captures)
			}
		})
		b.StopTimer()
	}
}

func BenchmarkCoordinatorInit(b *testing.B) {
	benchmarkCoordinator(b, func(total int) (
		name string,
		coord *coordinator,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*model.CaptureInfo{}
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &model.CaptureInfo{}
		}
		currentTables = make([]model.TableID, 0, total)
		for i := 0; i < total; i++ {
			currentTables = append(currentTables, int64(10000+i))
		}
		coord = &coordinator{
			trans:        &mockTrans{},
			scheduler:    []scheduler{newBalancer()},
			replicationM: newReplicationManager(10),
			// Disable heartbeat.
			captureM: newCaptureManager(schedulepb.OwnerRevision{}, math.MaxInt),
		}
		name = fmt.Sprintf("InitTable %d", total)
		return name, coord, currentTables, captures
	})
}

func BenchmarkCoordinatorHeartbeat(b *testing.B) {
	benchmarkCoordinator(b, func(total int) (
		name string,
		coord *coordinator,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*model.CaptureInfo{}
		// Always heartbeat.
		captureM := newCaptureManager(schedulepb.OwnerRevision{}, 0)
		captureM.initialized = true
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &model.CaptureInfo{}
			captureM.Captures[fmt.Sprint(i)] = &CaptureStatus{State: CaptureStateInitialized}
		}
		currentTables = make([]model.TableID, 0, total)
		for i := 0; i < total; i++ {
			currentTables = append(currentTables, int64(10000+i))
		}
		coord = &coordinator{
			trans:        &mockTrans{},
			scheduler:    []scheduler{},
			replicationM: newReplicationManager(10),
			captureM:     captureM,
		}
		name = fmt.Sprintf("Heartbeat %d", total)
		return name, coord, currentTables, captures
	})
}

func BenchmarkCoordinatorHeartbeatResponse(b *testing.B) {
	benchmarkCoordinator(b, func(total int) (
		name string,
		coord *coordinator,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*model.CaptureInfo{}
		// Disable heartbeat.
		captureM := newCaptureManager(schedulepb.OwnerRevision{}, math.MaxInt)
		captureM.initialized = true
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &model.CaptureInfo{}
			captureM.Captures[fmt.Sprint(i)] = &CaptureStatus{State: CaptureStateInitialized}
		}
		replicationM := newReplicationManager(10)
		currentTables = make([]model.TableID, 0, total)
		heartbeatResp := make(map[model.CaptureID]*schedulepb.Message)
		for i := 0; i < total; i++ {
			tableID := int64(10000 + i)
			currentTables = append(currentTables, tableID)
			captureID := fmt.Sprint(i % captureCount)
			rep, err := newReplicationSet(tableID, 0, map[string]*schedulepb.TableStatus{
				captureID: {
					TableID: tableID,
					State:   schedulepb.TableStateReplicating,
				},
			})
			if err != nil {
				b.Fatal(err)
			}
			replicationM.tables[tableID] = rep
			_, ok := heartbeatResp[captureID]
			if !ok {
				heartbeatResp[captureID] = &schedulepb.Message{
					Header:            &schedulepb.Message_Header{},
					From:              captureID,
					MsgType:           schedulepb.MsgHeartbeatResponse,
					HeartbeatResponse: &schedulepb.HeartbeatResponse{},
				}
			}
			heartbeatResp[captureID].HeartbeatResponse.Tables = append(
				heartbeatResp[captureID].HeartbeatResponse.Tables,
				schedulepb.TableStatus{
					TableID: tableID,
					State:   schedulepb.TableStateReplicating,
				})
		}
		recvMsgs := make([]*schedulepb.Message, 0, len(heartbeatResp))
		for _, resp := range heartbeatResp {
			recvMsgs = append(recvMsgs, resp)
		}
		trans := &mockTrans{
			recvBuffer:     recvMsgs,
			keepRecvBuffer: true,
		}
		coord = &coordinator{
			trans:        trans,
			scheduler:    []scheduler{},
			replicationM: replicationM,
			captureM:     captureM,
		}
		name = fmt.Sprintf("HeartbeatResponse %d", total)
		return name, coord, currentTables, captures
	})
}
