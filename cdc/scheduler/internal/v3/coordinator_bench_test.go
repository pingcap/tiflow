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

package v3

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/transport"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap/zapcore"
)

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
	size := 131072 // 2^17
	for total := 1; total <= size; total *= 2 {
		name, coord, currentTables, captures := factory(total)
		b.ResetTimer()
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				coord.poll(ctx, 0, currentTables, captures, schedulepb.NewBarrierWithMinTs(0))
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
		// Disable heartbeat.
		cfg := config.NewDefaultSchedulerConfig()
		cfg.HeartbeatTick = math.MaxInt
		coord = &coordinator{
			trans:        transport.NewMockTrans(),
			replicationM: replication.NewReplicationManager(10, model.ChangeFeedID{}),
			captureM: member.NewCaptureManager(
				"", model.ChangeFeedID{}, schedulepb.OwnerRevision{}, cfg),
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
		cfg := config.NewDefaultSchedulerConfig()
		cfg.HeartbeatTick = 1
		captureM := member.NewCaptureManager(
			"", model.ChangeFeedID{}, schedulepb.OwnerRevision{}, cfg)
		captureM.SetInitializedForTests(true)
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &model.CaptureInfo{}
			captureM.Captures[fmt.Sprint(i)] = &member.CaptureStatus{
				State: member.CaptureStateInitialized,
			}
		}
		currentTables = make([]model.TableID, 0, total)
		for i := 0; i < total; i++ {
			currentTables = append(currentTables, int64(10000+i))
		}
		coord = &coordinator{
			trans:        transport.NewMockTrans(),
			replicationM: replication.NewReplicationManager(10, model.ChangeFeedID{}),
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
		cfg := config.NewDefaultSchedulerConfig()
		cfg.HeartbeatTick = math.MaxInt
		captureM := member.NewCaptureManager(
			"", model.ChangeFeedID{}, schedulepb.OwnerRevision{}, cfg)
		captureM.SetInitializedForTests(true)
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &model.CaptureInfo{}
			captureM.Captures[fmt.Sprint(i)] = &member.CaptureStatus{
				State: member.CaptureStateInitialized,
			}
		}
		replicationM := replication.NewReplicationManager(10, model.ChangeFeedID{})
		currentTables = make([]model.TableID, 0, total)
		heartbeatResp := make(map[model.CaptureID]*schedulepb.Message)
		for i := 0; i < total; i++ {
			tableID := int64(10000 + i)
			currentTables = append(currentTables, tableID)
			captureID := fmt.Sprint(i % captureCount)
			rep, err := replication.NewReplicationSet(
				tableID, 0, map[string]*tablepb.TableStatus{
					captureID: {
						TableID: tableID,
						State:   tablepb.TableStateReplicating,
					},
				}, model.ChangeFeedID{})
			if err != nil {
				b.Fatal(err)
			}
			replicationM.SetReplicationSetForTests(rep)
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
				tablepb.TableStatus{
					TableID: tableID,
					State:   tablepb.TableStateReplicating,
				})
		}
		recvMsgs := make([]*schedulepb.Message, 0, len(heartbeatResp))
		for _, resp := range heartbeatResp {
			recvMsgs = append(recvMsgs, resp)
		}
		trans := &transport.MockTrans{
			RecvBuffer:     recvMsgs,
			KeepRecvBuffer: true,
		}
		coord = &coordinator{
			trans:        trans,
			replicationM: replicationM,
			captureM:     captureM,
		}
		name = fmt.Sprintf("HeartbeatResponse %d", total)
		return name, coord, currentTables, captures
	})
}
