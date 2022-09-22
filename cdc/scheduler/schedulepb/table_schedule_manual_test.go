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

package schedulepb

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
)

func benchmarkMessageHeartbeatResponse(
	b *testing.B, bench func(b *testing.B, m *Message, total int),
) {
	size := 16384
	for total := 1; total <= size; total *= 2 {
		msg := Message{
			MsgType: MsgHeartbeatResponse,
			HeartbeatResponse: &HeartbeatResponse{
				Tables:   make([]tablepb.TableStatus, 0, total),
				Liveness: model.LivenessCaptureAlive,
			},
		}

		for i := 0; i < total; i++ {
			msg.HeartbeatResponse.Tables = append(msg.HeartbeatResponse.Tables,
				tablepb.TableStatus{
					TableID: model.TableID(10000 + i),
					State:   tablepb.TableStateReplicating,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 433331421532337260,
						ResolvedTs:   433331421532337261,
					},
				})
		}

		b.ResetTimer()
		bench(b, &msg, total)
		b.StopTimer()
	}
}

func BenchmarkMessageHeartbeatResponseProtoMarshal(b *testing.B) {
	benchmarkMessageHeartbeatResponse(b, func(b *testing.B, m *Message, total int) {
		b.Run(fmt.Sprintf("%d checkpoints(s) marshal", total), func(b *testing.B) {
			totalLen := 0
			for i := 0; i < b.N; i++ {
				bytes, err := proto.Marshal(m)
				if err != nil {
					panic(err)
				}
				totalLen += len(bytes)
			}
			b.SetBytes(int64(totalLen / b.N))
		})
	})
}

func BenchmarkMessageHeartbeatResponseProtoUnmarshal(b *testing.B) {
	benchmarkMessageHeartbeatResponse(b, func(b *testing.B, m *Message, total int) {
		b.Run(fmt.Sprintf("%d checkpoints(s) marshal", total), func(b *testing.B) {
			bytes, err := proto.Marshal(m)
			if err != nil {
				panic(err)
			}

			dst := Message{}
			totalLen := 0
			for i := 0; i < b.N; i++ {
				if err := proto.Unmarshal(bytes, &dst); err != nil {
					panic(err)
				}
				totalLen += len(bytes)
			}
			b.SetBytes(int64(totalLen / b.N))
		})
	})
}

func BenchmarkMessageHeartbeatResponseProtoSize(b *testing.B) {
	benchmarkMessageHeartbeatResponse(b, func(b *testing.B, m *Message, total int) {
		b.Run(fmt.Sprintf("%d checkpoint(s) size", total), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				m.Size()
			}
		})
	})
}
