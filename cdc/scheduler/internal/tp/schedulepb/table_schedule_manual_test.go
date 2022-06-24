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
	fmt "fmt"
	testing "testing"

	"github.com/gogo/protobuf/proto"
)

func benchmarkMessageCheckpoints(b *testing.B, bench func(b *testing.B, m *Message)) {
	size := 16384
	for total := 1; total <= size; total *= 2 {
		msg := Message{
			Checkpoints: map[int64]Checkpoint{},
		}
		for i := 0; i < total; i++ {
			msg.Checkpoints[int64(10000+i)] = Checkpoint{
				CheckpointTs: 433331421532337260,
				ResolvedTs:   433331421532337261,
			}
		}
		b.ResetTimer()
		bench(b, &msg)
		b.StopTimer()
	}

}

func BenchmarkMessageCheckpointsProtoMarshal(b *testing.B) {
	benchmarkMessageCheckpoints(b, func(b *testing.B, msg *Message) {
		total := len(msg.Checkpoints)
		b.Run(fmt.Sprintf("%d checkpoint(s) marshal", total), func(b *testing.B) {
			totalLen := 0
			for i := 0; i < b.N; i++ {
				dAtA, err := proto.Marshal(msg)
				if err != nil {
					panic(err)
				}
				totalLen += len(dAtA)
			}
			b.SetBytes(int64(totalLen / b.N))
		})
	})
}

func BenchmarkMessageCheckpointsProtoUnmarshal(b *testing.B) {
	benchmarkMessageCheckpoints(b, func(b *testing.B, msg *Message) {
		total := len(msg.Checkpoints)
		b.Run(fmt.Sprintf("%d checkpoint(s) unmarshal", total), func(b *testing.B) {
			dAtA, err := proto.Marshal(msg)
			if err != nil {
				panic(err)
			}
			dst := Message{}
			totalLen := 0

			for i := 0; i < b.N; i++ {
				err := proto.Unmarshal(dAtA, &dst)
				if err != nil {
					panic(err)
				}
				totalLen += len(dAtA)
			}
			b.SetBytes(int64(totalLen / b.N))
		})
	})
}

func BenchmarkMessageCheckpointsProtoSize(b *testing.B) {
	benchmarkMessageCheckpoints(b, func(b *testing.B, msg *Message) {
		total := len(msg.Checkpoints)
		b.Run(fmt.Sprintf("%d checkpoint(s) size", total), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				msg.Size()
			}
		})
	})
}
