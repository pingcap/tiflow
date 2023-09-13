// Copyright 2020 PingCAP, Inc.
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

package frontier

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/spanz"
)

func toCMPBytes(i int) []byte {
	s := fmt.Sprintf("t_XXXXXXXX_r_%09d", i)
	return []byte(s)
}

func BenchmarkSpanForwardAndFrontier_random_900k_10(b *testing.B) {
	benchmarkSpanForwardAndFrontier(b, "random", 900_000, 10)
}

func BenchmarkSpanForwardAndFrontier_ordered_900k_10(b *testing.B) {
	benchmarkSpanForwardAndFrontier(b, "ordered", 900_000, 10)
}

func BenchmarkSpanForwardAndFrontier_random_400k_10(b *testing.B) {
	benchmarkSpanForwardAndFrontier(b, "random", 400_000, 10)
}

func BenchmarkSpanForwardAndFrontier_ordered_400k_10(b *testing.B) {
	benchmarkSpanForwardAndFrontier(b, "ordered", 400_000, 10)
}

func benchmarkSpanForwardAndFrontier(b *testing.B, order string, regions, rounds int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		spans := make([]tablepb.Span, 0, regions)
		for i := 0; i < regions; i++ {
			span := tablepb.Span{StartKey: toCMPBytes(i), EndKey: toCMPBytes(i + 1)}
			spans = append(spans, span)
		}
		totalSpan := tablepb.Span{StartKey: spans[0].StartKey, EndKey: spans[len(spans)-1].EndKey}
		f := NewFrontier(0, totalSpan)

		offsets := make([]uint64, 0, regions*rounds)
		if order == "random" {
			r := rand.New(rand.NewSource(time.Now().Unix()))
			for i := 0; i < regions*rounds; i++ {
				offsets = append(offsets, r.Uint64()%uint64(regions))
			}
		} else {
			for i := 0; i < regions*rounds; i++ {
				offsets = append(offsets, uint64(i)%uint64(regions))
			}
		}

		b.StartTimer()
		start := time.Now()
		for i := 0; i < regions*rounds; i++ {
			offset := offsets[i]
			span := spans[offset]
			if spanz.IsSubSpan(span, totalSpan) {
				f.Forward(offset, span, uint64(1))
				if i%regions == 0 {
					f.Frontier()
				}
			}
		}
		b.Logf("finishes a round, time in ms: %d", time.Since(start).Milliseconds())
	}
}

func BenchmarkSpanFrontierOverlap(b *testing.B) {
	tests := []struct {
		name string
		n    int
	}{
		{name: "5k", n: 5000},
		{name: "10k", n: 10_000},
		{name: "50k", n: 50_000},
		{name: "100k", n: 100_000},
	}

	steps := []int{5, 10, 100, 500}

	for _, test := range tests {
		n := test.n

		for _, step := range steps {
			b.Run(fmt.Sprintf("%s_%d", test.name, step), func(b *testing.B) {
				spans := make([]tablepb.Span, 0, n)
				forward := make([]tablepb.Span, 0, n)
				for i := 0; i < n; i++ {
					spans = append(spans, tablepb.Span{
						StartKey: toCMPBytes(i),
						EndKey:   toCMPBytes(i + 1),
					})
					forward = append(forward, tablepb.Span{
						StartKey: toCMPBytes(i),
						EndKey:   toCMPBytes(i + step),
					})
				}

				f := NewFrontier(0, spans...)

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					f.Forward(0, forward[i%n], uint64(i))
				}
			})
		}
	}
}
