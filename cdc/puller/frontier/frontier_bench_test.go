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
	"testing"

	"github.com/pingcap/ticdc/pkg/regionspan"
)

func toCMPBytes(i int) []byte {
	s := fmt.Sprintf("%09d", i)
	return []byte(s)
}

func BenchmarkSpanFrontier(b *testing.B) {
	tests := []struct {
		name string
		n    int
	}{
		{name: "5k", n: 5000},
		{name: "10k", n: 10_000},
		{name: "50k", n: 50_000},
		{name: "100k", n: 100_000},
	}

	for _, test := range tests {
		n := test.n

		b.Run(test.name, func(b *testing.B) {
			spans := make([]regionspan.ComparableSpan, 0, n)
			for i := 0; i < n; i++ {
				span := regionspan.ComparableSpan{
					Start: toCMPBytes(i),
					End:   toCMPBytes(i + 1),
				}
				spans = append(spans, span)
			}

			f := NewFrontier(0, spans...)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				f.Forward(spans[i%n], uint64(i))
			}
		})
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
				spans := make([]regionspan.ComparableSpan, 0, n)
				forward := make([]regionspan.ComparableSpan, 0, n)
				for i := 0; i < n; i++ {
					spans = append(spans, regionspan.ComparableSpan{
						Start: toCMPBytes(i),
						End:   toCMPBytes(i + 1),
					})
					forward = append(forward, regionspan.ComparableSpan{
						Start: toCMPBytes(i),
						End:   toCMPBytes(i + step),
					})
				}

				f := NewFrontier(0, spans...)

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					f.Forward(forward[i%n], uint64(i))
				}
			})
		}
	}
}
