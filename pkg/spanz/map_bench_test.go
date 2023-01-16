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

package spanz

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/processor/tablepb"
)

func BenchmarkMap(b *testing.B) {
	bm := NewBtreeMap[int]()
	hm := NewHashMap[int]()
	sm := SyncMap{}

	spans := [100]tablepb.Span{}
	for i := 0; i < len(spans); i++ {
		spans[i] = TableIDToComparableSpan(int64(i))
	}
	for i, span := range spans {
		bm.ReplaceOrInsert(span, i)
		hm.ReplaceOrInsert(span, i)
		sm.Store(span, i)
	}

	bench := func(name string, benchBm, benchHm, benchSm func(i int)) {
		b.Run(name, func(b *testing.B) {
			if benchBm != nil {
				b.ResetTimer()
				b.Run("BtreeMap", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						benchBm(i)
					}
				})
				b.StopTimer()
			}
			if benchHm != nil {
				b.ResetTimer()
				b.Run("HashMap", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						benchHm(i)
					}
				})
				b.StopTimer()
			}
			if benchSm != nil {
				b.ResetTimer()
				b.Run("SyncMap", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						benchSm(i)
					}
				})
				b.StopTimer()
			}
		})
	}

	bench("Get", func(i int) {
		bm.Get(spans[i%100])
	}, func(i int) {
		hm.Get(spans[i%100])
	}, func(i int) {
		sm.Load(spans[i%100])
	})

	bench("Store", func(i int) {
		bm.ReplaceOrInsert(spans[i%100], i)
	}, func(i int) {
		hm.ReplaceOrInsert(spans[i%100], i)
	}, func(i int) {
		sm.Store(spans[i%100], i)
	})

	bench("Delete+Store", func(i int) {
		bm.Delete(spans[i%100])
		bm.ReplaceOrInsert(spans[i%100], i)
	}, func(i int) {
		hm.Delete(spans[i%100])
		hm.ReplaceOrInsert(spans[i%100], i)
	}, func(i int) {
		sm.Delete(spans[i%100])
		sm.Store(spans[i%100], i)
	})

	bench("Range", func(i int) {
		bm.Ascend(func(span tablepb.Span, value int) bool {
			return span.TableID > -1
		})
	}, func(i int) {
		hm.Range(func(span tablepb.Span, value int) bool {
			return span.TableID > -1
		})
	}, func(i int) {
		sm.Range(func(span tablepb.Span, value any) bool {
			return span.TableID > -1
		})
	})

	start, end := spans[0], TableIDToComparableSpan(int64(len(spans)))
	bench("AscendRange", func(i int) {
		bm.AscendRange(start, end, func(span tablepb.Span, value int) bool {
			return span.TableID > -1
		})
	}, nil, nil)
}
