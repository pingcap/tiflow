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
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
)

func benchmarkCollectTableStatus(b *testing.B, bench func(b *testing.B, a *agent)) {
	upperBound := 16384
	for size := 1; size <= upperBound; size *= 2 {
		tableExec := NewMockTableExecutor()
		a := &agent{
			tableExec: tableExec,
		}
		for j := 0; j < size; j++ {
			tableExec.tables[model.TableID(j)] = pipeline.TableStatusReplicating
		}

		b.ResetTimer()
		bench(b, a)
		b.StopTimer()
	}
}

func BenchmarkCollectTableStatus(b *testing.B) {
	benchmarkCollectTableStatus(b, func(b *testing.B, a *agent) {
		total := len(a.tableExec.GetAllCurrentTables())
		b.Run(fmt.Sprintf("%d tables", total), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				a.collectTableStatus()
			}
		})
	})
}
