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
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/spanz"
)

func benchmarkHeartbeatResponse(b *testing.B, bench func(b *testing.B, a *agent)) {
	upperBound := 16384
	for size := 1; size <= upperBound; size *= 2 {
		tableExec := newMockTableExecutor()
		liveness := model.LivenessCaptureAlive
		a := &agent{
			tableM:   newTableSpanManager(model.ChangeFeedID{}, tableExec),
			liveness: &liveness,
		}

		for j := 0; j < size; j++ {
			span := spanz.TableIDToComparableSpan(model.TableID(10000 + j))
			_ = a.tableM.addTableSpan(span)
		}

		b.ResetTimer()
		bench(b, a)
		b.StopTimer()
	}
}

func BenchmarkRefreshAllTables(b *testing.B) {
	benchmarkHeartbeatResponse(b, func(b *testing.B, a *agent) {
		total := a.tableM.tables.Len()
		b.Run(fmt.Sprintf("%d tables", total), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				a.handleMessageHeartbeat(&schedulepb.Heartbeat{})
			}
		})
	})
}
