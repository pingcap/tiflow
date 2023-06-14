// Copyright 2023 PingCAP, Inc.
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

import "github.com/pingcap/tiflow/cdc/model"

// BarrierWithMinTs is a barrier with the minimum commitTs of all barriers.
type BarrierWithMinTs struct {
	*Barrier
	// MinTableBarrierTs is the minimum commitTs of all DDL events and is only
	// used to check whether there is a pending DDL job at the checkpointTs when
	// initializing the changefeed.
	MinTableBarrierTs model.Ts
	RedoBarrierTs     model.Ts
}

// NewBarrierWithMinTs creates a new BarrierWithMinTs.
func NewBarrierWithMinTs(ts model.Ts) *BarrierWithMinTs {
	return &BarrierWithMinTs{
		Barrier: &Barrier{
			GlobalBarrierTs: ts,
		},
		MinTableBarrierTs: ts,
		RedoBarrierTs:     ts,
	}
}
