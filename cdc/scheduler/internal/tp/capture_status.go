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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
)

// captureState is the state of a capture.
//
//            ┌────────┐ onSync() ┌────────┐
//  Absent ─> │ UnSync ├─────────>│ Synced │
//            └───┬────┘          └────┬───┘
//                │                    │
//     onReject() │    ┌──────────┐    │ onReject()
//                └──> │ Rejected │ <──┘
//                     └──────────┘
type captureState int

const (
	captureStateUnSync  captureState = 1
	captureStateSynced  captureState = 2
	captureStateRejcted captureState = 3
)

type captureStatus struct {
	state captureState
	// tableSet is a set of table ID that is in a capture.
	tableSet map[model.TableID]schedulepb.TableState
}

func (c *captureStatus) onSync(sync *schedulepb.Sync)   {}
func (c *captureStatus) onReject(sync *schedulepb.Sync) {}
