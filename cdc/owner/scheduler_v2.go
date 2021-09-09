// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/p2p"
)

type schedulerV2 struct {
	state         *model.ChangefeedReactorState
	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter
}

func (s *schedulerV2) Tick(state *model.ChangefeedReactorState, currentTables []model.TableID, captures map[model.CaptureID]*model.CaptureInfo) {
}

func (s *schedulerV2) MoveTable(tableID model.TableID, target model.CaptureID) {
}

func (s *schedulerV2) Rebalance() {
}
