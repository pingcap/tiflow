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
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/tp/schedulepb"
)

var _ scheduler.Agent = (*agent)(nil)

type agent struct {
	trans     transport
	tableExec scheduler.TableExecutor

	tables       map[model.TableID]*schedulepb.TableStatus
	runningTasks map[model.TableID]*schedulepb.Message
}

func (a *agent) Tick(ctx context.Context) error {
	return nil
}

func (a *agent) GetLastSentCheckpointTs() (checkpointTs model.Ts) {
	return scheduler.CheckpointCannotProceed
}

func (a *agent) Close() error {
	return nil
}

func (a *agent) handleMessage(msg []*schedulepb.Message) {
	// s.handleMessageAnnounce()
	// s.handleMessageDispatchTableRequest()
}

func (a *agent) handleMessageAnnounce(msg *schedulepb.Sync) {
	// TODO: build s.tables from Sync message.
}

func (a *agent) handleMessageDispatchTableRequest(msg *schedulepb.DispatchTableResponse) {
	// TODO: update s.tables from DispatchTableResponse message.
}
