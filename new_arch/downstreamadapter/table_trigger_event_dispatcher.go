// Copyright 2024 PingCAP, Inc.
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

package downstreamadapter

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/filter"
)

// 这个是专门来拉取跟建表语句相关的 dispatcher
type TableTriggerEventDispatcher struct {
	ctx          context.Context
	changefeedID model.ChangeFeedID
	checkpointTs uint64
	//newTableSpans []*tablepb.Span // 需要创建的新表 span
	spanToCheckpointTs map[*tablepb.Span]uint64 // 每个 span 对应 checkpointTs，用来计算整个的 checkpointTs
	state              TableTriggerState
	filter             filter.Filter
	endTs              uint64
	worker             Worker
}

func createTableTriggerEventDispatcher(ctx context.Context, changefeedID model.ChangeFeedID, startTs uint64, filter filter.Filter) *TableTriggerEventDispatcher {
	return &TableTriggerEventDispatcher{
		ctx:          ctx,
		changefeedID: changefeedID,
		checkpointTs: startTs, // 不确定这里是应该用 startTs 还是 startTs - 1
		filter:       filter,
	}
}

func (d *TableTriggerEventDispatcher) States() TableTriggerState {
	return d.state
}

func (d *TableTriggerEventDispatcher) pullEvents(beginTs uint64) ([]*TxnEvent, bool, error) { // ddl event 会有相同 commitTs 的么
	// 这边要确定能拉什么开头的 ddl event （create table / truncate table / add partition table / reorganize partition table / truncate partition table）
	// 拉到 event 后用 filter 做判断，如果需要的话，就加入 state 的 newTableSpans，以及更新 spanToCheckpointTs （ 所以这个 event 那的时候需要带 commitTs ）
	// checkpointTs 只在没有拉到 event 时候推进，要么就是拉到第一个的时候更新到 -1
}

func (d *TableTriggerEventDispatcher) handleHeartBeatResponse(newTable messages.NewTable) {
	// 遍历所有的 newTables，清理 spanToCheckpointTs，重新计算  checkpointTs
	// 如果 spanToCheckpointTs 不为空，就是里面最小的 ts -1, 否则就是拉到的 endTs
}
