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

import "github.com/pingcap/tiflow/cdc/processor/tablepb"

type BlockType string

const (
	DDLBlock  BlockType = "ddl"
	SyncPoint BlockType = "syncPoint"
)

// 用来定义 tableEventDispatcher 上报的状态
type State struct {
	isBlocked    bool            // 是否卡着 cross table ddl / sync point event
	checkpointTs uint64          // 目前已经完全成功写入下游的最大 checkpointTs
	blockType    BlockType       // 因为什么被 block
	BlockFor     []*tablepb.Span // 这个最好再想一下存什么更好
}

type TableTriggerState struct {
	newTableSpans []*tablepb.Span
	checkpointTs  uint64
}
