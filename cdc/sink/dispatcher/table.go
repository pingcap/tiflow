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

package dispatcher

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/hash"
)

type tableDispatcher struct {
	partitionNum int32
	hasher       *hash.PositionInertia
}

func newTableDispatcher(partitionNum int32) *tableDispatcher {
	return &tableDispatcher{
		partitionNum: partitionNum,
		hasher:       hash.NewPositionInertia(),
	}
}

func (t *tableDispatcher) Dispatch(row *model.RowChangedEvent) int32 {
	t.hasher.Reset()
	// distribute partition by table
	t.hasher.Write([]byte(row.Table.Schema), []byte(row.Table.Table))
	return int32(t.hasher.Sum32() % uint32(t.partitionNum))
}
