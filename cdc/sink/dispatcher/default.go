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
	"github.com/pingcap/tiflow/cdc/model"
)

type defaultDispatcher struct {
	partitionNum   int32
	tbd            *tableDispatcher
	ivd            *indexValueDispatcher
	enableOldValue bool
}

func newDefaultDispatcher(partitionNum int32, enableOldValue bool) *defaultDispatcher {
	return &defaultDispatcher{
		partitionNum:   partitionNum,
		tbd:            newTableDispatcher(partitionNum),
		ivd:            newIndexValueDispatcher(partitionNum),
		enableOldValue: enableOldValue,
	}
}

func (d *defaultDispatcher) Dispatch(row *model.RowChangedEvent) int32 {
	if d.enableOldValue {
		return d.tbd.Dispatch(row)
	}
	if len(row.IndexColumns) != 1 {
		return d.tbd.Dispatch(row)
	}
	return d.ivd.Dispatch(row)
}
