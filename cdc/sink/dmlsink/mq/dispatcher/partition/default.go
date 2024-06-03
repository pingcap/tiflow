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

package partition

import (
	"github.com/pingcap/tiflow/cdc/model"
)

// DefaultDispatcher is the default partition dispatcher.
type DefaultDispatcher struct {
	tbd *TableDispatcher
}

// NewDefaultDispatcher creates a DefaultDispatcher.
func NewDefaultDispatcher() *DefaultDispatcher {
	return &DefaultDispatcher{
		tbd: NewTableDispatcher(),
	}
}

// IsPartitionKeyUpdated checks whether the partition key is updated in the RowChangedEvent.
func (d *DefaultDispatcher) IsPartitionKeyUpdated(row *model.RowChangedEvent) (bool, error) {
	return d.tbd.IsPartitionKeyUpdated(row)
}

// DispatchRowChangedEvent returns the target partition to which
// a row changed event should be dispatched.
func (d *DefaultDispatcher) DispatchRowChangedEvent(row *model.RowChangedEvent, partitionNum int32) (int32, string, error) {
	return d.tbd.DispatchRowChangedEvent(row, partitionNum)
}
