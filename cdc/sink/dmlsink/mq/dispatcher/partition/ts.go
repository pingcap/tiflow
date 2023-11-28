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
	"fmt"

	"github.com/pingcap/tiflow/cdc/model"
)

// TsDispatcher is a partition dispatcher which dispatch events based on ts.
type TsDispatcher struct{}

// NewTsDispatcher creates a TsDispatcher.
func NewTsDispatcher() *TsDispatcher {
	return &TsDispatcher{}
}

// DispatchRowChangedEvent returns the target partition to which
// a row changed event should be dispatched.
func (t *TsDispatcher) DispatchRowChangedEvent(row *model.RowChangedEvent, partitionNum int32) (int32, string, error) {
	return int32(row.CommitTs % uint64(partitionNum)), fmt.Sprintf("%d", row.CommitTs), nil
}
