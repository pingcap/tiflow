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

package partition

import (
	"github.com/pingcap/tiflow/cdc/model"
)

// KeyDispatcher is a partition dispatcher which dispatches events
// using the provided partition key.
type KeyDispatcher struct {
	partitionKey string
}

// NewKeyDispatcher creates a TableDispatcher.
func NewKeyDispatcher(partitionKey string) *KeyDispatcher {
	return &KeyDispatcher{
		partitionKey: partitionKey,
	}
}

// DispatchRowChangedEvent returns the target partition to which
// a row changed event should be dispatched.
func (t *KeyDispatcher) DispatchRowChangedEvent(*model.RowChangedEvent, int32) (int32, string, error) {
	return 0, t.partitionKey, nil
}
