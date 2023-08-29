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

// PulsarTransportDispatcher is a partition dispatcher which dispatches events based transport
type PulsarTransportDispatcher struct {
	partitionKey string
}

// NewPulsarTransportDispatcher creates a TableDispatcher.
func NewPulsarTransportDispatcher(partitionKey string) *PulsarTransportDispatcher {
	return &PulsarTransportDispatcher{
		partitionKey: partitionKey,
	}
}

// DispatchRowChangedEvent returns the target partition to which
// a row changed event should be dispatched.
func (t *PulsarTransportDispatcher) DispatchRowChangedEvent(*model.RowChangedEvent, int32) (int32, *string) {
	return 0, &t.partitionKey
}
