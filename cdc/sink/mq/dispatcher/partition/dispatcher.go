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

// Dispatcher is an abstraction for dispatching rows into different partitions
type Dispatcher interface {
	// DispatchRowChangedEvent returns an index of partitions according to RowChangedEvent.
	// Concurrency Note: This method is thread-safe.
	DispatchRowChangedEvent(row *model.RowChangedEvent, partitionNum int32) int32
}
