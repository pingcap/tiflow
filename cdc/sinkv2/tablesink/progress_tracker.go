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

package tablesink

import (
	"sync"

	"github.com/emirpasic/gods/maps/linkedhashmap"
	"github.com/pingcap/tiflow/cdc/model"
)

type progressTracker struct {
	lock              sync.Mutex
	pendingTs         *linkedhashmap.Map
	lastMinResolvedTs model.ResolvedTs
}

func (r *progressTracker) add(key any, resolvedTs model.ResolvedTs) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.pendingTs.Put(key, resolvedTs)
}

func (r *progressTracker) remove(key any) {
	r.lock.Lock()
	defer r.lock.Unlock()
	iterator := r.pendingTs.Iterator()
	if iterator.First() {
		if iterator.Key() == key {
			// It means we need to advance the min ts.
			// We need to store the last min ts.
			r.lastMinResolvedTs = iterator.Value().(model.ResolvedTs)
		}
	}
	r.pendingTs.Remove(key)
}

func (r *progressTracker) minTs() model.ResolvedTs {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.lastMinResolvedTs
}
