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

type tsProgressTracker struct {
	lock      sync.Mutex
	pendingTs *linkedhashmap.Map
}

func (r *tsProgressTracker) add(key any, resolvedTs model.ResolvedTs) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.pendingTs.Put(key, resolvedTs)
}

func (r *tsProgressTracker) remove(key any) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.pendingTs.Remove(key)
}

func (r *tsProgressTracker) minTs() model.ResolvedTs {
	r.lock.Lock()
	defer r.lock.Unlock()
	iterator := r.pendingTs.Iterator()
	if !iterator.First() {
		return model.NewResolvedTs(0)
	}
	// TODO: commitTs -1
	return iterator.Value().(model.ResolvedTs)
}
