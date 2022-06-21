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

package internal

import "sync"

type Dependency struct {
	id int64

	mu       sync.Mutex
	parents  map[int64]*Dependency
	children map[int64]*Dependency
	dying    bool
}

func (d *Dependency) DependOn(parent *Dependency) {
	lockPair(d, parent)
	defer unlockPair(d, parent)

	if parent.dying {
		return
	}

	d.parents[parent.id] = parent
	parent.children[d.id] = d
}

func (d *Dependency) AbandonAll() {
	d.mu.Lock()
	d.dying = true
	d.mu.Unlock()

	// No one else will modify our children map,
	// now that dying == true.
	for _, child := range d.children {
		// Use a closure so that deferred unlock can be used.
		func() {
			child.mu.Lock()
			defer child.mu.Unlock()

			delete(child.parents, d.id)
		}()
	}
}

func lockPair(a, b *Dependency) {
	if a.id >= b.id {
		a.mu.Lock()
		b.mu.Lock()
	} else {
		b.mu.Lock()
		a.mu.Lock()
	}
}

func unlockPair(a, b *Dependency) {
	a.mu.Unlock()
	b.mu.Unlock()
}
