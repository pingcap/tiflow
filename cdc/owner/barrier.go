// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"math"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
)

type barrierType int

const (
	// ddlJobBarrier denotes a replication barrier caused by a DDL.
	ddlJobBarrier barrierType = iota
	// syncPointBarrier denotes a barrier for snapshot replication.
	syncPointBarrier
	// finishBarrier denotes a barrier for changefeed finished.
	finishBarrier
)

// barriers stores some barrierType and barrierTs, and can calculate the min barrierTs
// barriers is NOT-THREAD-SAFE
type barriers struct {
	inner map[barrierType]model.Ts
	dirty bool
	min   barrierType
}

func newBarriers() *barriers {
	return &barriers{
		inner: make(map[barrierType]model.Ts),
		dirty: true,
	}
}

func (b *barriers) Update(tp barrierType, barrierTs model.Ts) {
	// the barriers structure was given the ability to handle a fallback barrierTs by design.
	// but the barrierTs should never fallback in owner replication model
	if !b.dirty && (tp == b.min || barrierTs <= b.inner[b.min]) {
		b.dirty = true
	}
	b.inner[tp] = barrierTs
}

func (b *barriers) Min() (tp barrierType, barrierTs model.Ts) {
	if !b.dirty {
		return b.min, b.inner[b.min]
	}
	tp, minTs := b.calcMin()
	b.min = tp
	b.dirty = false
	return tp, minTs
}

func (b *barriers) calcMin() (tp barrierType, barrierTs model.Ts) {
	barrierTs = uint64(math.MaxUint64)
	for br, ts := range b.inner {
		if ts <= barrierTs {
			tp = br
			barrierTs = ts
		}
	}
	if barrierTs == math.MaxUint64 {
		log.Panic("the barriers is empty, please report a bug")
	}
	return
}

func (b *barriers) Remove(tp barrierType) {
	delete(b.inner, tp)
	b.dirty = true
}
