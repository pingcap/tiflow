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
	"github.com/pingcap/ticdc/cdc/model"
)

type barrierType = int

const (
	// DDLJobBarrier denotes a replication barrier caused by a DDL.
	DDLJobBarrier barrierType = iota
	// SyncPointBarrier denotes a barrier for snapshot replication.
	SyncPointBarrier
)

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
	if !b.dirty && barrierTs < b.inner[b.min] {
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
	if len(b.inner) == 0 {
		log.Panic("there are no barrier in owner, please report a bug")
	}
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
