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
	// DDLBarrier denotes a replication barrier caused by a DDL.
	DDLJobBarrier barrierType = iota
	DDLResolvedTs
	// SyncPointBarrier denotes a barrier for snapshot replication.
	SyncPointBarrier
	// TODO support snapshot replication.
)

type barrier struct {
	tp    barrierType
	index uint64
}

type barriers struct {
	inner map[barrier]model.Ts
	dirty bool
	min   barrier
}

func newBarriers() *barriers {
	return &barriers{
		inner: make(map[barrier]model.Ts),
		dirty: true,
	}
}

func (b *barriers) Update(tp barrierType, index uint64, barrierTs model.Ts) {
	if !b.dirty && barrierTs < b.inner[b.min] {
		b.dirty = true
	}
	b.inner[barrier{tp: tp, index: index}] = barrierTs
}

func (b *barriers) Min() (tp barrierType, index uint64, barrierTs model.Ts) {
	if !b.dirty {
		return b.min.tp, b.min.index, b.inner[b.min]
	}
	tp, index, minTs := b.calcMin()
	b.min = barrier{tp: tp, index: index}
	b.dirty = false
	return tp, index, minTs
}

func (b *barriers) calcMin() (tp barrierType, index uint64, barrierTs model.Ts) {
	if len(b.inner) == 0 {
		log.Panic("there are no barrier in owner, please report a bug")
	}
	barrierTs = uint64(math.MaxUint64)
	for br, ts := range b.inner {
		if ts <= barrierTs {
			tp = br.tp
			index = br.index
			barrierTs = ts
		}
	}
	return
}

func (b *barriers) Remove(tp barrierType, index uint64) {
	delete(b.inner, barrier{tp: tp, index: index})
	b.dirty = true
}
