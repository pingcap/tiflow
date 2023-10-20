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

package sql

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"go.uber.org/zap"
)

type versionedRecord[K uint64 | string] interface {
	GetKey() K
	IncreaseVersion()
	GetVersion() uint64
	GetUpdateAt() time.Time
}

type entity[K uint64 | string, V versionedRecord[K]] struct {
	// The lock is used to protect the data in the entity. It is equivalent to
	// a table lock in database.
	sync.RWMutex
	// maxExecTime is the maximum insert/update time of the entity. Then, it can be
	// determined that all data before `lastUpdateAt-maxExecTime` has been pulled locally.
	maxExecTime  time.Duration
	lastUpdateAt time.Time

	// the data already cached locally.
	m map[K]V
}

func newEntity[K uint64 | string, V versionedRecord[K]](maxExecTime time.Duration) *entity[K, V] {
	return &entity[K, V]{
		maxExecTime:  maxExecTime,
		lastUpdateAt: time.Time{},
		m:            make(map[K]V),
	}
}

// getSafePoint returns the most recent safe timestamp, before which all data has
// been pulled locally.
func (e *entity[K, V]) getSafePoint() time.Time {
	e.RLock()
	defer e.RUnlock()

	if len(e.m) == 0 {
		// if there is no data, it means that the entity has not been initialized.
		return time.Time{}
	}
	return e.lastUpdateAt.Truncate(e.maxExecTime)
}

// getAll returns all the data in the entity.
func (e *entity[K, V]) getAll() []V {
	e.RLock()
	defer e.RUnlock()

	ret := make([]V, 0, len(e.m))
	for _, v := range e.m {
		ret = append(ret, v)
	}
	return ret
}

// getByKeys returns the data of the given keys.
func (e *entity[K, V]) getByKeys(keys []K) []V {
	e.RLock()
	defer e.RUnlock()

	ret := make([]V, 0, len(keys))
	for _, key := range keys {
		if v, ok := e.m[key]; ok {
			ret = append(ret, v)
		}
	}
	return ret
}

// get returns the value of the key.
func (e *entity[K, V]) get(key K) V {
	e.RLock()
	defer e.RUnlock()

	return e.m[key]
}

// doUpsert inserts or updates the entity upon the incoming data, and
// run the onchange callback if data is changed.
func (e *entity[K, V]) doUpsert(
	onchange func(newV V) (skip bool),
	inComming []V,
) {
	e.Lock()
	defer e.Unlock()

	upsert := func(key K, value V) {
		e.m[key] = value
		newUpdataAt := value.GetUpdateAt()
		if newUpdataAt.After(e.lastUpdateAt) {
			e.lastUpdateAt = newUpdataAt
		}
	}

	for _, newV := range inComming {
		key := newV.GetKey()
		oldV, ok := e.m[key]
		if ok {
			// check the version and update_at consistency.
			versionEqual := oldV.GetVersion() == newV.GetVersion()
			updateAtEqual := oldV.GetUpdateAt() == newV.GetUpdateAt()

			// Only update the update_at since the change has been applied to cache.
			if versionEqual && !updateAtEqual {
				log.Debug("inconsistent version and update_at", zap.Any("old", oldV), zap.Any("new", newV))
				// TODO: maybe we should check the equality of other fields.
				upsert(key, newV)
				continue
			}
		}

		if !ok || oldV.GetVersion() < newV.GetVersion() {
			if onchange != nil && onchange(newV) {
				log.Debug("skip update or insert", zap.Any("old", oldV), zap.Any("new", newV))
				continue
			}
			upsert(key, newV)
		}
	}
}

func (e *entity[K, V]) update(
	onchange func(newV V) (skip bool),
	inComming ...V,
) {
	for _, v := range inComming {
		v.IncreaseVersion()
	}
	e.doUpsert(onchange, inComming)
}

func (e *entity[K, V]) insert(
	onchange func(newV V) (skip bool),
	inComming ...V,
) {
	e.doUpsert(onchange, inComming)
}

// upsert inserts or updates the entity with write lock.
func (e *entity[K, V]) delete(
	inComming ...V,
) {
	e.Lock()
	defer e.Unlock()

	for _, v := range inComming {
		key := v.GetKey()
		delete(e.m, key)
	}
}

// nolint:unused
type storage struct {
	// the key is the upstream id.
	upsteram *entity[uint64, *UpstreamDO]
	// the key is the changefeed uuid.
	info *entity[metadata.ChangefeedUUID, *ChangefeedInfoDO]
	// the key is the changefeed uuid.
	state *entity[metadata.ChangefeedUUID, *ChangefeedStateDO]
	// the key is the changefeed uuid.
	schedule *entity[metadata.ChangefeedUUID, *ScheduleDO]
	// the key is the capture id.
	progress *entity[model.CaptureID, *ProgressDO]
}
