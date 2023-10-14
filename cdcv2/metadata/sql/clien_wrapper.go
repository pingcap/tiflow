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
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type clientWithCache[T TxnContext] struct {
	client      client[T]
	cache       *storage
	tabletasChs map[string]chan struct{}

	options *clientOptions
}

func newClientWithCache[T TxnContext](
	leaderChecker LeaderChecker[T],
	checker checker[T],
	options *clientOptions,
) (client[T], error) {
	if options.timeout < 0 {
		return nil, errors.ErrMetaClientInvalidConfig.
			GenWithStackByArgs("timeout must be greater than or equal to 0 when client cache is enabled")
	}
	if options.timeout >= options.cacheFlushInterval {
		return nil, errors.ErrMetaClientInvalidConfig.
			GenWithStackByArgs("timeout must be less than cacheFlushInterval when client cache is enabled")
	}
	clientWithTimeout := &clientWithTimeout[T]{
		LeaderChecker: leaderChecker,
		checker:       checker,
		timeout:       options.timeout,
	}
	return &clientWithCache[T]{
		client:  clientWithTimeout,
		cache:   &storage{},
		options: options,
	}, nil
}

// Txn implements the client interface.
func (c *clientWithCache[T]) Txn(ctx context.Context, fn TxnAction[T]) error {
	return c.client.Txn(ctx, fn)
}

// TxnWithOwnerLock implements the client interface.
func (c *clientWithCache[T]) TxnWithOwnerLock(ctx context.Context, uuid metadata.ChangefeedUUID, fn TxnAction[T]) error {
	return c.client.TxnWithOwnerLock(ctx, uuid, fn)
}

// TxnWithLeaderLock implements the client interface.
func (c *clientWithCache[T]) TxnWithLeaderLock(ctx context.Context, leaderID string, fn func(T) error) error {
	return c.client.TxnWithLeaderLock(ctx, leaderID, fn)
}

// ================================ Upstream Client =================================

// createUpstream implements the upstreamClient interface.
func (c *clientWithCache[T]) createUpstream(tx T, up *UpstreamDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.upsteram.insert(nil, up)
		}
	}()
	return c.client.createUpstream(tx, up)
}

// deleteUpstream implements the upstreamClient interface.
func (c *clientWithCache[T]) deleteUpstream(tx T, up *UpstreamDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.upsteram.update(nil, up)
		}
	}()
	return c.client.deleteUpstream(tx, up)
}

// updateUpstream implements the upstreamClient interface.
func (c *clientWithCache[T]) updateUpstream(tx T, up *UpstreamDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.upsteram.update(nil, up)
		}
	}()
	return c.updateUpstream(tx, up)
}

// queryUpstreams implements the upstreamClient interface.
func (c *clientWithCache[T]) queryUpstreams(tx T) ([]*UpstreamDO, error) {
	return c.cache.upsteram.getAll(), nil
}

// queryUpstreamsByUpdateAt implements the upstreamClient interface.
func (c *clientWithCache[T]) queryUpstreamsByUpdateAt(tx T, lastUpdateAt time.Time) ([]*UpstreamDO, error) {
	return c.client.queryUpstreamsByUpdateAt(tx, lastUpdateAt)
}

// queryUpstreamByID implements the upstreamClient interface.
func (c *clientWithCache[T]) queryUpstreamByID(tx T, id uint64) (*UpstreamDO, error) {
	return c.cache.upsteram.get(id), nil
}

// ================================ ChangefeedInfo Client =================================

// createChangefeedInfo implements the changefeedInfoClient interface.
func (c *clientWithCache[T]) createChangefeedInfo(tx T, info *ChangefeedInfoDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.info.insert(nil, info)
		}
	}()
	return c.client.createChangefeedInfo(tx, info)
}

// deleteChangefeedInfo implements the changefeedInfoClient interface.
func (c *clientWithCache[T]) deleteChangefeedInfo(tx T, info *ChangefeedInfoDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.info.update(nil, info)
		}
	}()
	return c.client.deleteChangefeedInfo(tx, info)
}

// markChangefeedRemoved implements the changefeedInfoClient interface.
func (c *clientWithCache[T]) markChangefeedRemoved(tx T, info *ChangefeedInfoDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.info.update(nil, info)
		}
	}()
	return c.client.markChangefeedRemoved(tx, info)
}

// updateChangefeedInfo implements the changefeedInfoClient interface.
func (c *clientWithCache[T]) updateChangefeedInfo(tx T, info *ChangefeedInfoDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.info.update(nil, info)
		}
	}()
	return c.client.updateChangefeedInfo(tx, info)
}

// queryChangefeedInfos implements the changefeedInfoClient interface.
func (c *clientWithCache[T]) queryChangefeedInfos(tx T) ([]*ChangefeedInfoDO, error) {
	return c.cache.info.getAll(), nil
}

// queryChangefeedInfosByUpdateAt implements the changefeedInfoClient interface.
func (c *clientWithCache[T]) queryChangefeedInfosByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ChangefeedInfoDO, error) {
	return c.client.queryChangefeedInfosByUpdateAt(tx, lastUpdateAt)
}

// queryChangefeedInfosByUUIDs implements the changefeedInfoClient interface.
func (c *clientWithCache[T]) queryChangefeedInfosByUUIDs(tx T, uuids ...uint64) ([]*ChangefeedInfoDO, error) {
	return c.cache.info.getByKeys(uuids), nil
}

// queryChangefeedInfoByUUID implements the changefeedInfoClient interface.
func (c *clientWithCache[T]) queryChangefeedInfoByUUID(tx T, uuid uint64) (*ChangefeedInfoDO, error) {
	return c.cache.info.get(uuid), nil
}

// ================================ ChangefeedState Client =================================

// createChangefeedState implements the changefeedStateClient interface.
func (c *clientWithCache[T]) createChangefeedState(tx T, state *ChangefeedStateDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.state.insert(nil, state)
		}
	}()
	return c.client.createChangefeedState(tx, state)
}

// deleteChangefeedState implements the changefeedStateClient interface.
func (c *clientWithCache[T]) deleteChangefeedState(tx T, state *ChangefeedStateDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.state.delete(nil, state)
		}
	}()
	return c.deleteChangefeedState(tx, state)
}

// updateChangefeedState implements the changefeedStateClient interface.
func (c *clientWithCache[T]) updateChangefeedState(tx T, state *ChangefeedStateDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.state.update(nil, state)
		}
	}()
	return c.updateChangefeedState(tx, state)
}

// queryChangefeedStates implements the changefeedStateClient interface.
func (c *clientWithCache[T]) queryChangefeedStates(tx T) ([]*ChangefeedStateDO, error) {
	return c.cache.state.getAll(), nil
}

// queryChangefeedStatesByUpdateAt implements the changefeedStateClient interface.
func (c *clientWithCache[T]) queryChangefeedStatesByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ChangefeedStateDO, error) {
	return c.client.queryChangefeedStatesByUpdateAt(tx, lastUpdateAt)
}

// queryChangefeedStateByUUID implements the changefeedStateClient interface.
func (c *clientWithCache[T]) queryChangefeedStateByUUID(tx T, uuid uint64) (*ChangefeedStateDO, error) {
	return c.cache.state.get(uuid), nil
}

// queryChangefeedStateByUUIDs implements the changefeedStateClient interface.
func (c *clientWithCache[T]) queryChangefeedStateByUUIDs(tx T, uuids ...uint64) ([]*ChangefeedStateDO, error) {
	return c.cache.state.getByKeys(uuids), nil
}

// queryChangefeedStateByUUIDWithLock implements the changefeedStateClient interface.
func (c *clientWithCache[T]) queryChangefeedStateByUUIDWithLock(tx T, uuid uint64) (state *ChangefeedStateDO, err error) {
	defer func() {
		if err == nil {
			c.cache.state.update(nil, state)
		}
	}()
	return c.client.queryChangefeedStateByUUIDWithLock(tx, uuid)
}

// ================================ Schedule Client =================================

// createSchedule implements the scheduleClient interface.
func (c *clientWithCache[T]) createSchedule(tx T, sc *ScheduleDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.schedule.insert(nil, sc)
		}
	}()
	return c.client.createSchedule(tx, sc)
}

// deleteSchedule implements the scheduleClient interface.
func (c *clientWithCache[T]) deleteSchedule(tx T, sc *ScheduleDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.schedule.delete(sc)
		}
	}()
	return c.client.deleteSchedule(tx, sc)
}

// updateSchedule implements the scheduleClient interface.
func (c *clientWithCache[T]) updateSchedule(tx T, sc *ScheduleDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.schedule.update(nil, sc)
		}
	}()
	return c.client.updateSchedule(tx, sc)
}

// updateScheduleOwnerState implements the scheduleClient interface.
// TODO(CharlesCheung): check if the version needs to be checked.
func (c *clientWithCache[T]) updateScheduleOwnerState(tx T, sc *ScheduleDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.schedule.update(nil, sc)
		}
	}()
	return c.client.updateScheduleOwnerState(tx, sc)
}

// updateScheduleOwnerStateByOwnerID implements the scheduleClient interface.
func (c *clientWithCache[T]) updateScheduleOwnerStateByOwnerID(
	tx T, state metadata.SchedState, ownerID model.CaptureID,
) (err error) {
	defer func() {
		if err != nil {
			scs := c.cache.schedule.getAll()
			for _, sc := range scs {
				if sc.Owner != nil && *sc.Owner == ownerID {
					sc.OwnerState = state
					c.cache.schedule.update(nil, sc)
				}
			}
		}
	}()
	return c.client.updateScheduleOwnerStateByOwnerID(tx, state, ownerID)
}

// querySchedules implements the scheduleClient interface.
func (c *clientWithCache[T]) querySchedules(tx T) ([]*ScheduleDO, error) {
	return c.cache.schedule.getAll(), nil
}

// querySchedulesByUpdateAt implements the scheduleClient interface.
func (c *clientWithCache[T]) querySchedulesByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ScheduleDO, error) {
	return c.client.querySchedulesByUpdateAt(tx, lastUpdateAt)
}

// querySchedulesByOwnerIDAndUpdateAt implements the scheduleClient interface.
func (c *clientWithCache[T]) querySchedulesByOwnerIDAndUpdateAt(
	tx T, captureID model.CaptureID, lastUpdateAt time.Time,
) ([]*ScheduleDO, error) {
	return c.client.querySchedulesByOwnerIDAndUpdateAt(tx, captureID, lastUpdateAt)
}

// queryScheduleByUUID implements the scheduleClient interface.
func (c *clientWithCache[T]) queryScheduleByUUID(tx T, uuid uint64) (*ScheduleDO, error) {
	return c.cache.schedule.get(uuid), nil
}

// querySchedulesUinqueOwnerIDs implements the scheduleClient interface.
func (c *clientWithCache[T]) querySchedulesUinqueOwnerIDs(tx T) ([]model.CaptureID, error) {
	return c.client.querySchedulesUinqueOwnerIDs(tx)
}

// ================================ Progress Client =================================

// createProgress implements the progressClient interface.
func (c *clientWithCache[T]) createProgress(tx T, pr *ProgressDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.progress.insert(nil, pr)
		}
	}()
	return c.client.createProgress(tx, pr)
}

// deleteProgress implements the progressClient interface.
func (c *clientWithCache[T]) deleteProgress(tx T, pr *ProgressDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.progress.delete(nil, pr)
		}
	}()
	return c.client.deleteProgress(tx, pr)
}

// updateProgress implements the progressClient interface.
func (c *clientWithCache[T]) updateProgress(tx T, pr *ProgressDO) (err error) {
	defer func() {
		if err == nil {
			c.cache.progress.update(nil, pr)
		}
	}()
	return c.client.updateProgress(tx, pr)
}

// queryProgresses implements the progressClient interface.
func (c *clientWithCache[T]) queryProgresses(tx T) ([]*ProgressDO, error) {
	return c.cache.progress.getAll(), nil
}

// queryProgressesByUpdateAt implements the progressClient interface.
func (c *clientWithCache[T]) queryProgressesByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ProgressDO, error) {
	return c.client.queryProgressesByUpdateAt(tx, lastUpdateAt)
}

// queryProgressByCaptureID implements the progressClient interface.
func (c *clientWithCache[T]) queryProgressByCaptureID(tx T, id string) (*ProgressDO, error) {
	return c.cache.progress.get(id), nil
}

// queryProgressByCaptureIDsWithLock implements the progressClient interface.
func (c *clientWithCache[T]) queryProgressByCaptureIDsWithLock(tx T, ids []string) ([]*ProgressDO, error) {
	return c.cache.progress.getByKeys(ids), nil
}

func (c *clientWithCache[T]) Run(ctx context.Context) (err error) {
	defer func() {
		if err != nil && err != context.Canceled {
			log.Error("table cache service exit with error", zap.Error(err))
		}
	}()
	tabletaskChs, tableFlushFns := c.init()

	eg, egCtx := errgroup.WithContext(ctx)
	for t := range tabletaskChs {
		table := t
		eg.Go(func() error {
			log.Info("table cache service start", zap.String("table", table))
			for {
				select {
				case <-egCtx.Done():
					return egCtx.Err()
				case <-tabletaskChs[table]:
					if err := tableFlushFns[table](egCtx); err != nil {
						log.Error("flush cache failed", zap.String("table", table), zap.Error(err))
						return err
					}
				}
			}
		})
	}

	eg.Go(func() error {
		ticker := time.NewTicker(c.options.cacheFlushInterval)
		defer ticker.Stop()

		log.Info("background service of clientWithCache start",
			zap.Duration("interval", c.options.cacheFlushInterval))
		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case <-ticker.C:
				for table := range tabletaskChs {
					tabletaskChs[table] <- struct{}{}
				}
			}
		}
	})
	return eg.Wait()
}

func (c *clientWithCache[T]) init() (map[string]chan struct{}, map[string]flushFn) {
	tabletaskChs := make(map[string]chan struct{})
	tableFlushFns := make(map[string]flushFn)

	tabletaskChs[tableNameUpstream] = make(chan struct{}, 1)
	tableFlushFns[tableNameUpstream] = func(ctx context.Context) error {
		safepoint := c.cache.upsteram.getSafePoint()
		var ups []*UpstreamDO
		if err := c.client.Txn(ctx, func(tx T) error {
			inUps, inErr := c.client.queryUpstreamsByUpdateAt(tx, safepoint)
			ups = inUps
			return inErr
		}); err != nil {
			return errors.Trace(err)
		}

		c.cache.upsteram.update(nil, ups...)
		return nil
	}

	tabletaskChs[tableNameChangefeedInfo] = make(chan struct{}, 1)
	tableFlushFns[tableNameChangefeedInfo] = func(ctx context.Context) error {
		safepoint := c.cache.info.getSafePoint()
		var infos []*ChangefeedInfoDO
		if err := c.client.Txn(ctx, func(tx T) error {
			inInfos, inErr := c.client.queryChangefeedInfosByUpdateAt(tx, safepoint)
			infos = inInfos
			return inErr
		}); err != nil {
			return errors.Trace(err)
		}

		c.cache.info.update(nil, infos...)
		return nil
	}

	tabletaskChs[tableNameChangefeedState] = make(chan struct{}, 1)
	tableFlushFns[tableNameChangefeedState] = func(ctx context.Context) error {
		safepoint := c.cache.state.getSafePoint()
		var states []*ChangefeedStateDO
		if err := c.client.Txn(ctx, func(tx T) error {
			inStates, inErr := c.client.queryChangefeedStatesByUpdateAt(tx, safepoint)
			states = inStates
			return inErr
		}); err != nil {
			return errors.Trace(err)
		}

		c.cache.state.update(nil, states...)
		return nil
	}

	tabletaskChs[tableNameSchedule] = make(chan struct{}, 1)
	tableFlushFns[tableNameSchedule] = func(ctx context.Context) error {
		safepoint := c.cache.schedule.getSafePoint()
		var schedules []*ScheduleDO
		if err := c.client.Txn(ctx, func(tx T) error {
			inSchedules, inErr := c.client.querySchedulesByUpdateAt(tx, safepoint)
			schedules = inSchedules
			return inErr
		}); err != nil {
			return errors.Trace(err)
		}

		c.cache.schedule.update(nil, schedules...)
		return nil
	}

	tabletaskChs[tableNameProgress] = make(chan struct{}, 1)
	tableFlushFns[tableNameProgress] = func(ctx context.Context) error {
		safepoint := c.cache.progress.getSafePoint()
		var progresses []*ProgressDO
		if err := c.client.Txn(ctx, func(tx T) error {
			inProgresses, inErr := c.client.queryProgressesByUpdateAt(tx, safepoint)
			progresses = inProgresses
			return inErr
		}); err != nil {
			return errors.Trace(err)
		}

		c.cache.progress.update(nil, progresses...)
		return nil
	}

	return tabletaskChs, tableFlushFns
}

type flushFn func(ctx context.Context) error

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
				e.m[key] = newV
				continue
			}
		}

		if !ok || oldV.GetVersion() < newV.GetVersion() {
			if onchange != nil && onchange(newV) {
				log.Debug("skip update or insert", zap.Any("old", oldV), zap.Any("new", newV))
				continue
			}

			e.m[key] = newV
			newUpdataAt := newV.GetUpdateAt()
			if newUpdataAt.After(e.lastUpdateAt) {
				e.lastUpdateAt = newUpdataAt
			}
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
