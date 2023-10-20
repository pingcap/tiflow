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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

var (
	_ checker[*ormWrapper]       = &ormClientWithCache{}
	_ LeaderChecker[*ormWrapper] = &ormClientWithCache{}

	_ upstreamClient[*ormWrapper]        = &ormClientWithCache{}
	_ changefeedInfoClient[*ormWrapper]  = &ormClientWithCache{}
	_ changefeedStateClient[*ormWrapper] = &ormClientWithCache{}
	_ scheduleClient[*ormWrapper]        = &ormClientWithCache{}
	_ progressClient[*ormWrapper]        = &ormClientWithCache{}
)

type ormClientWithCache struct {
	client        checker[*gorm.DB]
	leaderChecker LeaderChecker[*gorm.DB]

	cache       *storage
	tabletasChs map[string]chan struct{}

	options *clientOptions
}

type ormWrapper struct {
	txn     *gorm.DB
	applyFn []func()
}

func unwrapAction(fn ormWrapperAction) ormTxnAction {
	return func(txn *gorm.DB) error {
		w := &ormWrapper{txn: txn}
		err := fn(w)
		if err != nil {
			return err
		}
		for _, applyFn := range w.applyFn {
			applyFn()
		}
		return nil
	}
}

func NewOrmClientWithCache(
	leaderChecker LeaderChecker[*gorm.DB],
	checker checker[*gorm.DB],
	opts ...ClientOptionFunc,
) (client[*ormWrapper], error) {
	options := setClientOptions(opts...)
	if options.cacheFlushInterval < 0 {
		return nil, errors.ErrMetaClientInvalidConfig.
			GenWithStackByArgs("cacheFlushInterval must be greater than or equal to 0")
	}
	if options.timeout < 0 {
		return nil, errors.ErrMetaClientInvalidConfig.
			GenWithStackByArgs("timeout must be greater than or equal to 0 when client cache is enabled")
	}
	if options.timeout >= options.cacheFlushInterval {
		return nil, errors.ErrMetaClientInvalidConfig.
			GenWithStackByArgs("timeout must be less than cacheFlushInterval when client cache is enabled")
	}

	client := &ormClientWithCache{
		client:        checker.(*ormClient),
		leaderChecker: leaderChecker,
		options:       options,
	}
	return &clientWithTimeout[*ormWrapper]{
		LeaderChecker: client,
		checker:       client,
		timeout:       options.timeout,
	}, nil
}

// Txn implements the client interface.
func (c *ormClientWithCache) Txn(ctx context.Context, fn ormWrapperAction) error {
	return c.client.Txn(ctx, unwrapAction(fn))
}

// TxnWithOwnerLock implements the client interface.
func (c *ormClientWithCache) TxnWithOwnerLock(ctx context.Context, uuid metadata.ChangefeedUUID, fn ormWrapperAction) error {
	return c.client.TxnWithOwnerLock(ctx, uuid, unwrapAction(fn))
}

// TxnWithLeaderLock implements the client interface.
func (c *ormClientWithCache) TxnWithLeaderLock(ctx context.Context, leaderID string, fn func(*ormWrapper) error) error {
	return c.leaderChecker.TxnWithLeaderLock(ctx, leaderID, unwrapAction(fn))
}

// ================================ Upstream Client =================================

// createUpstream implements the upstreamClient interface.
func (c *ormClientWithCache) createUpstream(tx *ormWrapper, up *UpstreamDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.upsteram.insert(nil, up)
	})
	return c.client.createUpstream(tx.txn, up)
}

// deleteUpstream implements the upstreamClient interface.
func (c *ormClientWithCache) deleteUpstream(tx *ormWrapper, up *UpstreamDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.upsteram.delete(nil, up)
	})
	return c.client.deleteUpstream(tx.txn, up)
}

// updateUpstream implements the upstreamClient interface.
func (c *ormClientWithCache) updateUpstream(tx *ormWrapper, up *UpstreamDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.upsteram.update(nil, up)
	})
	return c.client.updateUpstream(tx.txn, up)
}

// queryUpstreams implements the upstreamClient interface.
func (c *ormClientWithCache) queryUpstreams(tx *ormWrapper) ([]*UpstreamDO, error) {
	return c.cache.upsteram.getAll(), nil
}

// queryUpstreamsByUpdateAt implements the upstreamClient interface.
func (c *ormClientWithCache) queryUpstreamsByUpdateAt(tx *ormWrapper, lastUpdateAt time.Time) ([]*UpstreamDO, error) {
	return c.client.queryUpstreamsByUpdateAt(tx.txn, lastUpdateAt)
}

// queryUpstreamsByIDs implements the upstreamClient interface.
func (c *ormClientWithCache) queryUpstreamsByIDs(tx *ormWrapper, ids ...uint64) ([]*UpstreamDO, error) {
	return c.cache.upsteram.getByKeys(ids), nil
}

// queryUpstreamByID implements the upstreamClient interface.
func (c *ormClientWithCache) queryUpstreamByID(tx *ormWrapper, id uint64) (*UpstreamDO, error) {
	return c.cache.upsteram.get(id), nil
}

// ================================ ChangefeedInfo Client =================================

// createChangefeedInfo implements the changefeedInfoClient interface.
func (c *ormClientWithCache) createChangefeedInfo(tx *ormWrapper, info *ChangefeedInfoDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.info.insert(nil, info)
	})
	return c.client.createChangefeedInfo(tx.txn, info)
}

// deleteChangefeedInfo implements the changefeedInfoClient interface.
func (c *ormClientWithCache) deleteChangefeedInfo(tx *ormWrapper, info *ChangefeedInfoDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.info.delete(nil, info)
	})
	return c.client.deleteChangefeedInfo(tx.txn, info)
}

// markChangefeedRemoved implements the changefeedInfoClient interface.
func (c *ormClientWithCache) markChangefeedRemoved(tx *ormWrapper, info *ChangefeedInfoDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.info.update(nil, info)
	})
	return c.client.markChangefeedRemoved(tx.txn, info)
}

// updateChangefeedInfo implements the changefeedInfoClient interface.
func (c *ormClientWithCache) updateChangefeedInfo(tx *ormWrapper, info *ChangefeedInfoDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.info.update(nil, info)
	})
	return c.client.updateChangefeedInfo(tx.txn, info)
}

// queryChangefeedInfos implements the changefeedInfoClient interface.
func (c *ormClientWithCache) queryChangefeedInfos(tx *ormWrapper) ([]*ChangefeedInfoDO, error) {
	return c.cache.info.getAll(), nil
}

// queryChangefeedInfosByUpdateAt implements the changefeedInfoClient interface.
func (c *ormClientWithCache) queryChangefeedInfosByUpdateAt(tx *ormWrapper, lastUpdateAt time.Time) ([]*ChangefeedInfoDO, error) {
	return c.client.queryChangefeedInfosByUpdateAt(tx.txn, lastUpdateAt)
}

// queryChangefeedInfosByUUIDs implements the changefeedInfoClient interface.
func (c *ormClientWithCache) queryChangefeedInfosByUUIDs(tx *ormWrapper, uuids ...uint64) ([]*ChangefeedInfoDO, error) {
	return c.cache.info.getByKeys(uuids), nil
}

// queryChangefeedInfoByUUID implements the changefeedInfoClient interface.
func (c *ormClientWithCache) queryChangefeedInfoByUUID(tx *ormWrapper, uuid uint64) (*ChangefeedInfoDO, error) {
	return c.cache.info.get(uuid), nil
}

// ================================ ChangefeedState Client =================================

// createChangefeedState implements the changefeedStateClient interface.
func (c *ormClientWithCache) createChangefeedState(tx *ormWrapper, state *ChangefeedStateDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.state.insert(nil, state)
	})
	return c.client.createChangefeedState(tx.txn, state)
}

// deleteChangefeedState implements the changefeedStateClient interface.
func (c *ormClientWithCache) deleteChangefeedState(tx *ormWrapper, state *ChangefeedStateDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.state.delete(state)
	})
	return c.client.deleteChangefeedState(tx.txn, state)
}

// updateChangefeedState implements the changefeedStateClient interface.
func (c *ormClientWithCache) updateChangefeedState(tx *ormWrapper, state *ChangefeedStateDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.state.update(nil, state)
	})
	return c.client.updateChangefeedState(tx.txn, state)
}

// queryChangefeedStates implements the changefeedStateClient interface.
func (c *ormClientWithCache) queryChangefeedStates(tx *ormWrapper) ([]*ChangefeedStateDO, error) {
	return c.cache.state.getAll(), nil
}

// queryChangefeedStatesByUpdateAt implements the changefeedStateClient interface.
func (c *ormClientWithCache) queryChangefeedStatesByUpdateAt(tx *ormWrapper, lastUpdateAt time.Time) ([]*ChangefeedStateDO, error) {
	return c.client.queryChangefeedStatesByUpdateAt(tx.txn, lastUpdateAt)
}

// queryChangefeedStateByUUID implements the changefeedStateClient interface.
func (c *ormClientWithCache) queryChangefeedStateByUUID(tx *ormWrapper, uuid uint64) (*ChangefeedStateDO, error) {
	return c.cache.state.get(uuid), nil
}

// queryChangefeedStateByUUIDs implements the changefeedStateClient interface.
func (c *ormClientWithCache) queryChangefeedStateByUUIDs(tx *ormWrapper, uuids ...uint64) ([]*ChangefeedStateDO, error) {
	return c.cache.state.getByKeys(uuids), nil
}

// queryChangefeedStateByUUIDWithLock implements the changefeedStateClient interface.
func (c *ormClientWithCache) queryChangefeedStateByUUIDWithLock(tx *ormWrapper, uuid uint64) (state *ChangefeedStateDO, err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.state.update(nil, state)
	})
	return c.client.queryChangefeedStateByUUIDWithLock(tx.txn, uuid)
}

// ================================ Schedule Client =================================

// createSchedule implements the scheduleClient interface.
func (c *ormClientWithCache) createSchedule(tx *ormWrapper, sc *ScheduleDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.schedule.insert(nil, sc)
	})
	return c.client.createSchedule(tx.txn, sc)
}

// deleteSchedule implements the scheduleClient interface.
func (c *ormClientWithCache) deleteSchedule(tx *ormWrapper, sc *ScheduleDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.schedule.delete(nil, sc)
	})
	return c.client.deleteSchedule(tx.txn, sc)
}

// updateSchedule implements the scheduleClient interface.
func (c *ormClientWithCache) updateSchedule(tx *ormWrapper, sc *ScheduleDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.schedule.update(nil, sc)
	})
	return c.client.updateSchedule(tx.txn, sc)
}

// updateScheduleOwnerState implements the scheduleClient interface.
// TODO(CharlesCheung): check if the version needs to be checked.
func (c *ormClientWithCache) updateScheduleOwnerState(tx *ormWrapper, sc *ScheduleDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.schedule.update(nil, sc)
	})
	return c.client.updateScheduleOwnerState(tx.txn, sc)
}

// updateScheduleOwnerStateByOwnerID implements the scheduleClient interface.
func (c *ormClientWithCache) updateScheduleOwnerStateByOwnerID(
	tx *ormWrapper, state metadata.SchedState, ownerID model.CaptureID,
) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		scs := c.cache.schedule.getAll()
		for _, sc := range scs {
			if sc.Owner != nil && *sc.Owner == ownerID {
				sc.OwnerState = state
				c.cache.schedule.update(nil, sc)
			}
		}
	})
	return c.client.updateScheduleOwnerStateByOwnerID(tx.txn, state, ownerID)
}

// querySchedules implements the scheduleClient interface.
func (c *ormClientWithCache) querySchedules(tx *ormWrapper) ([]*ScheduleDO, error) {
	return c.cache.schedule.getAll(), nil
}

// querySchedulesByUpdateAt implements the scheduleClient interface.
func (c *ormClientWithCache) querySchedulesByUpdateAt(tx *ormWrapper, lastUpdateAt time.Time) ([]*ScheduleDO, error) {
	return c.client.querySchedulesByUpdateAt(tx.txn, lastUpdateAt)
}

// querySchedulesByOwnerIDAndUpdateAt implements the scheduleClient interface.
func (c *ormClientWithCache) querySchedulesByOwnerIDAndUpdateAt(
	tx *ormWrapper, captureID model.CaptureID, lastUpdateAt time.Time,
) ([]*ScheduleDO, error) {
	return c.client.querySchedulesByOwnerIDAndUpdateAt(tx.txn, captureID, lastUpdateAt)
}

// queryScheduleByUUID implements the scheduleClient interface.
func (c *ormClientWithCache) queryScheduleByUUID(tx *ormWrapper, uuid uint64) (*ScheduleDO, error) {
	return c.cache.schedule.get(uuid), nil
}

// querySchedulesUinqueOwnerIDs implements the scheduleClient interface.
func (c *ormClientWithCache) querySchedulesUinqueOwnerIDs(tx *ormWrapper) ([]model.CaptureID, error) {
	return c.client.querySchedulesUinqueOwnerIDs(tx.txn)
}

// ================================ Progress Client =================================

// createProgress implements the progressClient interface.
func (c *ormClientWithCache) createProgress(tx *ormWrapper, pr *ProgressDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.progress.insert(nil, pr)
	})
	return c.client.createProgress(tx.txn, pr)
}

// deleteProgress implements the progressClient interface.
func (c *ormClientWithCache) deleteProgress(tx *ormWrapper, pr *ProgressDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.progress.delete(nil, pr)
	})
	return c.client.deleteProgress(tx.txn, pr)
}

// updateProgress implements the progressClient interface.
func (c *ormClientWithCache) updateProgress(tx *ormWrapper, pr *ProgressDO) (err error) {
	tx.applyFn = append(tx.applyFn, func() {
		c.cache.progress.update(nil, pr)
	})
	return c.client.updateProgress(tx.txn, pr)
}

// queryProgresses implements the progressClient interface.
func (c *ormClientWithCache) queryProgresses(tx *ormWrapper) ([]*ProgressDO, error) {
	return c.cache.progress.getAll(), nil
}

// queryProgressesByUpdateAt implements the progressClient interface.
func (c *ormClientWithCache) queryProgressesByUpdateAt(tx *ormWrapper, lastUpdateAt time.Time) ([]*ProgressDO, error) {
	return c.client.queryProgressesByUpdateAt(tx.txn, lastUpdateAt)
}

// queryProgressByCaptureID implements the progressClient interface.
func (c *ormClientWithCache) queryProgressByCaptureID(tx *ormWrapper, id string) (*ProgressDO, error) {
	return c.cache.progress.get(id), nil
}

// queryProgressByCaptureIDsWithLock implements the progressClient interface.
func (c *ormClientWithCache) queryProgressByCaptureIDsWithLock(tx *ormWrapper, ids []string) ([]*ProgressDO, error) {
	return c.cache.progress.getByKeys(ids), nil
}

func (c *ormClientWithCache) Run(ctx context.Context) (err error) {
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

func (c *ormClientWithCache) init() (map[string]chan struct{}, map[string]flushFn) {
	tabletaskChs := make(map[string]chan struct{})
	tableFlushFns := make(map[string]flushFn)

	tabletaskChs[tableNameUpstream] = make(chan struct{}, 1)
	tableFlushFns[tableNameUpstream] = func(ctx context.Context) error {
		safepoint := c.cache.upsteram.getSafePoint()
		var ups []*UpstreamDO
		if err := c.client.Txn(ctx, func(tx *gorm.DB) error {
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
		if err := c.client.Txn(ctx, func(tx *gorm.DB) error {
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
		if err := c.client.Txn(ctx, func(tx *gorm.DB) error {
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
		if err := c.client.Txn(ctx, func(tx *gorm.DB) error {
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
		if err := c.client.Txn(ctx, func(tx *gorm.DB) error {
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
