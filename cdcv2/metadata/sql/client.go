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
	"database/sql"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const defaultMaxExecTime = 5 * time.Second

type client[T TxnContext] interface {
	LeaderChecker[T]
	checker[T]
}

type clientImpl[T TxnContext] struct {
	LeaderChecker[T]
	checker[T]

	options *clientOptions
}

func newClient[T TxnContext](
	leaderChecker LeaderChecker[T],
	checker checker[T],
	opts ...ClientOptionFunc,
) client[T] {
	options := setClientOptions(opts...)

	return &clientImpl[T]{
		LeaderChecker: leaderChecker,
		checker:       checker,
		options:       options,
	}
}

func (c *clientImpl[T]) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.options.maxExecTime <= 0 {
		return ctx, nil
	}
	return context.WithTimeout(ctx, c.options.maxExecTime)
}

func (c *clientImpl[T]) Txn(ctx context.Context, fn TxnAction[T]) error {
	ctx, cancel := c.withTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}
	return c.checker.Txn(ctx, fn)
}

func (c *clientImpl[T]) TxnWithOwnerLock(ctx context.Context, uuid metadata.ChangefeedUUID, fn TxnAction[T]) error {
	ctx, cancel := c.withTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}
	return c.checker.TxnWithOwnerLock(ctx, uuid, fn)
}

func (c *clientImpl[T]) TxnWithLeaderLock(ctx context.Context, leaderID string, fn func(T) error) error {
	ctx, cancel := c.withTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}
	return c.LeaderChecker.TxnWithLeaderLock(ctx, leaderID, fn)
}

// TxnContext is a type set that can be used as the transaction context.
type TxnContext interface {
	*gorm.DB | *sql.Tx
}

// TxnAction is a series of operations that can be executed in a transaction and the
// generic type T represents the transaction context.
//
// Note that in the current implementation the metadata operation and leader check are
// always in the same transaction context.
type TxnAction[T TxnContext] func(T) error

// ormTxnAction represents a transaction action that uses gorm.DB as the transaction context.
type ormTxnAction = TxnAction[*gorm.DB]

// sqlTxnAction represents a transaction action that uses sql.Tx as the transaction context.
// Note that sqlTxnAction is not implemented yet, it is reserved for future use.
//
//nolint:unused
type sqlTxnAction = TxnAction[*sql.Tx]

// LeaderChecker enables the controller to ensure its leadership during a series of actions.
type LeaderChecker[T TxnContext] interface {
	TxnWithLeaderLock(ctx context.Context, leaderID string, fn func(T) error) error
}

type checker[T TxnContext] interface {
	Txn(ctx context.Context, fn TxnAction[T]) error
	TxnWithOwnerLock(ctx context.Context, uuid metadata.ChangefeedUUID, fn TxnAction[T]) error

	upstreamClient[T]
	changefeedInfoClient[T]
	changefeedStateClient[T]
	scheduleClient[T]
	progressClient[T]
}

// TODO(CharlesCheung): only update changed fields to reduce the pressure on io and database.
type upstreamClient[T TxnContext] interface {
	createUpstream(tx T, up *UpstreamDO) error
	deleteUpstream(tx T, up *UpstreamDO) error
	updateUpstream(tx T, up *UpstreamDO) error
	queryUpstreams(tx T) ([]*UpstreamDO, error)
	queryUpstreamsByUpdateAt(tx T, lastUpdateAt time.Time) ([]*UpstreamDO, error)
	queryUpstreamByID(tx T, id uint64) (*UpstreamDO, error)
}

type changefeedInfoClient[T TxnContext] interface {
	createChangefeedInfo(tx T, info *ChangefeedInfoDO) error
	deleteChangefeedInfo(tx T, info *ChangefeedInfoDO) error
	markChangefeedRemoved(tx T, info *ChangefeedInfoDO) error
	updateChangefeedInfo(tx T, info *ChangefeedInfoDO) error
	queryChangefeedInfos(tx T) ([]*ChangefeedInfoDO, error)
	queryChangefeedInfosByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ChangefeedInfoDO, error)
	queryChangefeedInfosByUUIDs(tx T, uuids ...uint64) ([]*ChangefeedInfoDO, error)
	queryChangefeedInfoByUUID(tx T, uuid uint64) (*ChangefeedInfoDO, error)
}

type changefeedStateClient[T TxnContext] interface {
	createChangefeedState(tx T, state *ChangefeedStateDO) error
	deleteChangefeedState(tx T, state *ChangefeedStateDO) error
	updateChangefeedState(tx T, state *ChangefeedStateDO) error
	queryChangefeedStates(tx T) ([]*ChangefeedStateDO, error)
	queryChangefeedStatesByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ChangefeedStateDO, error)
	queryChangefeedStateByUUID(tx T, uuid uint64) (*ChangefeedStateDO, error)
	queryChangefeedStateByUUIDs(tx T, uuid ...uint64) ([]*ChangefeedStateDO, error)
	queryChangefeedStateByUUIDWithLock(tx T, uuid uint64) (*ChangefeedStateDO, error)
}

type scheduleClient[T TxnContext] interface {
	createSchedule(tx T, sc *ScheduleDO) error
	deleteSchedule(tx T, sc *ScheduleDO) error
	updateSchedule(tx T, sc *ScheduleDO) error
	updateScheduleOwnerState(tx T, sc *ScheduleDO) error
	updateScheduleOwnerStateByOwnerID(tx T, state metadata.SchedState, ownerID model.CaptureID) error
	querySchedules(tx T) ([]*ScheduleDO, error)
	querySchedulesByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ScheduleDO, error)
	querySchedulesByOwnerIDAndUpdateAt(tx T, captureID model.CaptureID, lastUpdateAt time.Time) ([]*ScheduleDO, error)
	queryScheduleByUUID(tx T, uuid uint64) (*ScheduleDO, error)
	querySchedulesUinqueOwnerIDs(tx T) ([]model.CaptureID, error)
}

type progressClient[T TxnContext] interface {
	createProgress(tx T, pr *ProgressDO) error
	deleteProgress(tx T, pr *ProgressDO) error
	updateProgress(tx T, pr *ProgressDO) error
	queryProgresss(tx T) ([]*ProgressDO, error)
	queryProgresssByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ProgressDO, error)
	queryProgressByCaptureID(tx T, id string) (*ProgressDO, error)
	queryProgressByCaptureIDsWithLock(tx T, ids []string) ([]*ProgressDO, error)
}

type clientOptions struct {
	maxExecTime time.Duration
}

func setClientOptions(opts ...ClientOptionFunc) *clientOptions {
	o := &clientOptions{
		maxExecTime: defaultMaxExecTime,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// ClientOptionFunc is the option function for the client.
type ClientOptionFunc func(*clientOptions)

// WithMaxExecTime sets the maximum execution time of the client.
func WithMaxExecTime(d time.Duration) ClientOptionFunc {
	return func(o *clientOptions) {
		o.maxExecTime = d
	}
}

// TODO(CharlesCheung): implement a cache layer to reduce the pressure on io and database.
// nolint:unused
type clientWithCache[T TxnContext] struct {
	c     client[T]
	cache *storage
}

type versionedRecord[K uint64 | string] interface {
	GetKey() K
	GetVersion() uint64
	GetUpdateAt() time.Time
}

type entity[K uint64 | string, V versionedRecord[K]] struct {
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

// get returns the value of the key.
func (e *entity[K, V]) get(key K) V {
	e.RLock()
	defer e.RUnlock()

	return e.m[key]
}

// doUpsert inserts or updates the entity upon the incoming data, and
// run the onchange callback if data is changed.
func (e *entity[K, V]) doUpsert(
	inComming []V,
	onchange func(newV V) (skip bool),
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
			if versionEqual != updateAtEqual {
				log.Panic("bad version and update_at", zap.Any("old", oldV), zap.Any("new", newV))
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

// upsert inserts or updates the entity with write lock.
// nolint:unused
func (e *entity[K, V]) upsert(inComming ...V) {
	e.doUpsert(inComming, nil)
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
