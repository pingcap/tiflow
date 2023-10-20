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
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"gorm.io/gorm"
)

const defaultMaxExecTime = 5 * time.Second

type client[T TxnContext] interface {
	LeaderChecker[T]
	checker[T]
}

// TxnContext is a type set that can be used as the transaction context.
type TxnContext interface {
	*gorm.DB | *sql.Tx | *ormWrapper
}

// TxnAction is a series of operations that can be executed in a transaction and the
// generic type T represents the transaction context.
//
// Note that in the current implementation the metadata operation and leader check are
// always in the same transaction context.
type TxnAction[T TxnContext] func(T) error

// ormTxnAction represents a transaction action that uses gorm.DB as the transaction context.
type ormTxnAction = TxnAction[*gorm.DB]

type ormWrapperAction = TxnAction[*ormWrapper]

// sqlTxnAction represents a transaction action that uses sql.Tx as the transaction context.
// Note that sqlTxnAction is not implemented yet, it is reserved for future use.
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
	queryUpstreamsByIDs(tx T, ids ...uint64) ([]*UpstreamDO, error)
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
	queryProgresses(tx T) ([]*ProgressDO, error)
	queryProgressesByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ProgressDO, error)
	queryProgressByCaptureID(tx T, id string) (*ProgressDO, error)
	queryProgressByCaptureIDsWithLock(tx T, ids []string) ([]*ProgressDO, error)
}

type clientOptions struct {
	timeout            time.Duration
	cacheFlushInterval time.Duration
}

func setClientOptions(opts ...ClientOptionFunc) *clientOptions {
	o := &clientOptions{
		timeout: defaultMaxExecTime,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// ClientOptionFunc is the option function for the client.
type ClientOptionFunc func(*clientOptions)

// WithTimeout sets the maximum execution time of the client.
func WithTimeout(d time.Duration) ClientOptionFunc {
	return func(o *clientOptions) {
		o.timeout = d
	}
}

// WithCacheFlushInterval sets the query interval of the client.
func WithCacheFlushInterval(d time.Duration) ClientOptionFunc {
	return func(o *clientOptions) {
		o.cacheFlushInterval = d
	}
}

type clientWithTimeout[T TxnContext] struct {
	LeaderChecker[T]
	checker[T]

	timeout time.Duration
}

func newClientWithTimeout[T TxnContext](
	leaderChecker LeaderChecker[T],
	checker checker[T],
	timeout time.Duration,
) client[T] {
	return &clientWithTimeout[T]{
		LeaderChecker: leaderChecker,
		checker:       checker,
		timeout:       timeout,
	}
}

func (c *clientWithTimeout[T]) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return ctx, nil
	}
	return context.WithTimeout(ctx, c.timeout)
}

func (c *clientWithTimeout[T]) Txn(ctx context.Context, fn TxnAction[T]) error {
	ctx, cancel := c.withTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}
	return c.checker.Txn(ctx, fn)
}

func (c *clientWithTimeout[T]) TxnWithOwnerLock(ctx context.Context, uuid metadata.ChangefeedUUID, fn TxnAction[T]) error {
	ctx, cancel := c.withTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}
	return c.checker.TxnWithOwnerLock(ctx, uuid, fn)
}

func (c *clientWithTimeout[T]) TxnWithLeaderLock(ctx context.Context, leaderID string, fn func(T) error) error {
	ctx, cancel := c.withTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}
	return c.LeaderChecker.TxnWithLeaderLock(ctx, leaderID, fn)
}
