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

	"gorm.io/gorm"
)

// TxnAction is a series of operations that can be executed in a transaction and the
// generic type T represents the transaction context.
//
// Note that in the current implementation the metadata operation and leader check are
// always in the same transaction context. In the future, cross-database transactions
// could also be supported with different implementations.
type TxnAction[T any] func(T) error

// ormTxnAction represents a transaction action that uses gorm.DB as the transaction context.
type ormTxnAction = TxnAction[*gorm.DB]

// sqlTxnAction represents a transaction action that uses sql.Tx as the transaction context.
// Note that sqlTxnAction is not implemented yet, it is reserved for future use.
//
//nolint:unused
type sqlTxnAction = TxnAction[*sql.Tx]

// LeaderChecker enables the controller to ensure its leadership during a series of actions.
type LeaderChecker[T any] interface {
	TxnWithLeaderLock(ctx context.Context, leaderID string, fn TxnAction[T]) error
}

type Checker[T any] interface {
	Txn(ctx context.Context, fn TxnAction[T]) error
	TxnWithOwnerLock(ctx context.Context, uuid uint64, fn TxnAction[T]) error
}

// TODO(CharlesCheung): only update changed fields to reduce the pressure on io and database.
type upstreamClient[T any] interface {
	createUpstream(tx T, up *UpstreamDO) error
	deleteUpstream(tx T, up *UpstreamDO) error
	updateUpstream(tx T, up *UpstreamDO) error
	queryUpstreams(tx T) ([]*UpstreamDO, error)
	queryUpstreamsByUpdateAt(tx T, lastUpdateAt time.Time) ([]*UpstreamDO, error)
	queryUpstreamByID(tx T, id uint64) (*UpstreamDO, error)
}

type changefeedInfoClient[T any] interface {
	createChangefeedInfo(tx T, info *ChangefeedInfoDO) error
	deleteChangefeedInfo(tx T, info *ChangefeedInfoDO) error
	softDeleteChangefeedInfo(tx T, info *ChangefeedInfoDO) error
	updateChangefeedInfo(tx T, info *ChangefeedInfoDO) error
	queryChangefeedInfos(tx T) ([]*ChangefeedInfoDO, error)
	queryChangefeedInfosByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ChangefeedInfoDO, error)
	queryChangefeedInfoByUUID(tx T, uuid uint64) (*ChangefeedInfoDO, error)
}

type changefeedStateClient[T any] interface {
	createChangefeedState(tx T, state *ChangefeedStateDO) error
	deleteChangefeedState(tx T, state *ChangefeedStateDO) error
	updateChangefeedState(tx T, state *ChangefeedStateDO) error
	queryChangefeedStates(tx T) ([]*ChangefeedStateDO, error)
	queryChangefeedStatesByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ChangefeedStateDO, error)
	queryChangefeedStateByUUID(tx T, uuid uint64) (*ChangefeedStateDO, error)
}

type scheduleClient[T any] interface {
	createSchedule(tx T, sc *ScheduleDO) error
	deleteSchedule(tx T, sc *ScheduleDO) error
	updateSchedule(tx T, sc *ScheduleDO) error
	querySchedules(tx T) ([]*ScheduleDO, error)
	querySchedulesByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ScheduleDO, error)
	querySchedulesByOwnerIDAndUpdateAt(tx T, captureID string, lastUpdateAt time.Time) ([]*ScheduleDO, error)
	queryScheduleByUUID(tx T, uuid uint64) (*ScheduleDO, error)
}

type progressClient[T any] interface {
	createProgress(tx T, pr *ProgressDO) error
	deleteProgress(tx T, pr *ProgressDO) error
	updateProgress(tx T, pr *ProgressDO) error
	queryProgresss(tx T) ([]*ProgressDO, error)
	queryProgresssByUpdateAt(tx T, lastUpdateAt time.Time) ([]*ProgressDO, error)
	queryProgressByCaptureID(tx T, id string) (*ProgressDO, error)
	queryProgressByCaptureIDsWithLock(tx T, ids []string) ([]*ProgressDO, error)
}
