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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

// sqlTxnAction represents a transaction action that uses sql.DB as the transaction context.
// Note that sqlTxnAction is not implemented yet, it is reserved for future use.
type sqlTxnAction = TxnAction[*sql.DB]

// LeaderChecker enables the controller to ensure its leadership during a series of actions.
type LeaderChecker[T any] interface {
	TxnWithLeaderLock(ctx context.Context, leaderID string, fn TxnAction[T]) error
}

type Checker[T any] interface {
	Txn(ctx context.Context, fn TxnAction[T]) error
	TxnWithOwnerLock(ctx context.Context, uuid uint64, fn TxnAction[T]) error
}

var _ Checker[*gorm.DB] = &ormClient{}

type ormClient struct {
	selfID model.CaptureID
	db     *gorm.DB
}

// Txn executes the given transaction action in a transaction.
func (c *ormClient) Txn(ctx context.Context, fn ormTxnAction) error {
	return c.db.WithContext(ctx).Transaction(fn)
}

// TxnWithOwnerLock executes the given transaction action in a transaction with owner lock.
func (c *ormClient) TxnWithOwnerLock(ctx context.Context, uuid uint64, fn ormTxnAction) error {
	return c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var pr ProgressDO
		ret := tx.Select("owner").
			// TODO(charledCheung): use a variable to replace the hard-coded owner state.
			Where("changefeed_uuid = ? and owner = ? and owner_state != removed", uuid, c.selfID).
			Clauses(clause.Locking{
				Strength: "SHARE",
				Table:    clause.Table{Name: clause.CurrentTable},
			}).Limit(1).Find(&pr)
		if err := handleSingleOpErr(ret, 1, "TxnWithOwnerLock"); err != nil {
			return errors.Trace(err)
		}
		return fn(tx)
	})
}
