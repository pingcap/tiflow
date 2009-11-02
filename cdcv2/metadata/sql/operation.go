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

	"gorm.io/gorm"
)

// TxnAction is a series of operations that can be executed in a transaction and the
// generic type T represents the transaction context.
//
// Note that in the current implementation the metadata operation and leader check are
// always in the same transaction context. In the future, cross-database transactions
// could also be supported with different implementations.
type TxnAction[T any] func(T) error

type sqlTxnAction TxnAction[*gorm.DB]

// LeaderChecker enables the controller to ensure its leadership during a series of actions.
type LeaderChecker[T any] interface {
	TxnWithLeaderLock(ctx context.Context, leaderID string, fn TxnAction[T]) error
}

type ControllerClient interface {
}

// =========================== Capture ===========================
// CaptureOb is an implement for metadata.CaptureObservation.
// type CaptureOb struct {
// 	// election related fields.
// 	metadata.Elector
// 	selfInfo *model.CaptureInfo

// 	storage *meatStorageClient

// 	tasks struct {
// 		sync.RWMutex
// 		owners     sortedScheduledChangefeeds
// 		processors sortedScheduledChangefeeds
// 	}

// 	ownerChanges     *chann.DrainableChann[metadata.ScheduledChangefeed]
// 	processorChanges *chann.DrainableChann[metadata.ScheduledChangefeed]
// }

// func (s *meatStorageClient) test() {
// 	s.db.Transaction()
// }

// =========================== Controller ===========================

// =========================== Owner ===========================
