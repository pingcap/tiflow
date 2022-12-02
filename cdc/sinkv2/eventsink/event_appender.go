// Copyright 2022 PingCAP, Inc.
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
// limitations under the License

package eventsink

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// Appender is the interface for appending events to buffer.
type Appender[E TableEvent] interface {
	// Append appends the event to buffer.
	Append(buffer []E, rows ...*model.RowChangedEvent) []E
}

// Assert Appender[E TableEvent] implementation
var _ Appender[*model.RowChangedEvent] = (*RowChangeEventAppender)(nil)

// RowChangeEventAppender is the builder for RowChangedEvent.
type RowChangeEventAppender struct{}

// Append appends the given rows to the given buffer.
func (r *RowChangeEventAppender) Append(
	buffer []*model.RowChangedEvent,
	rows ...*model.RowChangedEvent,
) []*model.RowChangedEvent {
	return append(buffer, rows...)
}

// Assert Appender[E TableEvent] implementation
var _ Appender[*model.SingleTableTxn] = (*TxnEventAppender)(nil)

// TxnEventAppender is the appender for SingleTableTxn.
type TxnEventAppender struct{}

// Append appends the given rows to the given txn buffer.
// The callers of this function should **make sure** that
// the commitTs and startTs of rows is **strictly increasing**.
// 1. Txns ordered by commitTs and startTs.
// 2. Rows are grouped into SingleTableTxn by startTs and big txn batch,
// since the startTs is the unique identifier of a transaction.
// After Append, the structure of the buffer is:
// buffer = [Txn1[row11, row12...], Txn2[row21,row22...]...], in which:
//  1. If Txn1.CommitTs < Txn2.CommitTs, then Txn1.startTs can be
//     either less or larger than Txn2.startTs.
//  2. If Txn1.CommitTs == Txn2.CommitTs, then Txn1.startTs must be
//     **less than** Txn2.startTs.
func (t *TxnEventAppender) Append(
	buffer []*model.SingleTableTxn,
	rows ...*model.RowChangedEvent,
) []*model.SingleTableTxn {
	for _, row := range rows {
		// This means no txn is in the buffer.
		if len(buffer) == 0 {
			txn := &model.SingleTableTxn{
				StartTs:   row.StartTs,
				CommitTs:  row.CommitTs,
				Table:     row.Table,
				TableInfo: row.TableInfo,
			}
			txn.Append(row)
			buffer = append(buffer, txn)
			continue
		}

		lastTxn := buffer[len(buffer)-1]

		lastCommitTs := lastTxn.GetCommitTs()
		if lastCommitTs > row.CommitTs {
			log.Panic("The commitTs of the emit row is less than the received row",
				zap.Uint64("lastReceivedCommitTs", lastCommitTs),
				zap.Any("row", row))
		} else if lastCommitTs == row.CommitTs && lastTxn.StartTs > row.StartTs {
			log.Panic("The startTs of the emit row is less than the received row with same CommitTs",
				zap.Uint64("lastReceivedCommitTs", lastCommitTs),
				zap.Uint64("lastReceivedStartTs", lastTxn.StartTs),
				zap.Any("row", row))
		}

		// Split on big transactions or a new one. For 2 transactions,
		// their commitTs can be same but startTs will be never same.
		if row.SplitTxn || lastTxn.StartTs != row.StartTs {
			buffer = append(buffer, &model.SingleTableTxn{
				StartTs:   row.StartTs,
				CommitTs:  row.CommitTs,
				Table:     row.Table,
				TableInfo: row.TableInfo,
			})
		}

		buffer[len(buffer)-1].Append(row)
	}

	return buffer
}
