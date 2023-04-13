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

package dmlsink

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
type TxnEventAppender struct {
	// TableSinkStartTs is the startTs of the table sink.
	TableSinkStartTs model.Ts
	// IgnoreStartTs indicates whether to ignore the startTs of the row.
	// This is used by consumer to keep compatibility with the old version.
	// Most of our protocols are ignoring the startTs of the row, so we
	// can not use the startTs to identify a transaction.
	IgnoreStartTs bool
}

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
			txn := t.createSingleTableTxn(row)
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
		normalBoundary := row.SplitTxn || lastTxn.StartTs != row.StartTs
		// NOTICE: This is a special case for compatibility with old version.
		// In our lots of protocols, we are ignoring the startTs of the row,
		// so we can not use the startTs to identify a transaction.
		ignoreStartTsBoundary := t.IgnoreStartTs && lastCommitTs != row.CommitTs
		if normalBoundary || ignoreStartTsBoundary {
			buffer = append(buffer, t.createSingleTableTxn(row))
		}

		buffer[len(buffer)-1].Append(row)
	}

	return buffer
}

func (t *TxnEventAppender) createSingleTableTxn(
	row *model.RowChangedEvent,
) *model.SingleTableTxn {
	txn := &model.SingleTableTxn{
		StartTs:   row.StartTs,
		CommitTs:  row.CommitTs,
		Table:     row.Table,
		TableInfo: row.TableInfo,
	}
	if row.TableInfo != nil {
		txn.TableInfoVersion = row.TableInfo.Version
	}
	// If one table is just scheduled to a new processor, the txn.TableInfoVersion should be
	// greater than or equal to the startTs of table sink.
	if txn.TableInfoVersion < t.TableSinkStartTs {
		txn.TableInfoVersion = t.TableSinkStartTs
	}
	return txn
}
