// Copyright 2020 PingCAP, Inc.
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

package mysql

import (
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/container/queue"
	"go.uber.org/zap"
)

type txnsWithTheSameCommitTs struct {
	txns     []*model.SingleTableTxn
	commitTs model.Ts
}

func (t *txnsWithTheSameCommitTs) Append(row *model.RowChangedEvent) {
	if row.CommitTs != t.commitTs {
		log.Panic("unexpected row change event",
			zap.Uint64("commitTs", t.commitTs),
			zap.Any("row", row))
	}

	var txn *model.SingleTableTxn
	if len(t.txns) == 0 || row.SplitTxn || t.txns[len(t.txns)-1].StartTs < row.StartTs {
		txn = &model.SingleTableTxn{
			StartTs:   row.StartTs,
			CommitTs:  row.CommitTs,
			Table:     row.Table,
			TableInfo: row.TableInfo,
		}
		t.txns = append(t.txns, txn)
	} else if t.txns[len(t.txns)-1].StartTs == row.StartTs {
		txn = t.txns[len(t.txns)-1]
	} else {
		log.Panic("Row changed event received by the sink module should be ordered",
			zap.Any("previousTxn", t.txns[len(t.txns)-1]),
			zap.Any("currentRow", row))
	}
	txn.Append(row)
}

// unresolvedTxnCache caches unresolved txns
type unresolvedTxnCache struct {
	unresolvedTxnsMu sync.Mutex
	unresolvedTxns   map[model.TableID]*queue.ChunkQueue[*txnsWithTheSameCommitTs]
}

// newUnresolvedTxnCache returns a new unresolvedTxnCache
func newUnresolvedTxnCache() *unresolvedTxnCache {
	return &unresolvedTxnCache{
		unresolvedTxns: make(map[model.TableID]*queue.ChunkQueue[*txnsWithTheSameCommitTs]),
	}
}

// RemoveTableTxn removes  unresolved rows from cache
func (c *unresolvedTxnCache) RemoveTableTxn(tableID model.TableID) {
	c.unresolvedTxnsMu.Lock()
	defer c.unresolvedTxnsMu.Unlock()
	delete(c.unresolvedTxns, tableID)
}

// Append adds unresolved rows to cache
// the rows inputted into this function will go through the following handling logic
// 1. group by tableID from one input stream
// 2. for each tableID stream, the callers of this function should **make sure** that the CommitTs of rows is **strictly increasing**
// 3. group by CommitTs, according to CommitTs cut the rows into many group of rows in the same CommitTs
// 4. group by StartTs, cause the StartTs is the unique identifier of the transaction, according to StartTs cut the rows into many txns
func (c *unresolvedTxnCache) Append(rows ...*model.RowChangedEvent) int {
	c.unresolvedTxnsMu.Lock()
	defer c.unresolvedTxnsMu.Unlock()
	appendRows := 0
	for _, row := range rows {
		txns, ok := c.unresolvedTxns[row.Table.TableID]
		if !ok {
			txns = queue.NewChunkQueue[*txnsWithTheSameCommitTs]()
			c.unresolvedTxns[row.Table.TableID] = txns
		}

		lastTxn, ok := txns.Tail()
		if txns.Empty() || ok && lastTxn.commitTs != row.CommitTs {
			// fail-fast check
			if ok && lastTxn.commitTs > row.CommitTs {
				log.Panic("the commitTs of the emit row is less than the received row",
					zap.Uint64("lastReceivedCommitTs", lastTxn.commitTs),
					zap.Any("row", row))
			}
			txns.Push(&txnsWithTheSameCommitTs{
				commitTs: row.CommitTs,
			})
		}

		lastTxn, _ = txns.Tail()
		lastTxn.Append(row)
		appendRows++
	}
	return appendRows
}

// Resolved returns resolved txns according to resolvedTs
// The returned map contains many txns grouped by tableID. for each table, the each commitTs of txn in txns slice is strictly increasing
func (c *unresolvedTxnCache) Resolved(
	resolvedTsMap *sync.Map,
) (map[model.TableID]model.ResolvedTs, map[model.TableID][]*model.SingleTableTxn) {
	c.unresolvedTxnsMu.Lock()
	defer c.unresolvedTxnsMu.Unlock()

	return splitResolvedTxn(resolvedTsMap, c.unresolvedTxns)
}

func splitResolvedTxn(
	resolvedTsMap *sync.Map,
	unresolvedTxns map[model.TableID]*queue.ChunkQueue[*txnsWithTheSameCommitTs],
) (checkpointTsMap map[model.TableID]model.ResolvedTs,
	resolvedRowsMap map[model.TableID][]*model.SingleTableTxn,
) {
	checkpointTsMap = make(map[model.TableID]model.ResolvedTs, len(unresolvedTxns))
	resolvedTsMap.Range(func(k, v any) bool {
		tableID := k.(model.TableID)
		resolved := v.(model.ResolvedTs)
		checkpointTsMap[tableID] = resolved
		return true
	})

	resolvedRowsMap = make(map[model.TableID][]*model.SingleTableTxn, len(unresolvedTxns))
	resolvedTxnsBuf := queue.NewChunkQueue[*model.SingleTableTxn]()
	for tableID, resolved := range checkpointTsMap {
		txnQueue, ok := unresolvedTxns[tableID]
		if !ok || txnQueue.Empty() {
			continue
		}

		txnQueue.RangeAndPop(func(txns *txnsWithTheSameCommitTs) bool {
			if txns.commitTs <= resolved.Ts {
				resolvedTxnsBuf.PushMany(txns.txns...)
				return true
			}
			return false
		})
		if resolvedTxnsBuf.Empty() {
			continue
		}
		resolvedRowsMap[tableID] = resolvedTxnsBuf.PopAll()
		if txnQueue.Empty() {
			txnQueue.Shrink()
		}
	}
	return checkpointTsMap, resolvedRowsMap
}
