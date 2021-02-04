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

package common

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/filter"
	"go.uber.org/zap"
)

type txnsWithTheSameCommitTs struct {
	txns     map[model.Ts]*model.SingleTableTxn
	commitTs model.Ts
}

func (t *txnsWithTheSameCommitTs) Append(row *model.RowChangedEvent) {
	if row.CommitTs != t.commitTs {
		log.Panic("unexpected row change event",
			zap.Uint64("commitTs of txn", t.commitTs),
			zap.Any("row", row))
	}
	if t.txns == nil {
		t.txns = make(map[model.Ts]*model.SingleTableTxn)
	}
	txn, exist := t.txns[row.StartTs]
	if !exist {
		txn = &model.SingleTableTxn{
			StartTs:  row.StartTs,
			CommitTs: row.CommitTs,
			Table:    row.Table,
		}
		t.txns[row.StartTs] = txn
	}
	txn.Append(row)
}

// UnresolvedTxnCache caches unresolved txns
type UnresolvedTxnCache struct {
	unresolvedTxnsMu sync.Mutex
	unresolvedTxns   map[model.TableID][]*txnsWithTheSameCommitTs
	checkpointTs     uint64
}

// NewUnresolvedTxnCache returns a new UnresolvedTxnCache
func NewUnresolvedTxnCache() *UnresolvedTxnCache {
	return &UnresolvedTxnCache{
		unresolvedTxns: make(map[model.TableID][]*txnsWithTheSameCommitTs),
	}
}

// Append adds unresolved rows to cache
// the rows inputed into this function will go through the following handling logic
// 1. group by tableID from one input stream
// 2. for each tableID stream, the callers of this function should **make sure** that the CommitTs of rows is **strictly increasing**
// 3. group by CommitTs, according to CommitTs cut the rows into many group of rows in the same CommitTs
// 4. group by StartTs, cause the StartTs is the unique identifier of the transaction, according to StartTs cut the rows into many txns
func (c *UnresolvedTxnCache) Append(filter *filter.Filter, rows ...*model.RowChangedEvent) int {
	c.unresolvedTxnsMu.Lock()
	defer c.unresolvedTxnsMu.Unlock()
	appendRows := 0
	for _, row := range rows {
		if filter != nil && filter.ShouldIgnoreDMLEvent(row.StartTs, row.Table.Schema, row.Table.Table) {
			log.Info("Row changed event ignored", zap.Uint64("start-ts", row.StartTs))
			continue
		}
		txns := c.unresolvedTxns[row.Table.TableID]
		if len(txns) == 0 || txns[len(txns)-1].commitTs != row.CommitTs {
			// fail-fast check
			if len(txns) != 0 && txns[len(txns)-1].commitTs > row.CommitTs {
				log.Panic("the commitTs of the emit row is less than the received row",
					zap.Stringer("table", row.Table),
					zap.Uint64("emit row startTs", row.StartTs),
					zap.Uint64("emit row commitTs", row.CommitTs),
					zap.Uint64("last received row commitTs", txns[len(txns)-1].commitTs))
			}
			txns = append(txns, &txnsWithTheSameCommitTs{
				commitTs: row.CommitTs,
			})
			c.unresolvedTxns[row.Table.TableID] = txns
		}
		txns[len(txns)-1].Append(row)
		appendRows++
	}
	return appendRows
}

// Resolved returns resolved txns according to resolvedTs
// The returned map contains many txns grouped by tableID. for each table, the each commitTs of txn in txns slice is strictly increasing
func (c *UnresolvedTxnCache) Resolved(resolvedTs uint64) map[model.TableID][]*model.SingleTableTxn {
	if resolvedTs <= atomic.LoadUint64(&c.checkpointTs) {
		return nil
	}

	c.unresolvedTxnsMu.Lock()
	defer c.unresolvedTxnsMu.Unlock()
	if len(c.unresolvedTxns) == 0 {
		return nil
	}

	_, resolvedTxnsMap := splitResolvedTxn(resolvedTs, c.unresolvedTxns)
	return resolvedTxnsMap
}

// UpdateCheckpoint updates the checkpoint ts
func (c *UnresolvedTxnCache) UpdateCheckpoint(checkpointTs uint64) {
	atomic.StoreUint64(&c.checkpointTs, checkpointTs)
}

func splitResolvedTxn(
	resolvedTs uint64, unresolvedTxns map[model.TableID][]*txnsWithTheSameCommitTs,
) (minTs uint64, resolvedRowsMap map[model.TableID][]*model.SingleTableTxn) {
	resolvedRowsMap = make(map[model.TableID][]*model.SingleTableTxn, len(unresolvedTxns))
	minTs = resolvedTs
	for tableID, txns := range unresolvedTxns {
		i := sort.Search(len(txns), func(i int) bool {
			return txns[i].commitTs > resolvedTs
		})
		if i == 0 {
			continue
		}
		var resolvedTxnsWithTheSameCommitTs []*txnsWithTheSameCommitTs
		if i == len(txns) {
			resolvedTxnsWithTheSameCommitTs = txns
			delete(unresolvedTxns, tableID)
		} else {
			resolvedTxnsWithTheSameCommitTs = txns[:i]
			unresolvedTxns[tableID] = txns[i:]
		}
		var txnsLength int
		for _, txns := range resolvedTxnsWithTheSameCommitTs {
			txnsLength += len(txns.txns)
		}
		resolvedTxns := make([]*model.SingleTableTxn, 0, txnsLength)
		for _, txns := range resolvedTxnsWithTheSameCommitTs {
			for _, txn := range txns.txns {
				resolvedTxns = append(resolvedTxns, txn)
			}
		}
		resolvedRowsMap[tableID] = resolvedTxns
		if len(resolvedTxnsWithTheSameCommitTs) > 0 && resolvedTxnsWithTheSameCommitTs[0].commitTs < minTs {
			minTs = resolvedTxnsWithTheSameCommitTs[0].commitTs
		}
	}
	return
}
