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
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sort"

	"github.com/pingcap/ticdc/cdc/model"
)

// UnresolvedTxnCache caches unresolved txns, not thread safe
type UnresolvedTxnCache struct {
	unresolvedTxns map[model.TableID][]*model.SingleTableTxn
}

// NewUnresolvedTxnCache returns a new UnresolvedTxnCache
func NewUnresolvedTxnCache() *UnresolvedTxnCache {
	return &UnresolvedTxnCache{
		unresolvedTxns: make(map[model.TableID][]*model.SingleTableTxn),
	}
}

// Append adds unresolved rows to cache
func (c *UnresolvedTxnCache) Append(row *model.RowChangedEvent) {
	key := row.Table.TableID
	txns := c.unresolvedTxns[key]
	if len(txns) == 0 || txns[len(txns)-1].StartTs != row.StartTs {
		// fail-fast check
		if len(txns) != 0 && txns[len(txns)-1].CommitTs > row.CommitTs {
			log.Fatal("the commitTs of the emit row is less than the received row",
				zap.Stringer("table", row.Table),
				zap.Uint64("emit row startTs", row.StartTs),
				zap.Uint64("emit row commitTs", row.CommitTs),
				zap.Uint64("last received row startTs", txns[len(txns)-1].StartTs),
				zap.Uint64("last received row commitTs", txns[len(txns)-1].CommitTs))
		}
		txns = append(txns, &model.SingleTableTxn{
			StartTs:  row.StartTs,
			CommitTs: row.CommitTs,
			Table:    row.Table,
		})
		c.unresolvedTxns[key] = txns
	}
	txns[len(txns)-1].Append(row)
}

// Resolved returns resolved txns according to resolvedTs
func (c *UnresolvedTxnCache) Resolved(resolvedTs uint64) map[model.TableID][]*model.SingleTableTxn {
	if len(c.unresolvedTxns) == 0 {
		return nil
	}
	_, resolvedTxnsMap := splitResolvedTxn(resolvedTs, c.unresolvedTxns)
	return resolvedTxnsMap
}

// Unresolved returns unresolved txns
func (c *UnresolvedTxnCache) Unresolved() map[model.TableID][]*model.SingleTableTxn {
	return c.unresolvedTxns
}

func splitResolvedTxn(
	resolvedTs uint64, unresolvedTxns map[model.TableID][]*model.SingleTableTxn,
) (minTs uint64, resolvedRowsMap map[model.TableID][]*model.SingleTableTxn) {
	resolvedRowsMap = make(map[model.TableID][]*model.SingleTableTxn, len(unresolvedTxns))
	minTs = resolvedTs
	for tableID, txns := range unresolvedTxns {
		i := sort.Search(len(txns), func(i int) bool {
			return txns[i].CommitTs > resolvedTs
		})
		if i == 0 {
			continue
		}
		var resolvedTxns []*model.SingleTableTxn
		if i == len(txns) {
			resolvedTxns = txns
			delete(unresolvedTxns, tableID)
		} else {
			resolvedTxns = txns[:i]
			unresolvedTxns[tableID] = txns[i:]
		}
		resolvedRowsMap[tableID] = resolvedTxns

		if len(resolvedTxns) > 0 && resolvedTxns[0].CommitTs < minTs {
			minTs = resolvedTxns[0].CommitTs
		}
	}
	return
}
