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

type UnresolvedTxnCache struct {
	unresolvedTxnsMu sync.Mutex
	unresolvedTxns   map[model.TableName][]*model.Txn
	checkpointTs     uint64
}

func NewUnresolvedTxnCache() *UnresolvedTxnCache {
	return &UnresolvedTxnCache{
		unresolvedTxns: make(map[model.TableName][]*model.Txn),
	}
}

func (c *UnresolvedTxnCache) Append(filter *filter.Filter, rows ...*model.RowChangedEvent) {
	c.unresolvedTxnsMu.Lock()
	defer c.unresolvedTxnsMu.Unlock()
	for _, row := range rows {
		if filter.ShouldIgnoreDMLEvent(row.StartTs, row.Table.Schema, row.Table.Table) {
			log.Info("Row changed event ignored", zap.Uint64("ts", row.CommitTs))
			continue
		}
		key := *row.Table
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
			txns = append(txns, &model.Txn{
				StartTs:  row.StartTs,
				CommitTs: row.CommitTs,
			})
			c.unresolvedTxns[key] = txns
		}
		txns[len(txns)-1].Append(row)
	}
}

func (c *UnresolvedTxnCache) Resolved(resolvedTs uint64) map[model.TableName][]*model.Txn {
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

func (c *UnresolvedTxnCache) Unresolved() map[model.TableName][]*model.Txn {
	return c.unresolvedTxns
}

func (c *UnresolvedTxnCache) UpdateCheckpoint(checkpointTs uint64) {
	atomic.StoreUint64(&c.checkpointTs, checkpointTs)
}

func splitResolvedTxn(
	resolvedTs uint64, unresolvedTxns map[model.TableName][]*model.Txn,
) (minTs uint64, resolvedRowsMap map[model.TableName][]*model.Txn) {
	resolvedRowsMap = make(map[model.TableName][]*model.Txn, len(unresolvedTxns))
	minTs = resolvedTs
	for key, txns := range unresolvedTxns {
		i := sort.Search(len(txns), func(i int) bool {
			return txns[i].CommitTs > resolvedTs
		})
		if i == 0 {
			continue
		}
		var resolvedTxns []*model.Txn
		if i == len(txns) {
			resolvedTxns = txns
			delete(unresolvedTxns, key)
		} else {
			resolvedTxns = txns[:i]
			unresolvedTxns[key] = txns[i:]
		}
		resolvedRowsMap[key] = resolvedTxns

		if len(resolvedTxns) > 0 && resolvedTxns[0].CommitTs < minTs {
			minTs = resolvedTxns[0].CommitTs
		}
	}
	return
}
