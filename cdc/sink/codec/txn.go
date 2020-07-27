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

package codec

import (
	"sort"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/cdc/model"
)

// TxnGenerator is used to collect queries into transactions
type TxnGenerator struct {
	UnresolvedTxns map[model.TableName][]*model.Txn
}

// NewTxnGenerator create a transaction generator
func NewTxnGenerator() *TxnGenerator {
	return &TxnGenerator{UnresolvedTxns: make(map[model.TableName][]*model.Txn)}
}

// Append a row change event
func (t *TxnGenerator) Append(row *model.RowChangedEvent) {
	key := *row.Table
	txns := t.UnresolvedTxns[key]
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
		t.UnresolvedTxns[key] = txns
	}
	txns[len(txns)-1].Append(row)
}

// SplitResolvedTxns split txns into resolvedTxns and unresolvedTxns base on resolvedTs
func (t *TxnGenerator) SplitResolvedTxns(resolvedTs uint64) (minTs uint64, resolvedRowsMap map[model.TableName][]*model.Txn) {
	return SplitResolvedTxn(resolvedTs, t.UnresolvedTxns)
}

// Empty show the TxnGenerator is empty
func (t *TxnGenerator) Empty() bool {
	return len(t.UnresolvedTxns) == 0
}

// SplitResolvedTxn split UnresolvedTxns into resolvedTxns and unresolvedTxns base on resolvedTs
func SplitResolvedTxn(
	resolvedTs uint64, UnresolvedTxns map[model.TableName][]*model.Txn,
) (minTs uint64, resolvedRowsMap map[model.TableName][]*model.Txn) {
	resolvedRowsMap = make(map[model.TableName][]*model.Txn, len(UnresolvedTxns))
	minTs = resolvedTs
	for key, txns := range UnresolvedTxns {
		i := sort.Search(len(txns), func(i int) bool {
			return txns[i].CommitTs > resolvedTs
		})
		if i == 0 {
			continue
		}
		var resolvedTxns []*model.Txn
		if i == len(txns) {
			resolvedTxns = txns
			delete(UnresolvedTxns, key)
		} else {
			resolvedTxns = txns[:i]
			UnresolvedTxns[key] = txns[i:]
		}
		resolvedRowsMap[key] = resolvedTxns

		if len(resolvedTxns) > 0 && resolvedTxns[0].CommitTs < minTs {
			minTs = resolvedTxns[0].CommitTs
		}
	}
	return
}
