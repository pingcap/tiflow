// Copyright 2019 PingCAP, Inc.
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

package txn

import (
	"context"
	"sort"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

// RawTxn represents a complete collection of Entries that belong to the same transaction
type RawTxn struct {
	Ts      uint64
	Entries []*kv.RawKVEntry
}

// DMLType represents the dml type
type DMLType int

// DMLType types
const (
	UnknownDMLType DMLType = iota
	InsertDMLType
	UpdateDMLType
	DeleteDMLType
)

// DML holds the dml info
type DML struct {
	Database string
	Table    string
	Tp       DMLType
	Values   map[string]types.Datum
	// only set when Tp = UpdateDMLType
	OldValues map[string]types.Datum
}

// TableName returns the fully qualified name of the DML's table
func (dml *DML) TableName() string {
	return util.QuoteSchema(dml.Database, dml.Table)
}

// DDL holds the ddl info
type DDL struct {
	Database string
	Table    string
	Job      *model.Job
}

// Txn holds transaction info, an DDL or DML sequences
type Txn struct {
	// TODO: Group changes by tables to improve efficiency
	DMLs []*DML
	DDL  *DDL

	Ts uint64
}

func (t Txn) IsDDL() bool {
	return t.DDL != nil
}

type ResolveTsTracker interface {
	Forward(span util.Span, ts uint64) bool
	Frontier() uint64
}

func CollectRawTxns(
	ctx context.Context,
	inputFn func(context.Context) (kv.KvOrResolved, error),
	outputFn func(context.Context, RawTxn) error,
	tracker ResolveTsTracker,
) error {
	entryGroups := make(map[uint64][]*kv.RawKVEntry)
	for {
		be, err := inputFn(ctx)
		if err != nil {
			return err
		}
		if be.KV != nil {
			entryGroups[be.KV.Ts] = append(entryGroups[be.KV.Ts], be.KV)
		} else if be.Resolved != nil {
			resolvedTs := be.Resolved.Timestamp
			// 1. Forward is called in a single thread
			// 2. The only way the global minimum resolved Ts can be forwarded is that
			// 	  the resolveTs we pass in replaces the original one
			// Thus, we can just use resolvedTs here as the new global minimum resolved Ts.
			forwarded := tracker.Forward(be.Resolved.Span, resolvedTs)
			if !forwarded {
				continue
			}
			var readyTxns []RawTxn
			for ts, entries := range entryGroups {
				if ts <= resolvedTs {
					readyTxns = append(readyTxns, RawTxn{ts, entries})
					delete(entryGroups, ts)
				}
			}
			sort.Slice(readyTxns, func(i, j int) bool {
				return readyTxns[i].Ts < readyTxns[j].Ts
			})
			for _, t := range readyTxns {
				err := outputFn(ctx, t)
				if err != nil {
					return err
				}
			}
			if len(readyTxns) == 0 {
				log.Info("Forwarding fake txn", zap.Uint64("ts", resolvedTs))
				fakeTxn := RawTxn{
					Ts:      resolvedTs,
					Entries: nil,
				}
				outputFn(ctx, fakeTxn)
			}
		}
	}
}
