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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// ResolveTsTracker checks resolved event of spans and moves the global resolved ts ahead
type ResolveTsTracker interface {
	Forward(span util.Span, ts uint64) bool
	Frontier() uint64
}

// CollectRawTxns collects KV events from the inputFn,
// groups them by transactions and sends them to the outputFn.
func CollectRawTxns(
	ctx context.Context,
	inputFn func(context.Context) (model.KvOrResolved, error),
	outputFn func(context.Context, model.RawTxn) error,
	tracker ResolveTsTracker,
) error {
	entryGroups := make(map[uint64][]*model.RawKVEntry)
	for {
		be, err := inputFn(ctx)
		if err != nil {
			return errors.Trace(err)
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
			var readyTxns []model.RawTxn
			for ts, entries := range entryGroups {
				if ts <= resolvedTs {
					readyTxns = append(readyTxns, model.RawTxn{Ts: ts, Entries: entries})
					delete(entryGroups, ts)
				}
			}
			sort.Slice(readyTxns, func(i, j int) bool {
				return readyTxns[i].Ts < readyTxns[j].Ts
			})
			for _, t := range readyTxns {
				err := outputFn(ctx, t)
				if err != nil {
					return errors.Trace(err)
				}
			}
			if len(readyTxns) == 0 {
				log.Info("Forwarding fake txn", zap.Uint64("ts", resolvedTs))
				fakeTxn := model.RawTxn{
					Ts:      resolvedTs,
					Entries: nil,
				}
				err := outputFn(ctx, fakeTxn)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}
