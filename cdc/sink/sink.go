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

package sink

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	EmitResolvedEvent(ctx context.Context, ts uint64) error
	EmitCheckpointEvent(ctx context.Context, ts uint64) error
	// EmitDMLs saves the specified DMLs to the sink backend
	EmitRowChangedEvent(ctx context.Context, txn ...*model.RowChangedEvent) error
	// EmitDDL saves the specified DDL to the sink backend
	EmitDDLEvent(ctx context.Context, txn *model.DDLEvent) error
	CheckpointTs() uint64
	Run(ctx context.Context) error
	// Close does not guarantee delivery of outstanding messages.
	Close() error
}

// NewBlackHoleSink creates a block hole sink
func NewBlackHoleSink() *blackHoleSink {
	return &blackHoleSink{}
}

type blackHoleSink struct {
	checkpointTs uint64
}

func (b *blackHoleSink) EmitResolvedEvent(ctx context.Context, ts uint64) error {
	log.Info("BlockHoleSink: Resolved Event", zap.Uint64("resolved ts", ts))
	return nil
}

func (b *blackHoleSink) EmitCheckpointEvent(ctx context.Context, ts uint64) error {
	log.Info("BlockHoleSink: Checkpoint Event", zap.Uint64("checkpoint ts", ts))
	return nil
}

func (b *blackHoleSink) EmitRowChangedEvent(ctx context.Context, rows ...*model.RowChangedEvent) error {
	for _, row := range rows {
		if row.Resolved {
			atomic.StoreUint64(&b.checkpointTs, row.Ts)
		}
		log.Info("BlockHoleSink: Row Changed Event", zap.Any("row", row))
	}
	return nil
}

func (b *blackHoleSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	log.Info("BlockHoleSink: DDL Event", zap.Any("ddl", ddl))
	return nil
}

func (b *blackHoleSink) CheckpointTs() uint64 {
	return atomic.LoadUint64(&b.checkpointTs)
}

func (b *blackHoleSink) Close() error {
	return nil
}
