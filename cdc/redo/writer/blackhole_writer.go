// Copyright 2021 PingCAP, Inc.
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

package writer

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// blackholeWriter defines a blackhole storage, it receives events and persists
// without any latency
type blackholeWriter struct {
	tableRtsMap  map[model.TableID]uint64
	tableRtsMu   sync.RWMutex
	resolvedTs   uint64
	checkpointTs uint64
}

// NewBlackHoleWriter creates a blackhole writer
func NewBlackHoleWriter() *blackholeWriter {
	return &blackholeWriter{
		tableRtsMap: make(map[model.TableID]uint64),
	}
}

func (bs *blackholeWriter) WriteLog(ctx context.Context, tableID model.TableID, logs []*model.RedoRowChangedEvent) (resolvedTs uint64, err error) {
	bs.tableRtsMu.Lock()
	defer bs.tableRtsMu.Unlock()
	if len(logs) == 0 {
		return bs.tableRtsMap[tableID], nil
	}
	resolvedTs = bs.tableRtsMap[tableID]
	current := logs[len(logs)-1].Row.CommitTs
	bs.tableRtsMap[tableID] = current
	log.Debug("write row redo logs", zap.Int("count", len(logs)),
		zap.Uint64("resolvedTs", resolvedTs), zap.Uint64("current", current))
	return
}

func (bs *blackholeWriter) FlushLog(ctx context.Context, tableID model.TableID, resolvedTs uint64) error {
	bs.tableRtsMu.Lock()
	defer bs.tableRtsMu.Unlock()
	bs.tableRtsMap[tableID] = resolvedTs
	return nil
}

func (bs *blackholeWriter) SendDDL(ctx context.Context, ddl *model.RedoDDLEvent) error {
	log.Debug("send ddl event", zap.Any("ddl", ddl))
	return nil
}

func (bs *blackholeWriter) EmitResolvedTs(ctx context.Context, ts uint64) error {
	bs.resolvedTs = ts
	return nil
}

func (bs *blackholeWriter) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	bs.checkpointTs = ts
	return nil
}

func (bs *blackholeWriter) GetCurrentResolvedTs(ctx context.Context, tableIDs []int64) (map[int64]uint64, error) {
	bs.tableRtsMu.RLock()
	defer bs.tableRtsMu.RUnlock()
	rtsMap := make(map[int64]uint64, len(bs.tableRtsMap))
	for _, tableID := range tableIDs {
		rtsMap[tableID] = bs.tableRtsMap[tableID]
	}
	return rtsMap, nil
}

func (bs *blackholeWriter) Close() error {
	return nil
}
