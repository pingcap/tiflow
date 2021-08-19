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

package redo

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// blackholeStorage defines a blockhole storage, it receives events and persists
// without any latency
type blackholeStorage struct {
	tableRtsMap  map[model.TableID]uint64
	tableRtsMu   sync.RWMutex
	resolvedTs   uint64
	checkpointTs uint64
}

func newBlackHoleStorage() *blackholeStorage {
	return &blackholeStorage{
		tableRtsMap: make(map[model.TableID]uint64),
	}
}

func (bs *blackholeStorage) WriteLog(ctx context.Context, tableID model.TableID, logs []*RowRedoLog) (resolvedTs uint64, err error) {
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

func (bs *blackholeStorage) FlushLog(ctx context.Context, tableID model.TableID, resolvedTs uint64) error {
	bs.tableRtsMu.Lock()
	defer bs.tableRtsMu.Unlock()
	bs.tableRtsMap[tableID] = resolvedTs
	return nil
}

func (bs *blackholeStorage) SendDDL(ctx context.Context, ddl *model.DDLEvent) error {
	log.Debug("send ddl event", zap.Any("ddl", ddl))
	return nil
}

func (bs *blackholeStorage) EmitResolvedTs(ctx context.Context, ts uint64) error {
	bs.resolvedTs = ts
	return nil
}

func (bs *blackholeStorage) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	bs.checkpointTs = ts
	return nil
}

func (bs *blackholeStorage) GetTableResolvedTs(ctx context.Context, tableIDs []int64) (map[int64]uint64, error) {
	bs.tableRtsMu.RLock()
	defer bs.tableRtsMu.RUnlock()
	rtsMap := make(map[int64]uint64, len(bs.tableRtsMap))
	for _, tableID := range tableIDs {
		rtsMap[tableID] = bs.tableRtsMap[tableID]
	}
	return rtsMap, nil
}

// BlackholeReader is a blockhole storage which implements LogReader interface
type BlackholeReader struct {
}

// NewBlackholeReader creates a new BlackholeReader
func NewBlackholeReader() *BlackholeReader {
	return &BlackholeReader{}
}

// ResetReader implements LogReader.ReadLog
func (br *BlackholeReader) ResetReader(ctx context.Context, startTs, endTs uint64) error {
	return nil
}

// ReadLog implements LogReader.ReadLog
func (br *BlackholeReader) ReadLog(ctx context.Context, maxNumberOfMessages int) ([]*RowRedoLog, error) {
	return nil, nil
}

// ReadDDL implements LogReader.ReadDDL
func (br *BlackholeReader) ReadDDL(ctx context.Context, maxNumberOfDDLs int) ([]*model.DDLEvent, error) {
	return nil, nil
}

// ReadMeta implements LogReader.ReadMeta
func (br *BlackholeReader) ReadMeta(ctx context.Context) (resolvedTs, checkpointTs uint64, err error) {
	return 1, 0, nil
}
