// Copyright 2022 PingCAP, Inc.
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

package sourcemanager

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	pullerwrapper "github.com/pingcap/tiflow/cdc/processor/sourcemanager/puller"
	"github.com/pingcap/tiflow/cdc/puller"
	cdccontext "github.com/pingcap/tiflow/pkg/context"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
)

const defaultMaxBatchSize = 256

// SourceManager is the manager of the source engine and puller.
type SourceManager struct {
	// changefeedID is the changefeed ID.
	// We use it to create the puller and log.
	changefeedID model.ChangeFeedID
	// up is the upstream of the puller.
	up *upstream.Upstream
	// mg is the mounter group for mount the raw kv entry.
	mg entry.MounterGroup
	// engine is the source engine.
	engine engine.SortEngine
	// pullers is the puller wrapper map.
	pullers sync.Map
	// Used to report the error to the processor.
	errChan chan error
	// Used to indicate whether the changefeed is in BDR mode.
	bdrMode bool

	safeModeAtStart bool
}

// New creates a new source manager.
func New(
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	mg entry.MounterGroup,
	engine engine.SortEngine,
	errChan chan error,
	bdrMode bool,
	safeModeAtStart bool,
) *SourceManager {
	return &SourceManager{
		changefeedID:    changefeedID,
		up:              up,
		mg:              mg,
		engine:          engine,
		errChan:         errChan,
		bdrMode:         bdrMode,
		safeModeAtStart: safeModeAtStart,
	}
}

func isOldUpdateKVEntry(raw *model.RawKVEntry, getReplicaTs func() model.Ts) bool {
	return raw != nil && raw.IsUpdate() && raw.CRTs < getReplicaTs()
}

// AddTable adds a table to the source manager. Start puller and register table to the engine.
func (m *SourceManager) AddTable(ctx cdccontext.Context, tableID model.TableID, tableName string, startTs model.Ts, getReplicaTs func() model.Ts) {
	// Add table to the engine first, so that the engine can receive the events from the puller.
	m.engine.AddTable(tableID)
	shouldSplitKVEntry := func(raw *model.RawKVEntry) bool {
		return m.safeModeAtStart && isOldUpdateKVEntry(raw, getReplicaTs)
	}
	p := pullerwrapper.NewPullerWrapper(m.changefeedID, tableID, tableName, startTs, m.bdrMode, shouldSplitKVEntry)
	p.Start(ctx, m.up, m.engine, m.errChan)
	m.pullers.Store(tableID, p)
}

// RemoveTable removes a table from the source manager. Stop puller and unregister table from the engine.
func (m *SourceManager) RemoveTable(tableID model.TableID) {
	if wrapper, ok := m.pullers.Load(tableID); ok {
		wrapper.(*pullerwrapper.Wrapper).Close()
		m.pullers.Delete(tableID)
	}
	m.engine.RemoveTable(tableID)
}

// OnResolve just wrap the engine's OnResolve method.
func (m *SourceManager) OnResolve(action func(model.TableID, model.Ts)) {
	m.engine.OnResolve(action)
}

// FetchByTable just wrap the engine's FetchByTable method.
func (m *SourceManager) FetchByTable(
	tableID model.TableID, lowerBound, upperBound engine.Position,
	quota *memquota.MemQuota,
) *engine.MountedEventIter {
	iter := m.engine.FetchByTable(tableID, lowerBound, upperBound)
	return engine.NewMountedEventIter(m.changefeedID, iter, m.mg, defaultMaxBatchSize, quota)
}

// CleanByTable just wrap the engine's CleanByTable method.
func (m *SourceManager) CleanByTable(tableID model.TableID, upperBound engine.Position) error {
	return m.engine.CleanByTable(tableID, upperBound)
}

// GetTablePullerStats returns the puller stats of the table.
func (m *SourceManager) GetTablePullerStats(tableID model.TableID) puller.Stats {
	p, ok := m.pullers.Load(tableID)
	if !ok {
		log.Panic("Table puller not found when getting table puller stats",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}
	return p.(*pullerwrapper.Wrapper).GetStats()
}

// GetTableSorterStats returns the sorter stats of the table.
func (m *SourceManager) GetTableSorterStats(tableID model.TableID) engine.TableStats {
	return m.engine.GetStatsByTable(tableID)
}

// Close closes the source manager. Stop all pullers and close the engine.
func (m *SourceManager) Close() error {
	log.Info("Closing source manager",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID))
	start := time.Now()
	m.pullers.Range(func(key, value interface{}) bool {
		value.(*pullerwrapper.Wrapper).Close()
		return true
	})
	log.Info("All pullers have been closed",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Duration("cost", time.Since(start)))
	if err := m.engine.Close(); err != nil {
		return cerrors.Trace(err)
	}
	log.Info("Closed source manager",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Duration("cost", time.Since(start)))
	return nil
}
