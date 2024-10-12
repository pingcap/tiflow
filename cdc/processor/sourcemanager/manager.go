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
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	pullerwrapper "github.com/pingcap/tiflow/cdc/processor/sourcemanager/puller"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
)

const defaultMaxBatchSize = 256

// PullerSplitUpdateMode is the mode to split update events in puller.
type PullerSplitUpdateMode int32

// PullerSplitUpdateMode constants.
const (
	PullerSplitUpdateModeNone    PullerSplitUpdateMode = 0
	PullerSplitUpdateModeAtStart PullerSplitUpdateMode = 1
	PullerSplitUpdateModeAlways  PullerSplitUpdateMode = 2
)

// SourceManager is the manager of the source engine and puller.
type SourceManager struct {
	ctx   context.Context
	ready chan struct{}

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
	pullers spanz.SyncMap
	// Used to collect errors in running.
	errChan chan error
	// Used to indicate whether the changefeed is in BDR mode.
	bdrMode bool

	splitUpdateMode PullerSplitUpdateMode

	// pullerWrapperCreator is used to create a puller wrapper.
	// Only used for testing.
	pullerWrapperCreator func(changefeed model.ChangeFeedID,
		span tablepb.Span,
		tableName string,
		startTs model.Ts,
		bdrMode bool,
		shouldSplitKVEntry model.ShouldSplitKVEntry,
	) pullerwrapper.Wrapper
}

// New creates a new source manager.
func New(
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	mg entry.MounterGroup,
<<<<<<< HEAD
	engine engine.SortEngine,
	bdrMode bool,
	safeModeAtStart bool,
) *SourceManager {
	return &SourceManager{
		ready:                make(chan struct{}),
		changefeedID:         changefeedID,
		up:                   up,
		mg:                   mg,
		engine:               engine,
		errChan:              make(chan error, 16),
		bdrMode:              bdrMode,
		safeModeAtStart:      safeModeAtStart,
		pullerWrapperCreator: pullerwrapper.NewPullerWrapper,
	}
=======
	engine sorter.SortEngine,
	splitUpdateMode PullerSplitUpdateMode,
	bdrMode bool,
	enableTableMonitor bool,
) *SourceManager {
	return newSourceManager(changefeedID, up, mg, engine, splitUpdateMode, bdrMode, enableTableMonitor)
>>>>>>> f1d2ee62f8 (puller(ticdc): always split update kv entries in sink safe mode (#11224))
}

// NewForTest creates a new source manager for testing.
func NewForTest(
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	mg entry.MounterGroup,
	engine engine.SortEngine,
	bdrMode bool,
) *SourceManager {
	return &SourceManager{
		ready:                make(chan struct{}),
		changefeedID:         changefeedID,
		up:                   up,
		mg:                   mg,
		engine:               engine,
		errChan:              make(chan error, 16),
		bdrMode:              bdrMode,
		pullerWrapperCreator: pullerwrapper.NewPullerWrapperForTest,
	}
}

func isOldUpdateKVEntry(raw *model.RawKVEntry, getReplicaTs func() model.Ts) bool {
	return raw != nil && raw.IsUpdate() && raw.CRTs < getReplicaTs()
}

<<<<<<< HEAD
=======
func newSourceManager(
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	mg entry.MounterGroup,
	engine sorter.SortEngine,
	splitUpdateMode PullerSplitUpdateMode,
	bdrMode bool,
	enableTableMonitor bool,
) *SourceManager {
	mgr := &SourceManager{
		ready:              make(chan struct{}),
		changefeedID:       changefeedID,
		up:                 up,
		mg:                 mg,
		engine:             engine,
		splitUpdateMode:    splitUpdateMode,
		bdrMode:            bdrMode,
		enableTableMonitor: enableTableMonitor,
	}

	serverConfig := config.GetGlobalServerConfig()
	grpcPool := sharedconn.NewConnAndClientPool(mgr.up.SecurityConfig, kv.GetGlobalGrpcMetrics())
	client := kv.NewSharedClient(
		mgr.changefeedID, serverConfig, mgr.bdrMode,
		mgr.up.PDClient, grpcPool, mgr.up.RegionCache, mgr.up.PDClock,
		txnutil.NewLockerResolver(mgr.up.KVStorage.(tikv.Storage), mgr.changefeedID),
	)

	// consume add raw kv entry to the engine.
	// It will be called by the puller when new raw kv entry is received.
	consume := func(ctx context.Context, raw *model.RawKVEntry, spans []tablepb.Span, shouldSplitKVEntry model.ShouldSplitKVEntry) error {
		if len(spans) > 1 {
			log.Panic("DML puller subscribes multiple spans",
				zap.String("namespace", mgr.changefeedID.Namespace),
				zap.String("changefeed", mgr.changefeedID.ID))
		}
		if raw != nil {
			if shouldSplitKVEntry(raw) {
				deleteKVEntry, insertKVEntry, err := model.SplitUpdateKVEntry(raw)
				if err != nil {
					return err
				}
				deleteEvent := model.NewPolymorphicEvent(deleteKVEntry)
				insertEvent := model.NewPolymorphicEvent(insertKVEntry)
				mgr.engine.Add(spans[0], deleteEvent, insertEvent)
			} else {
				pEvent := model.NewPolymorphicEvent(raw)
				mgr.engine.Add(spans[0], pEvent)
			}
		}
		return nil
	}
	slots, hasher := mgr.engine.SlotsAndHasher()

	mgr.puller = puller.NewMultiplexingPuller(
		mgr.changefeedID,
		client,
		up.PDClock,
		consume,
		slots,
		hasher,
		int(serverConfig.KVClient.FrontierConcurrent))

	return mgr
}

>>>>>>> f1d2ee62f8 (puller(ticdc): always split update kv entries in sink safe mode (#11224))
// AddTable adds a table to the source manager. Start puller and register table to the engine.
func (m *SourceManager) AddTable(span tablepb.Span, tableName string, startTs model.Ts, getReplicaTs func() model.Ts) {
	// Add table to the engine first, so that the engine can receive the events from the puller.
	m.engine.AddTable(span, startTs)
	shouldSplitKVEntry := func(raw *model.RawKVEntry) bool {
		if raw == nil || !raw.IsUpdate() {
			return false
		}
		switch m.splitUpdateMode {
		case PullerSplitUpdateModeNone:
			return false
		case PullerSplitUpdateModeAlways:
			return true
		case PullerSplitUpdateModeAtStart:
			return isOldUpdateKVEntry(raw, getReplicaTs)
		default:
			log.Panic("Unknown split update mode", zap.Int32("mode", int32(m.splitUpdateMode)))
		}
		log.Panic("Shouldn't reach here")
		return false
	}
	p := m.pullerWrapperCreator(m.changefeedID, span, tableName, startTs, m.bdrMode, shouldSplitKVEntry)
	p.Start(m.ctx, m.up, m.engine, m.errChan)
	m.pullers.Store(span, p)
}

// RemoveTable removes a table from the source manager. Stop puller and unregister table from the engine.
func (m *SourceManager) RemoveTable(span tablepb.Span) {
	if wrapper, ok := m.pullers.Load(span); ok {
		wrapper.(pullerwrapper.Wrapper).Close()
		m.pullers.Delete(span)
	}
	m.engine.RemoveTable(span)
}

// OnResolve just wrap the engine's OnResolve method.
func (m *SourceManager) OnResolve(action func(tablepb.Span, model.Ts)) {
	m.engine.OnResolve(action)
}

// FetchByTable just wrap the engine's FetchByTable method.
func (m *SourceManager) FetchByTable(
	span tablepb.Span, lowerBound, upperBound engine.Position,
	quota *memquota.MemQuota,
) *engine.MountedEventIter {
	iter := m.engine.FetchByTable(span, lowerBound, upperBound)
	return engine.NewMountedEventIter(m.changefeedID, iter, m.mg, defaultMaxBatchSize, quota)
}

// CleanByTable just wrap the engine's CleanByTable method.
func (m *SourceManager) CleanByTable(span tablepb.Span, upperBound engine.Position) error {
	return m.engine.CleanByTable(span, upperBound)
}

// GetTablePullerStats returns the puller stats of the table.
func (m *SourceManager) GetTablePullerStats(span tablepb.Span) puller.Stats {
	p, ok := m.pullers.Load(span)
	if !ok {
		log.Panic("Table puller not found when getting table puller stats",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
	}
	return p.(pullerwrapper.Wrapper).GetStats()
}

// GetTableSorterStats returns the sorter stats of the table.
func (m *SourceManager) GetTableSorterStats(span tablepb.Span) engine.TableStats {
	return m.engine.GetStatsByTable(span)
}

// Run implements util.Runnable.
func (m *SourceManager) Run(ctx context.Context, _ ...chan<- error) error {
	m.ctx = ctx
	close(m.ready)
	select {
	case err := <-m.errChan:
		return err
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

// WaitForReady implements util.Runnable.
func (m *SourceManager) WaitForReady(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-m.ready:
	}
}

// Close closes the source manager. Stop all pullers and close the engine.
// It also implements util.Runnable.
func (m *SourceManager) Close() {
	log.Info("Closing source manager",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID))
	start := time.Now()
	m.pullers.Range(func(span tablepb.Span, value interface{}) bool {
		value.(pullerwrapper.Wrapper).Close()
		return true
	})
	log.Info("All pullers have been closed",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Duration("cost", time.Since(start)))
	if err := m.engine.Close(); err != nil {
		log.Panic("Fail to close sort engine",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Error(err))
	}
	log.Info("Closed source manager",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Duration("cost", time.Since(start)))
}

// Add adds events to the engine. It is used for testing.
func (m *SourceManager) Add(span tablepb.Span, events ...*model.PolymorphicEvent) {
	m.engine.Add(span, events...)
}
