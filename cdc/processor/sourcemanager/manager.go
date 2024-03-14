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
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/kv/sharedconn"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	pullerwrapper "github.com/pingcap/tiflow/cdc/processor/sourcemanager/puller"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const defaultMaxBatchSize = 256

type pullerWrapperCreator func(
	changefeed model.ChangeFeedID,
	span tablepb.Span,
	tableName string,
	startTs model.Ts,
	bdrMode bool,
) pullerwrapper.Wrapper

type tablePullers struct {
	ctx     context.Context
	errChan chan error
	spanz.SyncMap
	pullerWrapperCreator pullerWrapperCreator
}

type multiplexingPuller struct {
	puller *pullerwrapper.MultiplexingWrapper
}

// SourceManager is the manager of the source engine and puller.
type SourceManager struct {
	ready chan struct{}

	// changefeedID is the changefeed ID.
	// We use it to create the puller and log.
	changefeedID model.ChangeFeedID
	// up is the upstream of the puller.
	up *upstream.Upstream
	// mg is the mounter group for mount the raw kv entry.
	mg entry.MounterGroup
	// engine is the source engine.
	engine sorter.SortEngine
	// Used to indicate whether the changefeed is in BDR mode.
	bdrMode bool

	// if `multiplexing` is true (the default value) then `multiplexingPuller` will be used.
	//  * tables in one changefeed will share grpc streams and region workers
	//  * tables in one changefeed will share goroutines
	multiplexing       bool
	enableTableMonitor bool
	tablePullers       tablePullers
	multiplexingPuller multiplexingPuller
}

// New creates a new source manager.
func New(
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	mg entry.MounterGroup,
	engine sorter.SortEngine,
	bdrMode bool,
	enableTableMonitor bool,
) *SourceManager {
	multiplexing := config.GetGlobalServerConfig().KVClient.EnableMultiplexing
	return newSourceManager(changefeedID, up, mg, engine, bdrMode, multiplexing, pullerwrapper.NewPullerWrapper, enableTableMonitor)
}

// NewForTest creates a new source manager for testing.
func NewForTest(
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	mg entry.MounterGroup,
	engine sorter.SortEngine,
	bdrMode bool,
) *SourceManager {
	return newSourceManager(changefeedID, up, mg, engine, bdrMode, false, pullerwrapper.NewPullerWrapperForTest, false)
}

func newSourceManager(
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	mg entry.MounterGroup,
	engine sorter.SortEngine,
	bdrMode bool,
	multiplexing bool,
	pullerWrapperCreator pullerWrapperCreator,
	enableTableMonitor bool,
) *SourceManager {
	mgr := &SourceManager{
		ready:              make(chan struct{}),
		changefeedID:       changefeedID,
		up:                 up,
		mg:                 mg,
		engine:             engine,
		bdrMode:            bdrMode,
		multiplexing:       multiplexing,
		enableTableMonitor: enableTableMonitor,
	}
	if !multiplexing {
		mgr.tablePullers.errChan = make(chan error, 16)
		mgr.tablePullers.pullerWrapperCreator = pullerWrapperCreator
	}
	return mgr
}

// AddTable adds a table to the source manager. Start puller and register table to the engine.
func (m *SourceManager) AddTable(span tablepb.Span, tableName string, startTs model.Ts) {
	// Add table to the engine first, so that the engine can receive the events from the puller.
	m.engine.AddTable(span, startTs)

	if m.multiplexing {
		m.multiplexingPuller.puller.Subscribe([]tablepb.Span{span}, startTs, tableName)
		return
	}

	p := m.tablePullers.pullerWrapperCreator(m.changefeedID, span, tableName, startTs, m.bdrMode)
	p.Start(m.tablePullers.ctx, m.up, m.engine, m.tablePullers.errChan, m.enableTableMonitor)
	m.tablePullers.Store(span, p)
}

// RemoveTable removes a table from the source manager. Stop puller and unregister table from the engine.
func (m *SourceManager) RemoveTable(span tablepb.Span) {
	if m.multiplexing {
		m.multiplexingPuller.puller.Unsubscribe([]tablepb.Span{span})
		m.engine.RemoveTable(span)
		return
	}

	if wrapper, ok := m.tablePullers.LoadAndDelete(span); ok {
		wrapper.(pullerwrapper.Wrapper).Close()
	}
	m.engine.RemoveTable(span)
}

// OnResolve just wrap the engine's OnResolve method.
func (m *SourceManager) OnResolve(action func(tablepb.Span, model.Ts)) {
	m.engine.OnResolve(action)
}

// FetchByTable just wrap the engine's FetchByTable method.
func (m *SourceManager) FetchByTable(
	span tablepb.Span, lowerBound, upperBound sorter.Position,
	quota *memquota.MemQuota,
) *sorter.MountedEventIter {
	iter := m.engine.FetchByTable(span, lowerBound, upperBound)
	return sorter.NewMountedEventIter(m.changefeedID, iter, m.mg, defaultMaxBatchSize, quota)
}

// CleanByTable just wrap the engine's CleanByTable method.
func (m *SourceManager) CleanByTable(span tablepb.Span, upperBound sorter.Position) error {
	return m.engine.CleanByTable(span, upperBound)
}

// GetTablePullerStats returns the puller stats of the table.
func (m *SourceManager) GetTablePullerStats(span tablepb.Span) puller.Stats {
	if m.multiplexing {
		return m.multiplexingPuller.puller.MultiplexingPuller.Stats(span)
	}

	p, ok := m.tablePullers.Load(span)
	if !ok {
		log.Panic("Table puller not found when getting table puller stats",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
	}
	return p.(pullerwrapper.Wrapper).GetStats()
}

// GetTableSorterStats returns the sorter stats of the table.
func (m *SourceManager) GetTableSorterStats(span tablepb.Span) sorter.TableStats {
	return m.engine.GetStatsByTable(span)
}

// Run implements util.Runnable.
func (m *SourceManager) Run(ctx context.Context, _ ...chan<- error) error {
	if m.multiplexing {
		serverConfig := config.GetGlobalServerConfig()
		grpcPool := sharedconn.NewConnAndClientPool(m.up.SecurityConfig, kv.GetGlobalGrpcMetrics())
		client := kv.NewSharedClient(
			m.changefeedID, serverConfig, m.bdrMode,
			m.up.PDClient, grpcPool, m.up.RegionCache, m.up.PDClock,
			txnutil.NewLockerResolver(m.up.KVStorage.(tikv.Storage), m.changefeedID),
		)
		m.multiplexingPuller.puller = pullerwrapper.NewMultiplexingPullerWrapper(
			m.changefeedID, client, m.engine,
			int(serverConfig.KVClient.FrontierConcurrent),
		)

		close(m.ready)
		return m.multiplexingPuller.puller.Run(ctx)
	}

	m.tablePullers.ctx = ctx
	close(m.ready)
	select {
	case err := <-m.tablePullers.errChan:
		return err
	case <-m.tablePullers.ctx.Done():
		return m.tablePullers.ctx.Err()
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
	m.tablePullers.Range(func(span tablepb.Span, value interface{}) bool {
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
