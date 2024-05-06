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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	pullerwrapper "github.com/pingcap/tiflow/cdc/processor/sourcemanager/puller"
	"github.com/pingcap/tiflow/cdc/puller"
<<<<<<< HEAD
	cdccontext "github.com/pingcap/tiflow/pkg/context"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
=======
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
>>>>>>> c710066a51 (*(ticdc): split old update kv entry after restarting changefeed (#10919))
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
<<<<<<< HEAD
	bdrMode            bool
=======
	bdrMode bool
	startTs model.Ts

>>>>>>> c710066a51 (*(ticdc): split old update kv entry after restarting changefeed (#10919))
	enableTableMonitor bool
}

// New creates a new source manager.
func New(
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	mg entry.MounterGroup,
	engine engine.SortEngine,
	errChan chan error,
	bdrMode bool,
	enableTableMonitor bool,
	safeModeAtStart bool,
) *SourceManager {
<<<<<<< HEAD
	return &SourceManager{
=======
	return newSourceManager(changefeedID, up, mg, engine, bdrMode, enableTableMonitor, safeModeAtStart)
}

// NewForTest creates a new source manager for testing.
func NewForTest(
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	mg entry.MounterGroup,
	engine sorter.SortEngine,
	bdrMode bool,
) *SourceManager {
	return &SourceManager{
		ready:        make(chan struct{}),
		changefeedID: changefeedID,
		up:           up,
		mg:           mg,
		engine:       engine,
		bdrMode:      bdrMode,
	}
}

func isOldUpdateKVEntry(raw *model.RawKVEntry, thresholdTs model.Ts) bool {
	return raw != nil && raw.IsUpdate() && raw.CRTs < thresholdTs
}

func splitUpdateKVEntry(raw *model.RawKVEntry) (*model.RawKVEntry, *model.RawKVEntry, error) {
	if raw == nil {
		return nil, nil, errors.New("nil event cannot be split")
	}
	deleteKVEntry := *raw
	deleteKVEntry.Value = nil

	insertKVEntry := *raw
	insertKVEntry.OldValue = nil

	return &deleteKVEntry, &insertKVEntry, nil
}

func newSourceManager(
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	mg entry.MounterGroup,
	engine sorter.SortEngine,
	bdrMode bool,
	enableTableMonitor bool,
	safeModeAtStart bool,
) *SourceManager {
	mgr := &SourceManager{
		ready:              make(chan struct{}),
>>>>>>> c710066a51 (*(ticdc): split old update kv entry after restarting changefeed (#10919))
		changefeedID:       changefeedID,
		up:                 up,
		mg:                 mg,
		engine:             engine,
		errChan:            errChan,
		bdrMode:            bdrMode,
		enableTableMonitor: enableTableMonitor,
	}
<<<<<<< HEAD
=======

	serverConfig := config.GetGlobalServerConfig()
	grpcPool := sharedconn.NewConnAndClientPool(mgr.up.SecurityConfig, kv.GetGlobalGrpcMetrics())
	client := kv.NewSharedClient(
		mgr.changefeedID, serverConfig, mgr.bdrMode,
		mgr.up.PDClient, grpcPool, mgr.up.RegionCache, mgr.up.PDClock,
		txnutil.NewLockerResolver(mgr.up.KVStorage.(tikv.Storage), mgr.changefeedID),
	)

	// consume add raw kv entry to the engine.
	// It will be called by the puller when new raw kv entry is received.
	consume := func(ctx context.Context, raw *model.RawKVEntry, spans []tablepb.Span) error {
		if len(spans) > 1 {
			log.Panic("DML puller subscribes multiple spans",
				zap.String("namespace", mgr.changefeedID.Namespace),
				zap.String("changefeed", mgr.changefeedID.ID))
		}
		if raw != nil {
			if safeModeAtStart && isOldUpdateKVEntry(raw, mgr.startTs) {
				deleteKVEntry, insertKVEntry, err := splitUpdateKVEntry(raw)
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
>>>>>>> c710066a51 (*(ticdc): split old update kv entry after restarting changefeed (#10919))
}

// AddTable adds a table to the source manager. Start puller and register table to the engine.
func (m *SourceManager) AddTable(ctx cdccontext.Context, tableID model.TableID, tableName string, startTs model.Ts) {
	// Add table to the engine first, so that the engine can receive the events from the puller.
	m.engine.AddTable(tableID)
	p := pullerwrapper.NewPullerWrapper(m.changefeedID, tableID, tableName, startTs, m.bdrMode)
	p.Start(ctx, m.up, m.engine, m.errChan, m.enableTableMonitor)
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
<<<<<<< HEAD
func (m *SourceManager) GetTableSorterStats(tableID model.TableID) engine.TableStats {
	return m.engine.GetStatsByTable(tableID)
=======
func (m *SourceManager) GetTableSorterStats(span tablepb.Span) sorter.TableStats {
	return m.engine.GetStatsByTable(span)
}

// Run implements util.Runnable.
func (m *SourceManager) Run(ctx context.Context, _ ...chan<- error) error {
	close(m.ready)
	// Only nil in unit tests.
	if m.puller == nil {
		return nil
	}
	startTs, err := getCurrentTs(ctx, m.up.PDClient)
	if err != nil {
		return err
	}
	m.startTs = startTs
	return m.puller.Run(ctx)
}

// WaitForReady implements util.Runnable.
func (m *SourceManager) WaitForReady(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-m.ready:
	}
>>>>>>> c710066a51 (*(ticdc): split old update kv entry after restarting changefeed (#10919))
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

func getCurrentTs(ctx context.Context, pdClient pd.Client) (model.Ts, error) {
	backoffBaseDelayInMs := int64(100)
	totalRetryDuration := 10 * time.Second
	var replicateTs model.Ts
	err := retry.Do(ctx, func() error {
		phy, logic, err := pdClient.GetTS(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		replicateTs = oracle.ComposeTS(phy, logic)
		return nil
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs),
		retry.WithTotalRetryDuratoin(totalRetryDuration),
		retry.WithIsRetryableErr(cerrors.IsRetryableError))
	if err != nil {
		return model.Ts(0), errors.Trace(err)
	}
	return replicateTs, nil
}
