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

package factory

import (
	"fmt"
	"strconv"
	"sync"
	stdatomic "sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	epebble "github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/pebble"
	metrics "github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/pkg/config"
	dbMetrics "github.com/pingcap/tiflow/pkg/db"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
)

type sortEngineType int

const (
	// pebbleEngine details are in package document of pkg/sorter/pebble.
	pebbleEngine sortEngineType = iota + 1

	metricsCollectInterval = 15 * time.Second
)

var (
	// Use singleton to be compatible with test cases.
	factoryMu sync.Mutex
	factory   *SortEngineFactory = nil
)

// SortEngineFactory is a manager to create or drop SortEngine.
type SortEngineFactory struct {
	// Read-only fields.
	engineType      sortEngineType
	dir             string
	memQuotaInBytes uint64

	mu      sync.Mutex
	engines map[model.ChangeFeedID]engine.SortEngine

	wg     sync.WaitGroup
	closed chan struct{}

	// Following fields are valid if engineType is pebbleEngine.
	pebbleConfig *config.DBConfig
	dbs          []*pebble.DB
	writeStalls  []writeStall

	// dbs is also readed in the background metrics collector.
	dbInitialized *atomic.Bool
}

// Create creates a SortEngine. If an engine with same ID already exists,
// it will be returned directly.
func (f *SortEngineFactory) Create(ID model.ChangeFeedID) (e engine.SortEngine, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch f.engineType {
	case pebbleEngine:
		exists := false
		if e, exists = f.engines[ID]; exists {
			return e, nil
		}
		if len(f.dbs) == 0 {
			f.dbs, f.writeStalls, err = createPebbleDBs(f.dir, f.pebbleConfig, f.memQuotaInBytes)
			if err != nil {
				return
			}
			f.dbInitialized.Store(true)
		}
		e = epebble.New(ID, f.dbs)
		f.engines[ID] = e
	default:
		log.Panic("not implemented")
	}
	return
}

// Drop cleans the given event sort engine.
func (f *SortEngineFactory) Drop(ID model.ChangeFeedID) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	engine, exists := f.engines[ID]
	if !exists {
		return nil
	}
	delete(f.engines, ID)
	return engine.Close()
}

// Close will close all created engines and release all resources.
func (f *SortEngineFactory) Close() (err error) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	factory = nil

	f.mu.Lock()
	defer f.mu.Unlock()

	close(f.closed)
	f.wg.Wait()

	for _, engine := range f.engines {
		err = multierr.Append(err, engine.Close())
	}
	for _, db := range f.dbs {
		err = multierr.Append(err, db.Close())
	}
	return
}

// NewForPebble will create a SortEngineFactory for the pebble implementation.
func NewForPebble(dir string, memQuotaInBytes uint64, cfg *config.DBConfig) *SortEngineFactory {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	if factory == nil {
		factory = &SortEngineFactory{
			engineType:      pebbleEngine,
			dir:             dir,
			memQuotaInBytes: memQuotaInBytes,
			engines:         make(map[model.ChangeFeedID]engine.SortEngine),
			closed:          make(chan struct{}),
			pebbleConfig:    cfg,
			dbInitialized:   atomic.NewBool(false),
		}
		factory.startMetricsCollector()
	}
	return factory
}

func (f *SortEngineFactory) startMetricsCollector() {
	f.wg.Add(1)
	ticker := time.NewTicker(metricsCollectInterval)
	go func() {
		defer f.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-f.closed:
				return
			case <-ticker.C:
				f.collectMetrics()
			}
		}
	}()
}

func (f *SortEngineFactory) collectMetrics() {
	if f.engineType == pebbleEngine && f.dbInitialized.Load() {
		for i, db := range f.dbs {
			stats := db.Metrics()
			id := strconv.Itoa(i + 1)
			metrics.OnDiskDataSizeGauge.WithLabelValues(id).Set(float64(stats.DiskSpaceUsage()))
			metrics.InMemoryDataSizeGauge.WithLabelValues(id).Set(float64(stats.BlockCache.Size))
			dbMetrics.IteratorGauge().WithLabelValues(id).Set(float64(stats.TableIters))
			dbMetrics.WriteDelayCount().WithLabelValues(id).Set(float64(stdatomic.LoadUint64(&f.writeStalls[i].counter)))

			metricLevelCount := dbMetrics.LevelCount().MustCurryWith(map[string]string{"id": id})
			for level, metric := range stats.Levels {
				metricLevelCount.WithLabelValues(fmt.Sprint(level)).Set(float64(metric.NumFiles))
			}
			dbMetrics.BlockCacheAccess().WithLabelValues(id, "hit").Set(float64(stats.BlockCache.Hits))
			dbMetrics.BlockCacheAccess().WithLabelValues(id, "miss").Set(float64(stats.BlockCache.Misses))
		}
	}
}
