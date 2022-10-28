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

package manager

import (
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	metrics "github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sorter"
	ngpebble "github.com/pingcap/tiflow/pkg/sorter/pebble"
	"go.uber.org/multierr"
)

type sortEngineType int

const (
	// pebbleEngine details are in package document of pkg/sorter/pebble.
	pebbleEngine sortEngineType = iota + 1

	metricsCollectInterval time.Duration = 15 * time.Second
)

// EventSortEngineManager is a manager to create or drop EventSortEngine.
type EventSortEngineManager struct {
	// Read-only fields.
	engineType      sortEngineType
	dir             string
	memQuotaInBytes uint64

	mu      sync.Mutex
	engines map[model.ChangeFeedID]sorter.EventSortEngine

	wg     sync.WaitGroup
	closed chan struct{}

	// Following fields are valid if engineType is pebbleEngine.
	pebbleConfig *config.DBConfig
	dbs          []*pebble.DB
	writeStalls  []writeStall
}

// Create creates an EventSortEngine. If an engine with same ID already exists,
// it will be returned directly.
func (f *EventSortEngineManager) Create(ID model.ChangeFeedID) (engine sorter.EventSortEngine, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch f.engineType {
	case pebbleEngine:
		exists := false
		if engine, exists = f.engines[ID]; exists {
			return engine, nil
		}
		if len(f.dbs) == 0 {
			f.dbs, f.writeStalls, err = createPebbleDBs(f.dir, f.pebbleConfig, f.memQuotaInBytes)
			if err != nil {
				return
			}
		}
		engine = ngpebble.New(ID, f.dbs)
		f.engines[ID] = engine
	default:
		log.Panic("not implemented")
	}
	return
}

// Drop cleans the given event sort engine.
func (f *EventSortEngineManager) Drop(ID model.ChangeFeedID) error {
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
func (f *EventSortEngineManager) Close() (err error) {
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

// NewForPebble will create a EventSortEngineManager for the pebble implementation.
func NewForPebble(dir string, memQuotaInBytes uint64, cfg *config.DBConfig) *EventSortEngineManager {
	manager := &EventSortEngineManager{
		engineType:      pebbleEngine,
		memQuotaInBytes: memQuotaInBytes,
		pebbleConfig:    cfg,
	}

	manager.startMetricsCollector()
	return manager
}

func (f *EventSortEngineManager) startMetricsCollector() {
	f.wg.Add(1)
	ticker := time.NewTicker(metricsCollectInterval)
	go func() {
		defer f.wg.Done()
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

func (f *EventSortEngineManager) collectMetrics() {
	if f.engineType == pebbleEngine {
		for i, db := range f.dbs {
			stats := db.Metrics()
			id := strconv.Itoa(i + 1)
			metrics.OnDiskDataSizeGauge.WithLabelValues(id).Set(float64(stats.DiskSpaceUsage()))
			metrics.InMemoryDataSizeGauge.WithLabelValues(id).Set(float64(stats.BlockCache.Size))
			// TODO(qupeng): add more metrics about db.
		}
	}
}
