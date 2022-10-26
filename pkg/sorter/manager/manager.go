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
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sorter"
	ngpebble "github.com/pingcap/tiflow/pkg/sorter/pebble"
	"go.uber.org/multierr"
)

type sortEngineType int

const (
	// pebbleEngine details are in package document of pkg/sorter/pebble.
	pebbleEngine sortEngineType = iota + 1
)

// EventSortEngineManager is a manager to create or drop EventSortEngine.
type EventSortEngineManager struct {
	// Read-only fields.
	engineType      sortEngineType
	dir             string
	memQuotaInBytes uint64

	mu sync.Mutex

	engines map[model.ChangeFeedID]sorter.EventSortEngine

	// Following fields are valid if engineType is pebbleEngine.
	pebbleConfig *config.DBConfig
	dbs          []*pebble.DB
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
			f.dbs, err = ngpebble.OpenDBs(f.dir, f.pebbleConfig, f.memQuotaInBytes)
			if err != nil {
				return
			}
		}
		engine = ngpebble.New(ID, f.dbs)
		f.engines[ID] = engine
		return
	default:
		panic("not implemented")
	}
}

// Drop cleans the given event sort engine.
func (f *EventSortEngineManager) Drop(ID model.ChangeFeedID) (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	engine, exists := f.engines[ID]
	if !exists {
		return
	}
	delete(f.engines, ID)
	err = engine.Close()
	return
}

// Close will close all created engines and release all resources.
func (f *EventSortEngineManager) Close() (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	return &EventSortEngineManager{
		engineType:      pebbleEngine,
		memQuotaInBytes: memQuotaInBytes,
		pebbleConfig:    cfg,
	}
}
