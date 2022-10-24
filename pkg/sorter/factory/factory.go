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
	memoryEngine sortEngineType = iota + 1
	pebbleEngine
)

// EventSortEngineFactory is a factory to create EventSortEngine.
type EventSortEngineFactory struct {
	mu         sync.Mutex
	engineType sortEngineType

	dir             string
	memQuotaInBytes uint64

	// Following fields are valid if engineType is pebbleEngine.
	pebbleConfig *config.DBConfig
	dbs          []*pebble.DB
}

func (f *EventSortEngineFactory) Create(ID model.ChangeFeedID) (engine sorter.EventSortEngine, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch f.engineType {
	case pebbleEngine:
		if len(f.dbs) == 0 {
			f.dbs, err = ngpebble.OpenDBs(f.dir, f.pebbleConfig, f.memQuotaInBytes)
			if err != nil {
				return
			}
		}
		engine = ngpebble.New(ID, f.dbs)
		return
	default:
		panic("not implemented")
	}
}

func (f *EventSortEngineFactory) Close() (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, db := range f.dbs {
		err = multierr.Append(err, db.Close())
	}
	return
}

func NewPebbleFactory(dir string, memQuotaInBytes uint64, cfg *config.DBConfig) *EventSortEngineFactory {
	return &EventSortEngineFactory{
		engineType:      pebbleEngine,
		memQuotaInBytes: memQuotaInBytes,
		pebbleConfig:    cfg,
	}
}
