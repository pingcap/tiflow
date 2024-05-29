// Copyright 2024 PingCAP, Inc.
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

package schema

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const DefaultGCInterval = 10 * time.Second

type SchemaManager struct {
	storageWrapperMap sync.Map
}

// Reset closes all schema storages and clear the map.
func (m *SchemaManager) Reset() {
	m.storageWrapperMap.Range(func(key, value interface{}) bool {
		w := value.(*storageWrapper)
		w.close(nil, "schema storage manager reset")
		return true
	})
	m.storageWrapperMap = sync.Map{}
}

// GetOrCreateSchemaStorage returns the schema storage and ddl puller for the specified changefeed.
func (m *SchemaManager) GetOrCreateSchemaStorage(
	cfg *StorageWrapperConfig,
) (p puller.DDLPuller, s entry.SchemaStorage, err error) {
	wrapper, _ := m.storageWrapperMap.LoadOrStore(cfg.ID, &storageWrapper{
		id: cfg.ID,
	})

	w := wrapper.(*storageWrapper)
	if err = w.register(cfg); err != nil {
		return nil, nil, err
	}
	return w.ddlPuller, w.storage, err
}

// ReleaseSchemaStorage closes the schema storage and ddl puller for the specified changefeed.
func (m *SchemaManager) ReleaseSchemaStorage(id model.ChangeFeedID, role util.Role) {
	wrapper, ok := m.storageWrapperMap.Load(id)
	if !ok {
		log.Warn("schema storage not found",
			zap.String("namespace", id.Namespace),
			zap.String("changefeed", id.ID))
		return
	}

	w := wrapper.(*storageWrapper)
	closed := w.deregister(role)
	if closed {
		m.storageWrapperMap.Delete(id)
	}
}

// UpdateGCTs updates the GC ts for the specified changefeed.
func (m *SchemaManager) UpdateGCTs(id model.ChangeFeedID, role util.Role, ts uint64) {
	wrapper, ok := m.storageWrapperMap.Load(id)
	if !ok {
		log.Warn("schema storage not found",
			zap.String("namespace", id.Namespace),
			zap.String("changefeed", id.ID))
		return
	}

	w := wrapper.(*storageWrapper)
	w.updateGCTs(role, ts)
}

type StorageWrapperConfig struct {
	ID         model.ChangeFeedID
	Role       util.Role
	ErrHandler func(error)
	GCInterval time.Duration

	ForceReplicate bool
	Filter         filter.Filter
	StartTs        uint64
	Upstream       *upstream.Upstream
}

type storageWrapper struct {
	id     model.ChangeFeedID
	wg     sync.WaitGroup
	cancel context.CancelFunc

	mu          sync.RWMutex
	initialized bool
	isClosed    bool

	// cancel releated context when the storage return error
	onErrorCallbacks map[util.Role]func(error)
	gcTsMap          sync.Map

	ddlPuller puller.DDLPuller
	storage   entry.SchemaStorage
}

func (w *storageWrapper) register(cfg *StorageWrapperConfig) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isClosed {
		return errors.New("register on closed storage wrapper, it is a temporal error, please resume the changefeed")
	}

	if !w.initialized {
		if err := w.init(cfg); err != nil {
			w.isClosed = true
			return err
		}
	}

	w.updateGCTs(cfg.Role, cfg.StartTs)
	w.onErrorCallbacks[cfg.Role] = cfg.ErrHandler
	return nil
}

func (w *storageWrapper) deregister(role util.Role) (closed bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.onErrorCallbacks, role)
	w.gcTsMap.Delete(role)

	// close the storage wrapper if all roles are deregistered
	if len(w.onErrorCallbacks) == 0 {
		w.close(nil, "storage wrapper closed since idle")
		return true
	}
	return false
}

// init must be called with lock
func (w *storageWrapper) init(cfg *StorageWrapperConfig) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.onErrorCallbacks = make(map[util.Role]func(error))

	w.storage, err = entry.NewSchemaStorage(cfg.Upstream.KVStorage, cfg.StartTs, cfg.ForceReplicate, cfg.ID, cfg.Role, cfg.Filter)
	if err != nil {
		return err
	}

	w.ddlPuller = puller.NewDDLPuller(cfg.Upstream, cfg.StartTs, cfg.ID, w.storage, cfg.Filter)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		defer w.ddlPuller.Close()
		err := w.ddlPuller.Run(ctx)
		if err != nil {
			w.close(err, "ddl puller meet error")
		}
	}()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		ticker := time.NewTicker(cfg.GCInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.doGC()
			}
		}
	}()

	w.initialized = true
	return nil
}

func (w *storageWrapper) close(err error, reason string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	log.Warn("schema wrapper closing",
		zap.String("namespace", w.id.Namespace),
		zap.String("changefeed", w.id.ID),
		zap.String("reason", reason),
		zap.Error(err))

	if err != nil {
		for _, cb := range w.onErrorCallbacks {
			cb(err)
		}
	}

	w.cancel()
	w.isClosed = true
	w.wg.Wait()
	log.Warn("schema wrapper closed",
		zap.String("namespace", w.id.Namespace),
		zap.String("changefeed", w.id.ID))
}

func (w *storageWrapper) updateGCTs(role util.Role, ts uint64) {
	old, ok := w.gcTsMap.Swap(role, ts)
	if ok && old.(uint64) > ts {
		log.Panic("gcTs regression in schema storage, please report a bug",
			zap.Uint64("old", old.(uint64)), zap.Uint64("new", ts))
	}
}

func (w *storageWrapper) doGC() (ok bool) {
	var gcTs uint64 = math.MaxUint64
	w.gcTsMap.Range(func(key, value interface{}) bool {
		ts := value.(uint64)
		if ts > gcTs {
			gcTs = ts
		}
		return true
	})

	if gcTs != math.MaxUint64 {
		w.storage.DoGC(gcTs)
		w.ddlPuller.DoGC(gcTs)
		return true
	}
	return false
}
