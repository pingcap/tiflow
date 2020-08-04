// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"fmt"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/flags"
	"github.com/pingcap/ticdc/pkg/security"
	tidbconfig "github.com/pingcap/tidb/config"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
)

const (
	storageVersionCacheUpdateInterval = time.Second * 2
)

// StorageWithCurVersionCache adds GetCachedCurrentVersion() to tikv.Storage
type StorageWithCurVersionCache struct {
	tikv.Storage
	cacheKey string
}

type curVersionCacheEntry struct {
	ts          model.Ts
	lastUpdated time.Time
	mu          sync.Mutex
}

var (
	curVersionCache   = make(map[string]*curVersionCacheEntry, 1)
	curVersionCacheMu sync.Mutex
)

func newStorageWithCurVersionCache(storage tidbkv.Storage, cacheKey string) tidbkv.Storage {
	curVersionCacheMu.Lock()
	defer curVersionCacheMu.Unlock()

	if _, exists := curVersionCache[cacheKey]; !exists {
		curVersionCache[cacheKey] = &curVersionCacheEntry{
			ts:          0,
			lastUpdated: time.Unix(0, 0),
			mu:          sync.Mutex{},
		}
	}

	return &StorageWithCurVersionCache{
		Storage:  storage.(tikv.Storage),
		cacheKey: cacheKey,
	}
}

// GetCachedCurrentVersion gets the cached version of currentVersion, and update the cache if necessary
func (s *StorageWithCurVersionCache) GetCachedCurrentVersion() (version tidbkv.Version, err error) {
	curVersionCacheMu.Lock()
	entry, exists := curVersionCache[s.cacheKey]
	curVersionCacheMu.Unlock()

	if !exists {
		err = errors.New("GetCachedCurrentVersion: cache entry does not exist")
		log.Warn("GetCachedCurrentVersion: cache entry does not exist", zap.String("cacheKey", s.cacheKey))
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if time.Now().After(entry.lastUpdated.Add(storageVersionCacheUpdateInterval)) {
		var ver tidbkv.Version
		ver, err = s.CurrentVersion()
		if err != nil {
			return
		}
		entry.ts = ver.Ver
		entry.lastUpdated = time.Now()
	}

	version.Ver = entry.ts
	return
}

// GetSnapshotMeta returns tidb meta information
func GetSnapshotMeta(tiStore tidbkv.Storage, ts uint64) (*meta.Meta, error) {
	snapshot, err := tiStore.GetSnapshot(tidbkv.NewVersion(ts))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta.NewSnapshotMeta(snapshot), nil
}

// CreateTiStore creates a new tikv storage client
func CreateTiStore(urls string, credential *security.Credential) (tidbkv.Storage, error) {
	urlv, err := flags.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Ignore error if it is already registered.
	_ = store.Register("tikv", tikv.Driver{})

	if credential.CAPath != "" {
		conf := tidbconfig.GetGlobalConfig()
		conf.Security.ClusterSSLCA = credential.CAPath
		conf.Security.ClusterSSLCert = credential.CertPath
		conf.Security.ClusterSSLKey = credential.KeyPath
		tidbconfig.StoreGlobalConfig(conf)
	}

	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := store.New(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tiStore = newStorageWithCurVersionCache(tiStore, tiPath)
	return tiStore, nil
}
