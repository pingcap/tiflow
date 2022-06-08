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
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/driver"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/flags"
	"github.com/pingcap/tiflow/pkg/security"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"go.uber.org/zap"
)

type storeCache struct {
	sync.Mutex
	cache map[string]*Storage
}

var mc storeCache

func init() {
	mc.cache = make(map[string]*Storage)
}

const (
	closed = 1
)

// Storage wrap a tidbkv.Storage in it for reuse and maintain its status to
// avoid inappropriate call of tidbkv.Storage.Close()
type Storage struct {
	tiStore tidbkv.Storage
	uuid    string
	count   int32
	status  int32
}

// UnWrap return a tidbkv.Storage
func (s *Storage) UnWrap() tidbkv.Storage {
	return s.tiStore
}

// Close and unregister the store.
func (s *Storage) Close() error {
	if atomic.LoadInt32(&s.status) == closed {
		return nil
	}
	atomic.AddInt32(&s.count, -1)
	if s.count == 0 {
		atomic.StoreInt32(&s.status, closed)
		// if s.hc.count == 0
		// we need to unlock here to avoid dead lock
		mc.Lock()
		delete(mc.cache, s.uuid)
		defer mc.Unlock()
		log.Info("fizz delete", zap.String("id", s.uuid))
		return s.tiStore.Close()
	}
	return nil
}

// GetSnapshotMeta returns tidb meta information
// TODO: Simplify the signature of this function
func GetSnapshotMeta(tiStore tidbkv.Storage, ts uint64) (*meta.Meta, error) {
	snapshot := tiStore.GetSnapshot(tidbkv.NewVersion(ts))
	return meta.NewSnapshotMeta(snapshot), nil
}

// CreateTiStore returns a Storage
func CreateTiStore(urls string, credential *security.Credential) (*Storage, error) {
	urlv, err := flags.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	securityCfg := tikvconfig.Security{
		ClusterSSLCA:    credential.CAPath,
		ClusterSSLCert:  credential.CertPath,
		ClusterSSLKey:   credential.KeyPath,
		ClusterVerifyCN: credential.CertAllowedCN,
	}
	d := driver.TiKVDriver{}
	// Note: It will return a same storage if the the tiPath connect to a same pd cluster
	tiStore, err := d.OpenWithOptions(tiPath, driver.WithSecurity(securityCfg))
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrNewStore, err)
	}
	return wrapStore(tiStore), nil
}

// wrapStore wraps a tidbkv.Storage into Storage and return it
func wrapStore(tiStore tidbkv.Storage) *Storage {
	mc.Lock()
	defer mc.Unlock()
	uuid := tiStore.UUID()
	if s, ok := mc.cache[uuid]; ok {
		if atomic.LoadInt32(&s.status) == closed {
			goto outSide
		}
		atomic.AddInt32(&s.count, 1)
		return s
	}
outSide:
	res := &Storage{tiStore: tiStore, uuid: uuid}
	atomic.AddInt32(&res.count, 1)
	log.Info("fizz add", zap.String("id", res.uuid))
	mc.cache[uuid] = res
	return res
}
