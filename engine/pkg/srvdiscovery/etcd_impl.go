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

package srvdiscovery

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
)

const defaultWatchChanSize = 8

// EtcdSrvDiscovery implements Discovery interface based on etcd as backend storage
// Note this struct is not thread-safe, and can be watched only once.
type EtcdSrvDiscovery struct {
	keyAdapter   adapter.KeyAdapter
	etcdCli      *clientv3.Client
	snapshot     Snapshot
	watchTickDur time.Duration
	watchCancel  context.CancelFunc
	watched      *atomic.Bool
}

// NewEtcdSrvDiscovery creates a new EtcdSrvDiscovery
func NewEtcdSrvDiscovery(etcdCli *clientv3.Client, ka adapter.KeyAdapter, watchTickDur time.Duration) *EtcdSrvDiscovery {
	return &EtcdSrvDiscovery{
		keyAdapter:   ka,
		etcdCli:      etcdCli,
		watchTickDur: watchTickDur,
		watched:      atomic.NewBool(false),
		snapshot:     make(map[UUID]ServiceResource),
	}
}

// Snapshot implements Discovery.Snapshot
func (d *EtcdSrvDiscovery) Snapshot(ctx context.Context) (Snapshot, error) {
	snapshot, err := d.getSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	d.CopySnapshot(snapshot)
	return snapshot, nil
}

// Watch implements Discovery.Watch, note when `Watch` starts, we should avoid
// to get Snapshot with `updateCache=true`
func (d *EtcdSrvDiscovery) Watch(ctx context.Context) <-chan WatchResp {
	notWatched := d.watched.CAS(false, true)
	if !notWatched {
		ch := make(chan WatchResp, 1)
		ch <- WatchResp{Err: errors.ErrDiscoveryDuplicateWatch.GenWithStackByArgs()}
		return ch
	}
	ch := make(chan WatchResp, defaultWatchChanSize)
	cctx, cancel := context.WithCancel(ctx)
	d.watchCancel = cancel
	go d.tickedWatch(cctx, ch)
	return ch
}

// CopySnapshot implements Discovery.CopySnapshot
func (d *EtcdSrvDiscovery) CopySnapshot(snapshot Snapshot) {
	d.snapshot = snapshot
}

// Close implements Discovery.Close
func (d *EtcdSrvDiscovery) Close() {
	if d.watchCancel != nil {
		d.watchCancel()
	}
}

func (d *EtcdSrvDiscovery) tickedWatch(ctx context.Context, ch chan<- WatchResp) {
	ticker := time.NewTicker(d.watchTickDur)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			ch <- WatchResp{Err: ctx.Err()}
			return
		case <-ticker.C:
			addSet, delSet, err := d.delta(ctx)
			if err != nil {
				ch <- WatchResp{Err: err}
				return
			}
			if len(addSet) > 0 || len(delSet) > 0 {
				ch <- WatchResp{
					AddSet: addSet,
					DelSet: delSet,
				}
			}
		}
	}
}

// delta returns snapshot diff compared with current snapshot, note delta will
// update current snapshot and revision.
func (d *EtcdSrvDiscovery) delta(ctx context.Context) (
	map[UUID]ServiceResource, map[UUID]ServiceResource, error,
) {
	snapshot, err := d.getSnapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	addSet := make(map[UUID]ServiceResource)
	delSet := make(map[UUID]ServiceResource)
	for k, v := range snapshot {
		if _, ok := d.snapshot[k]; !ok {
			addSet[k] = v
		}
	}
	for k, v := range d.snapshot {
		if _, ok := snapshot[k]; !ok {
			delSet[k] = v
		}
	}
	d.snapshot = snapshot
	return addSet, delSet, nil
}

// getSnapshot queries etcd and get a full set of service resource
func (d *EtcdSrvDiscovery) getSnapshot(ctx context.Context) (
	map[UUID]ServiceResource, error,
) {
	resp, err := d.etcdCli.Get(ctx, d.keyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(errors.ErrEtcdAPIError, err)
	}
	snapshot := make(map[UUID]ServiceResource, resp.Count)
	for _, kv := range resp.Kvs {
		uuid, resc, err := unmarshal(d.keyAdapter, kv.Key, kv.Value)
		if err != nil {
			return nil, err
		}
		snapshot[uuid] = resc
	}
	return snapshot, nil
}

// unmarshal wraps the unmarshal processing for key/value used in service discovery
func unmarshal(ka adapter.KeyAdapter, k, v []byte) (uuid UUID, resc ServiceResource, err error) {
	keys, err1 := ka.Decode(string(k))
	if err1 != nil {
		err = errors.Wrap(errors.ErrDecodeEtcdKeyFail, err1, string(k))
		return
	}
	uuid = keys[len(keys)-1]
	resc = ServiceResource{}
	err = json.Unmarshal(v, &resc)
	if err != nil {
		err = errors.Wrap(errors.ErrDecodeEtcdValueFail, err, string(v))
		return
	}
	return
}
