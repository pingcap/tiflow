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
	"time"

	"github.com/pingcap/tiflow/engine/pkg/adapter"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// DiscoveryRunner defines an interface to run Discovery service
type DiscoveryRunner interface {
	// ResetDiscovery creates a new discovery service, close the old discovery
	// watcher and creates a new watcher.
	// if resetSession is true, the session of discovery runner will be recreated
	// and returned.
	ResetDiscovery(ctx context.Context, resetSession bool) (Session, error)
	GetWatcher() <-chan WatchResp
	// returns current snapshot, DiscoveryRunner maintains this as snapshot plus
	// revision, which can be used in any failover scenarios. The drawback is to
	// consume more memory, but since it contains node address information only,
	// the memory consumption is acceptable.
	GetSnapshot() Snapshot
	// ApplyWatchResult applies changed ServiceResource to the snapshot of runner
	ApplyWatchResult(WatchResp)
}

// Snapshot alias to a map mapping from uuid to service resource (node info)
type Snapshot map[UUID]ServiceResource

// Clone returns a deep copy of Snapshot
func (s Snapshot) Clone() Snapshot {
	snapshot := make(map[UUID]ServiceResource, len(s))
	for k, v := range s {
		snapshot[k] = v
	}
	return snapshot
}

// DiscoveryRunnerImpl implements DiscoveryRunner
type DiscoveryRunnerImpl struct {
	etcdCli    *clientv3.Client
	sessionTTL int
	watchDur   time.Duration
	key        string
	value      string

	snapshot         Snapshot
	discovery        Discovery
	discoveryWatcher <-chan WatchResp
}

// NewDiscoveryRunnerImpl creates a new DiscoveryRunnerImpl
func NewDiscoveryRunnerImpl(
	etcdCli *clientv3.Client,
	sessionTTL int,
	watchDur time.Duration,
	key string,
	value string,
) *DiscoveryRunnerImpl {
	return &DiscoveryRunnerImpl{
		etcdCli:    etcdCli,
		sessionTTL: sessionTTL,
		watchDur:   watchDur,
		key:        key,
		value:      value,
	}
}

// Session defines a session used in discovery service
type Session interface {
	Done() <-chan struct{}
	Close() error
}

func (dr *DiscoveryRunnerImpl) connectToEtcdDiscovery(
	ctx context.Context, resetSession bool,
) (session Session, err error) {
	if resetSession {
		session, err = dr.createSession(ctx, dr.etcdCli)
		if err != nil {
			return
		}
	}

	// initialize a new service discovery, if old discovery exists, clones its
	// snapshot to the new one.
	old := dr.snapshot.Clone()
	dr.discovery = NewEtcdSrvDiscovery(
		dr.etcdCli, adapter.NodeInfoKeyAdapter, dr.watchDur)

	if len(old) > 0 {
		dr.discovery.CopySnapshot(old)
	} else {
		var snapshot Snapshot
		// must take a snapshot before discovery starts watch
		snapshot, err = dr.discovery.Snapshot(ctx)
		if err != nil {
			return
		}
		dr.snapshot = snapshot.Clone()
	}

	return
}

func (dr *DiscoveryRunnerImpl) createSession(ctx context.Context, etcdCli *clientv3.Client) (Session, error) {
	session, err := concurrency.NewSession(etcdCli, concurrency.WithTTL(dr.sessionTTL))
	if err != nil {
		return nil, err
	}
	_, err = etcdCli.Put(ctx, dr.key, dr.value, clientv3.WithLease(session.Lease()))
	if err != nil {
		return nil, err
	}
	return session, nil
}

// ResetDiscovery implements DiscoveryRunner.ResetDiscovery
func (dr *DiscoveryRunnerImpl) ResetDiscovery(ctx context.Context, resetSession bool) (Session, error) {
	session, err := dr.connectToEtcdDiscovery(ctx, resetSession)
	if err != nil {
		return nil, err
	}
	dr.discovery.Close()
	dr.discoveryWatcher = dr.discovery.Watch(ctx)
	return session, nil
}

// GetSnapshot implements DiscoveryRunner.GetSnapshot
func (dr *DiscoveryRunnerImpl) GetSnapshot() Snapshot {
	return dr.snapshot
}

// GetWatcher implements DiscoveryRunner.GetWatcher
func (dr *DiscoveryRunnerImpl) GetWatcher() <-chan WatchResp {
	return dr.discoveryWatcher
}

// ApplyWatchResult implements DiscoveryRunner.ApplyWatchResult
func (dr *DiscoveryRunnerImpl) ApplyWatchResult(resp WatchResp) {
	for uuid, add := range resp.AddSet {
		dr.snapshot[uuid] = add
	}
	for uuid := range resp.DelSet {
		delete(dr.snapshot, uuid)
	}
}
